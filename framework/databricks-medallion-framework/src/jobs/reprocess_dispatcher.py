"""
Reprocess Dispatcher

Queries approved reprocess_requests, marks executing, forces watermarks,
triggers orchestration workflows via Databricks SDK, waits for completion,
and sets status completed/failed.
"""

from __future__ import annotations

import json
import time
from typing import List, Dict, Any, Optional
from datetime import datetime

from pyspark.sql import SparkSession
from databricks.sdk import WorkspaceClient

from lib.sql_safe import sql_str, sql_int, qualified_table

spark = SparkSession.builder.getOrCreate()

try:
    from pyspark.dbutils import DBUtils

    dbutils = DBUtils(spark)
except Exception:
    dbutils = None

try:
    from jobs._cli_params import parse_job_args, get_param as _resolve_param
except ImportError:
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from jobs._cli_params import parse_job_args, get_param as _resolve_param

_CLI = parse_job_args()


def _get_param(name: str, default=None):
    return _resolve_param(
        name, default, cli=_CLI, spark=spark, dbutils=dbutils
    )


CONTROL_CATALOG = _get_param("control_catalog", "edw_platform_control_dev")
MAX_REQUESTS = int(_get_param("max_requests", "10") or "10")
# wait | fire_and_forget
WAIT_MODE = (_get_param("wait_mode", "wait") or "wait").lower()
WAIT_TIMEOUT_MINUTES = int(_get_param("wait_timeout_minutes", "120") or "120")
CONTROL_SCHEMA = "control"

print(f"=== Reprocess Dispatcher starting for catalog={CONTROL_CATALOG} ===")
print(f"    wait_mode={WAIT_MODE} timeout_min={WAIT_TIMEOUT_MINUTES}")

w = WorkspaceClient()


def _ctrl(table: str) -> str:
    return qualified_table(CONTROL_CATALOG, CONTROL_SCHEMA, table)


def get_approved_reprocess_requests() -> List[Dict[str, Any]]:
    """Return control.reprocess_requests rows in status 'approved' (oldest first, capped
    at MAX_REQUESTS) that are ready for the dispatcher to execute."""
    sql = f"""
        SELECT request_id, subject_area_key, requested_entities, reprocess_mode,
               from_watermark, to_watermark, reason, source_file_path, git_commit_sha
        FROM {_ctrl("reprocess_requests")}
        WHERE status = 'approved'
        ORDER BY created_ts ASC
        LIMIT {sql_int(MAX_REQUESTS)}
    """
    return [r.asDict() for r in spark.sql(sql).collect()]


def update_request_to_executing(request_id: str, run_id: Optional[str] = None) -> None:
    """Mark a reprocess request as 'executing' and stamp its execution run id."""
    exec_id = run_id or "dispatcher-" + datetime.utcnow().strftime("%Y%m%d%H%M%S")
    sql = f"""
        UPDATE {_ctrl("reprocess_requests")}
        SET status = 'executing',
            execution_run_id = {sql_str(exec_id)},
            updated_ts = current_timestamp()
        WHERE request_id = {sql_str(request_id)}
    """
    spark.sql(sql)


def update_request_status(
    request_id: str,
    status: str,
    *,
    run_id: Optional[str] = None,
    summary: Optional[Dict[str, Any]] = None,
) -> None:
    """Update a reprocess request's status (and optional run id / JSON result summary);
    stamps ``executed_at`` when transitioning to 'completed'."""
    parts = [
        f"status = {sql_str(status)}",
        "updated_ts = current_timestamp()",
    ]
    if run_id is not None:
        parts.append(f"execution_run_id = {sql_str(str(run_id))}")
    if summary is not None:
        parts.append(f"result_summary = {sql_str(json.dumps(summary), max_len=8000)}")
    if status == "completed":
        parts.append("executed_at = current_timestamp()")
    sql = f"""
        UPDATE {_ctrl("reprocess_requests")}
        SET {", ".join(parts)}
        WHERE request_id = {sql_str(request_id)}
    """
    spark.sql(sql)


def update_request_failed(request_id: str, error: str) -> None:
    """Convenience wrapper: mark a request 'failed' with a truncated error summary."""
    update_request_status(
        request_id,
        "failed",
        summary={"phase": "failed", "error": error[:2000]},
    )


def force_reprocess_watermark(entity_key: str, request: Dict[str, Any]) -> None:
    """Reset an entity's watermark_state to the request's ``from_watermark`` and flag it
    ``is_reprocessing`` so the next pipeline run re-reads history from that point."""
    from_watermark = request.get("from_watermark") or "1900-01-01T00:00:00Z"
    request_id = request["request_id"]
    wm = _ctrl("watermark_state")
    spark.createDataFrame(
        [
            {
                "entity_key": entity_key,
                "current_watermark": from_watermark,
                "reprocess_request_id": request_id,
            }
        ]
    ).createOrReplaceTempView("__reprocess_wm_src")

    sql = f"""
        MERGE INTO {wm} AS target
        USING __reprocess_wm_src AS src
        ON target.entity_key = src.entity_key
        WHEN MATCHED THEN UPDATE SET
            current_watermark = src.current_watermark,
            is_reprocessing = true,
            reprocess_request_id = src.reprocess_request_id,
            updated_ts = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (
            entity_key, current_watermark, is_reprocessing, reprocess_request_id, updated_ts
        ) VALUES (
            src.entity_key, src.current_watermark, true, src.reprocess_request_id, current_timestamp()
        )
    """
    spark.sql(sql)
    print(f"  Forced reprocess watermark for {entity_key} to {from_watermark}")


def clear_reprocess_flags(entities: List[str]) -> None:
    """Clear is_reprocessing after a finished run (success or fail)."""
    wm = _ctrl("watermark_state")
    for entity_key in entities:
        spark.sql(
            f"""
            UPDATE {wm}
            SET is_reprocessing = false,
                reprocess_request_id = NULL,
                updated_ts = current_timestamp()
            WHERE entity_key = {sql_str(entity_key)}
            """
        )


def find_orchestration_workflow_name(
    subject_area_key: str, *, reprocess: bool = True
) -> str:
    """Resolve the orchestration workflow name for a subject: prefer control.pipeline_assets
    (asset_type='workflow'), falling back to naming conventions (e.g. 'reprocess_hr_workday')."""
    try:
        if reprocess:
            sql = f"""
                SELECT asset_name FROM {_ctrl("pipeline_assets")}
                WHERE subject_area_key = {sql_str(subject_area_key)}
                  AND asset_type = 'workflow'
                  AND supports_reprocess = true
                  AND is_active = true
                ORDER BY asset_name
                LIMIT 1
            """
        else:
            sql = f"""
                SELECT asset_name FROM {_ctrl("pipeline_assets")}
                WHERE subject_area_key = {sql_str(subject_area_key)}
                  AND asset_type = 'workflow'
                  AND is_active = true
                ORDER BY asset_name
                LIMIT 1
            """
        rows = spark.sql(sql).collect()
        if rows:
            name = rows[0].asset_name
            print(f"  pipeline_assets → workflow '{name}' (reprocess={reprocess})")
            return name
    except Exception as e:
        print(f"  pipeline_assets lookup skipped: {e}")

    if reprocess:
        conventions = {
            "hr": "reprocess_hr_workday",
        }
        return conventions.get(subject_area_key, f"reprocess_{subject_area_key}")

    conventions = {
        "hr": "hr_workday_orchestration",
    }
    return conventions.get(subject_area_key, f"{subject_area_key}_orchestration")


def trigger_workflow(workflow_name: str, request: Dict[str, Any], entities: List[str]):
    """Find the Databricks job by name and trigger it via the SDK, passing reprocess
    parameters (request id, mode, entities, watermarks). Returns the run id or None."""
    try:
        all_jobs = list(w.jobs.list())
        target_job = None
        for j in all_jobs:
            if j.settings and j.settings.name == workflow_name:
                target_job = j
                break

        if not target_job:
            print(f"  WARNING: Could not find workflow/job named '{workflow_name}'.")
            return None

        # Prefer job parameters when defined; notebook_params for legacy notebook tasks
        job_params = {
            "reprocess_request_id": request["request_id"],
            "reprocess_mode": request.get("reprocess_mode", "full"),
            "entities": ",".join(entities),
            "from_watermark": request.get("from_watermark") or "",
            "to_watermark": request.get("to_watermark") or "",
            "control_catalog": CONTROL_CATALOG,
        }

        try:
            run = w.jobs.run_now(job_id=target_job.job_id, job_parameters=job_params)
        except TypeError:
            run = w.jobs.run_now(
                job_id=target_job.job_id, notebook_params=job_params
            )
        except Exception:
            # Older SDK / job without parameters
            run = w.jobs.run_now(job_id=target_job.job_id)

        print(
            f"  Triggered workflow '{workflow_name}' "
            f"(run_id={run.run_id}) for request {request['request_id']}"
        )
        return run.run_id
    except Exception as e:
        print(f"  ERROR triggering workflow {workflow_name}: {e}")
        return None


def wait_for_run(run_id: int, timeout_minutes: int) -> Dict[str, Any]:
    """Poll until run terminates or timeout. Returns status summary."""
    deadline = time.time() + timeout_minutes * 60
    terminal = {
        "TERMINATED",
        "SKIPPED",
        "INTERNAL_ERROR",
        "BLOCKED",
    }
    last = None
    while time.time() < deadline:
        run = w.jobs.get_run(run_id=run_id)
        life = None
        result = None
        if run.state:
            life = getattr(run.state.life_cycle_state, "value", None) or str(
                run.state.life_cycle_state
            )
            result = getattr(run.state.result_state, "value", None) or (
                str(run.state.result_state) if run.state.result_state else None
            )
        last = {"life_cycle_state": life, "result_state": result, "run_id": run_id}
        print(f"  run {run_id}: life={life} result={result}")
        if life and str(life).upper() in terminal:
            return last
        time.sleep(30)
    return {
        "life_cycle_state": "TIMEOUT",
        "result_state": "FAILED",
        "run_id": run_id,
        "error": f"Timed out after {timeout_minutes} minutes",
    }


def main():
    """Entry point: for each approved reprocess request, force watermarks, trigger the
    subject's orchestration workflow, optionally wait for completion, and record status."""
    approved = get_approved_reprocess_requests()
    print(f"Found {len(approved)} approved reprocess request(s)")

    for req in approved:
        request_id = req["request_id"]
        subject = req["subject_area_key"]
        entities = req.get("requested_entities") or []
        if isinstance(entities, str):
            try:
                entities = json.loads(entities)
            except Exception:
                entities = [e.strip() for e in entities.split(",") if e.strip()]
        entities = [str(e) for e in entities]

        print(f"\nProcessing request {request_id} for subject={subject}, entities={entities}")

        try:
            update_request_to_executing(request_id)
            for ent in entities:
                force_reprocess_watermark(ent, req)

            workflow_name = find_orchestration_workflow_name(subject, reprocess=True)
            run_id = trigger_workflow(workflow_name, req, entities)

            if run_id is None:
                update_request_failed(
                    request_id,
                    f"Failed to trigger reprocess workflow '{workflow_name}' "
                    "(check bundle deploy + pipeline_assets.asset_name)",
                )
                clear_reprocess_flags(entities)
                continue

            update_request_status(
                request_id,
                "executing",
                run_id=str(run_id),
                summary={
                    "phase": "triggered",
                    "workflow": workflow_name,
                    "entities": entities,
                },
            )

            if WAIT_MODE in ("wait", "sync", "true", "1"):
                status = wait_for_run(int(run_id), WAIT_TIMEOUT_MINUTES)
                result = (status.get("result_state") or "").upper()
                life = (status.get("life_cycle_state") or "").upper()
                success = result in ("SUCCESS", "SUCCEEDED", "SUCCESSFUL") or (
                    life == "TERMINATED" and result in ("SUCCESS", "SUCCEEDED", None, "NONE", "")
                )
                # Prefer explicit SUCCESS; treat TERMINATED without result carefully
                if result in ("SUCCESS", "SUCCEEDED", "SUCCESSFUL"):
                    success = True
                elif result in ("FAILED", "TIMEDOUT", "CANCELED", "CANCELLED", "MAXIMUM_CONCURRENT_RUNS_REACHED"):
                    success = False
                elif life == "TIMEOUT":
                    success = False

                if success:
                    update_request_status(
                        request_id,
                        "completed",
                        run_id=str(run_id),
                        summary={
                            "phase": "completed",
                            "workflow": workflow_name,
                            "entities": entities,
                            "run": status,
                        },
                    )
                    print(f"  Request {request_id} COMPLETED")
                else:
                    update_request_status(
                        request_id,
                        "failed",
                        run_id=str(run_id),
                        summary={
                            "phase": "failed",
                            "workflow": workflow_name,
                            "entities": entities,
                            "run": status,
                        },
                    )
                    print(f"  Request {request_id} FAILED: {status}")
                clear_reprocess_flags(entities)
            else:
                print(
                    f"  fire_and_forget: left {request_id} in executing "
                    f"(run_id={run_id}); complete via jobs UI or wait_mode=wait"
                )

        except Exception as e:
            print(f"  ERROR processing {request_id}: {e}")
            update_request_failed(request_id, str(e))
            try:
                clear_reprocess_flags(entities)
            except Exception:
                pass

    print("\nReprocess dispatcher finished.")


if __name__ == "__main__":
    main()
