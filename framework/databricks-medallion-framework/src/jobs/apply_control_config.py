"""
Production Config Apply job for the Databricks Medallion Ingestion Framework.

- Loads declarative YAML from config/ (sources, subject_areas, entities + env overlays,
  contracts, quality_rules, column_policies, reprocess_requests).
- Upserts into edw_platform_control_{env}.control.*
- Resolves UC Volume landing paths (never abfss:// in pipeline code).
- Records provenance on every row.

Run: databricks bundle run apply_control_config --target dev_personal
     (use --target dev_shared / qat / prod for the other environments)
"""

from __future__ import annotations

import os
import re
import json
import glob
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

spark = SparkSession.builder.getOrCreate()


try:
    from pyspark.dbutils import DBUtils

    _dbutils = DBUtils(spark)
except Exception:
    _dbutils = None

try:
    from jobs._cli_params import parse_job_args, get_param as _resolve_param
except ImportError:
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from jobs._cli_params import parse_job_args, get_param as _resolve_param

_CLI = parse_job_args()


def _get_param(name: str, default: Optional[str] = None) -> str:
    return _resolve_param(
        name, default, cli=_CLI, spark=spark, dbutils=_dbutils
    )


TARGET_CONTROL_CATALOG = _get_param("control_catalog", "edw_platform_control_dev")
GIT_COMMIT_SHA = _get_param("git_commit_sha", "local-dev")
GIT_BRANCH = _get_param("git_branch", "main")
TRIGGERED_BY = _get_param("triggered_by", "local")
CONFIG_ROOT = _get_param("config_root", "config")
ENVIRONMENT = _get_param("environment", "dev")
# DAB target that launched this apply (dev_personal | dev_shared | qat | prod).
# Passed by the bundle job as ${bundle.target}; defaults to 'local' when the
# script is run by hand outside a bundle. Recorded in control.config_deployments.
DAB_TARGET = _get_param("dab_target", "local")

CONTROL_SCHEMA = "control"
DEPLOYMENT_ID = f"deploy-{GIT_COMMIT_SHA[:8]}-{int(datetime.now(timezone.utc).timestamp())}"

print("Config Apply starting")
print(f"  Target catalog : {TARGET_CONTROL_CATALOG}")
print(f"  Environment    : {ENVIRONMENT}")
print(f"  DAB target     : {DAB_TARGET}")
print(f"  Git commit     : {GIT_COMMIT_SHA}")
print(f"  Deployment ID  : {DEPLOYMENT_ID}")


def _json_or_none(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return json.dumps(value)


def load_yaml_file(path: str) -> Optional[Dict[str, Any]]:
    """Load a single YAML config file into a dict, tagging it with its repo-relative
    ``_source_file`` for provenance. Returns None on error or non-mapping content."""
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
            if data and isinstance(data, dict):
                data["_source_file"] = os.path.relpath(path, CONFIG_ROOT)
                return data
    except Exception as e:
        print(f"ERROR loading {path}: {e}")
    return None


def load_all_yaml(base_path: str) -> List[Dict[str, Any]]:
    """Recursively load every ``*.yaml`` / ``*.yml`` under ``base_path`` into a list of dicts."""
    if not os.path.isdir(base_path):
        return []
    files = glob.glob(f"{base_path}/**/*.yaml", recursive=True) + glob.glob(
        f"{base_path}/**/*.yml", recursive=True
    )
    records = []
    for f in files:
        data = load_yaml_file(f)
        if data:
            records.append(data)
    return records


def _is_env_overlay_filename(path: str) -> bool:
    """Match foo.dev.yaml / foo.qat.yaml / foo.prod.yaml (not foo.yaml)."""
    name = os.path.basename(path)
    return bool(re.match(r"^.+\.(dev|qat|prod)\.ya?ml$", name, re.I))


def _env_from_filename(path: str) -> Optional[str]:
    m = re.match(r"^.+\.(dev|qat|prod)\.ya?ml$", os.path.basename(path), re.I)
    return m.group(1).lower() if m else None


def record_deployment_start() -> None:
    """Open a 'running' audit row in control.config_deployments (git provenance) for
    this apply run. Best-effort: warns but does not fail the job if it can't write."""
    from lib.sql_ops import insert_deployment_start

    try:
        insert_deployment_start(
            spark,
            TARGET_CONTROL_CATALOG,
            CONTROL_SCHEMA,
            DEPLOYMENT_ID,
            GIT_COMMIT_SHA,
            GIT_BRANCH,
            TRIGGERED_BY,
            TARGET_CONTROL_CATALOG,
            DAB_TARGET,
        )
    except Exception as e:
        print(f"WARNING: could not write config_deployments start row: {e}")


def record_deployment_end(
    status: str, tables_applied: List[str], error: Optional[str] = None
) -> None:
    """Close the config_deployments audit row as success/failed with the applied tables.
    Best-effort: warns but does not fail the job if it can't write."""
    from lib.sql_ops import update_deployment_end

    try:
        update_deployment_end(
            spark,
            TARGET_CONTROL_CATALOG,
            CONTROL_SCHEMA,
            DEPLOYMENT_ID,
            status,
            tables_applied,
            error=error,
        )
    except Exception as e:
        print(f"WARNING: could not write config_deployments end row: {e}")


def upsert_declarative_table(
    table_name: str,
    rows: List[Dict[str, Any]],
    key_cols: List[str],
    array_columns: Optional[List[str]] = None,
) -> int:
    """Safe upsert via DataFrame + explicit MERGE columns (no SET *).

    ``array_columns`` name control-table columns typed ``ARRAY<STRING>``; their
    values are preserved as native lists (not JSON-stringified) so they match the
    target schema on MERGE.
    """
    from lib.sql_ops import upsert_via_merge

    if not rows:
        return 0

    array_cols = set(array_columns or [])

    # Force complex values to JSON strings so createDataFrame schema is stable.
    # ARRAY<STRING> columns are kept as native lists (json.dumps would break MERGE).
    normalized = []
    for row in rows:
        nr = {}
        for k, v in row.items():
            if k in array_cols:
                if v is None:
                    nr[k] = None
                elif isinstance(v, (list, tuple)):
                    nr[k] = [str(x) for x in v]
                else:
                    nr[k] = [str(v)]
            elif isinstance(v, (dict, list)):
                nr[k] = json.dumps(v)
            elif v is None:
                nr[k] = None
            else:
                nr[k] = v
        normalized.append(nr)

    count = upsert_via_merge(
        spark,
        TARGET_CONTROL_CATALOG,
        CONTROL_SCHEMA,
        table_name,
        normalized,
        key_cols,
        provenance={
            "last_applied_git_commit_sha": GIT_COMMIT_SHA,
            "last_applied_deployment_id": DEPLOYMENT_ID,
            "last_applied_ts": "",  # sql_ops uses current_timestamp for this key
        },
        array_columns=array_columns,
    )
    print(
        f"  Upserted {count} rows into "
        f"{TARGET_CONTROL_CATALOG}.{CONTROL_SCHEMA}.{table_name}"
    )
    return count


def apply_subject_areas() -> int:
    """Upsert config/subject_areas/*.yaml into control.subject_areas (catalogs, schemas,
    landing volume). Env-overlay files are skipped here. Returns row count applied."""
    raw = load_all_yaml(f"{CONFIG_ROOT}/subject_areas")
    rows = []
    for item in raw:
        if _is_env_overlay_filename(item.get("_source_file", "")):
            continue
        key = item.get("subject_area_key")
        if not key:
            continue
        rows.append(
            {
                "subject_area_key": key,
                "description": item.get("description"),
                "catalogs_json": _json_or_none(item.get("catalogs")),
                "schemas_json": _json_or_none(item.get("schemas")),
                "landing_volume_json": _json_or_none(item.get("landing_volume")),
                "git_path": item.get("_source_file"),
                "is_active": True,
            }
        )
    return upsert_declarative_table("subject_areas", rows, ["subject_area_key"])


def apply_sources() -> int:
    """Base sources + env overlays (workday.yaml + workday.dev.yaml)."""
    base_path = f"{CONFIG_ROOT}/sources"
    if not os.path.isdir(base_path):
        return 0

    files = glob.glob(f"{base_path}/**/*.yaml", recursive=True) + glob.glob(
        f"{base_path}/**/*.yml", recursive=True
    )
    bases: Dict[str, Dict[str, Any]] = {}
    overlays: List[Dict[str, Any]] = []

    for f in files:
        data = load_yaml_file(f)
        if not data or "source_key" not in data:
            continue
        if _is_env_overlay_filename(f):
            data["_env"] = _env_from_filename(f) or ENVIRONMENT
            overlays.append(data)
        else:
            bases[data["source_key"]] = data

    # Apply matching env overlay onto base
    from lib.config_merge import deep_merge

    rows = []
    for source_key, base in bases.items():
        merged = base
        for ov in overlays:
            if ov.get("source_key") == source_key and ov.get("_env") == ENVIRONMENT:
                merged = deep_merge(base, ov)
                break

        conn = merged.get("connection") or {}
        rows.append(
            {
                "source_key": source_key,
                "environment": ENVIRONMENT,
                "source_type": merged.get("source_type"),
                "subject_area_key": merged.get("subject_area_key"),
                "description": merged.get("description"),
                "default_load_pattern": merged.get("default_load_pattern"),
                "connection_json": _json_or_none(conn),
                "connect_json": _json_or_none(merged.get("connect")),
                "extract_defaults_json": _json_or_none(merged.get("extract_defaults")),
                "secret_scope": (conn or {}).get("secret_scope"),
                "git_path": merged.get("_source_file"),
                "is_active": True,
            }
        )
        # Also store environment=all from base without overlay for fallback
        rows.append(
            {
                "source_key": source_key,
                "environment": "all",
                "source_type": base.get("source_type"),
                "subject_area_key": base.get("subject_area_key"),
                "description": base.get("description"),
                "default_load_pattern": base.get("default_load_pattern"),
                "connection_json": _json_or_none(base.get("connection")),
                "connect_json": _json_or_none(base.get("connect")),
                "extract_defaults_json": _json_or_none(base.get("extract_defaults")),
                "secret_scope": (base.get("connection") or {}).get("secret_scope"),
                "git_path": base.get("_source_file"),
                "is_active": True,
            }
        )

    return upsert_declarative_table("sources", rows, ["source_key", "environment"])


def _resolve_landing_for_entity(
    ent: Dict[str, Any],
    subject_area_key: str,
    subject_defaults: Optional[Dict[str, Any]],
    *,
    environment: Optional[str] = None,
    source_cfg: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Attach resolved paths onto load_config:
      - UC Volume raw/archive (API/file fallback)
      - Medallion bronze FQN + Connect staging FQN (env-aware catalog)

    environment:
      - concrete env (dev/qat/prod): resolve full FQNs with edw_{subject}_{env}
      - "all": keep {data_catalog}/{env} placeholders for runtime expansion
    """
    from lib.volumes import (
        resolve_raw_path,
        resolve_archive_path,
        resolve_data_catalog,
        resolve_medallion_table_fqn,
        resolve_connect_output_fqn,
        layer_schema_for_entity,
        table_name_with_source_prefix,
        expand_catalog_placeholders,
    )

    env = environment if environment is not None else ENVIRONMENT
    resolve_concrete = env not in ("all", "*", "")

    load = ent.setdefault("load_config", {})

    # Merge source-level load defaults (entity values win) so entities can omit
    # shared boilerplate. Lets a new source/subject define load behavior once.
    source_cfg = source_cfg or {}
    source_load_defaults = source_cfg.get("load_defaults") or {}
    if source_load_defaults.get("auto_loader_options"):
        load["auto_loader_options"] = {
            **source_load_defaults["auto_loader_options"],
            **(load.get("auto_loader_options") or {}),
        }

    vol = dict(load.get("landing_volume") or {})
    if subject_defaults and subject_defaults.get("landing_volume"):
        vol = {**subject_defaults["landing_volume"], **vol}

    if resolve_concrete:
        data_catalog = (
            ent.get("data_catalog")
            or (subject_defaults or {}).get("data_catalog")
            or vol.get("volume_catalog")
            or f"edw_{subject_area_key}_{env}"
        )
        data_catalog = expand_catalog_placeholders(
            str(data_catalog), str(data_catalog).replace("{env}", env), env
        )
        if "{env}" in data_catalog:
            data_catalog = data_catalog.replace("{env}", env)
    else:
        # Portable row — no env-specific catalog baked in
        data_catalog = (
            (subject_defaults or {}).get("data_catalog_pattern")
            or f"edw_{subject_area_key}_{{env}}"
        )
        if "{env}" not in data_catalog and not data_catalog.startswith("{"):
            # subject_defaults may have concrete catalog; re-template it
            data_catalog = f"edw_{subject_area_key}_{{env}}"

    vol.setdefault("volume_catalog", data_catalog)
    vol.setdefault("volume_schema", "files")
    vol.setdefault("volume_name", "landing")

    source_key = ent.get("source_key", "unknown")
    entity_name = ent.get("entity_name") or ent.get("entity_key")
    bronze_table = table_name_with_source_prefix(
        source_key,
        entity_name,
        explicit=ent.get("target_bronze_table"),
    )
    silver_table = table_name_with_source_prefix(
        source_key,
        entity_name,
        explicit=ent.get("target_silver_table") or bronze_table,
    )
    ent["target_bronze_table"] = bronze_table
    ent["target_silver_table"] = silver_table

    bronze_schema = layer_schema_for_entity(ent, "bronze")
    silver_schema = layer_schema_for_entity(ent, "silver")

    # --- Connect ownership: Connect → __src; framework DLT → final bronze ---
    connect = dict(load.get("lakeflow_connect_config") or {})
    source_connect = source_cfg.get("connect") or {}
    source_connect_defaults = source_connect.get("defaults") or {}
    if source_connect_defaults:
        # source connect defaults are the base; entity connect values win
        connect = {**source_connect_defaults, **connect}
    ent_for_resolve = {
        **ent,
        "data_catalog": data_catalog if resolve_concrete else f"edw_{subject_area_key}_{env if resolve_concrete else 'dev'}",
        "subject_area_key": subject_area_key,
        "load_config": load,
    }

    if resolve_concrete:
        final_bronze_fqn = resolve_medallion_table_fqn(
            {**ent_for_resolve, "data_catalog": data_catalog},
            layer="bronze",
            environment=env,
            table_name=bronze_table,
        )
        if connect.get("connect_output_table"):
            connect_output = expand_catalog_placeholders(
                connect["connect_output_table"], data_catalog, env
            )
        elif connect.get("raw_table"):
            connect_output = expand_catalog_placeholders(
                str(connect["raw_table"]), data_catalog, env
            )
        else:
            connect_output = f"{final_bronze_fqn}__src"
        # Enforce ownership: Connect must not target final bronze name without __src
        if connect_output.rstrip("`").endswith(f".{bronze_table}") and not connect_output.endswith(
            f"{bronze_table}__src"
        ):
            print(
                f"  WARN: rewriting connect_output for {ent.get('entity_key')} "
                f"from {connect_output} → {final_bronze_fqn}__src (ownership model)"
            )
            connect_output = f"{final_bronze_fqn}__src"
        silver_fqn = f"{data_catalog}.{silver_schema}.{silver_table}"
        vol["volume_catalog"] = data_catalog
    else:
        final_bronze_fqn = f"{{data_catalog}}.{bronze_schema}.{bronze_table}"
        connect_output = (
            connect.get("connect_output_table")
            or f"{{data_catalog}}.{bronze_schema}.{bronze_table}__src"
        )
        if not str(connect_output).endswith("__src"):
            connect_output = f"{{data_catalog}}.{bronze_schema}.{bronze_table}__src"
        silver_fqn = f"{{data_catalog}}.{silver_schema}.{silver_table}"
        vol.setdefault("volume_catalog", "{data_catalog}")

    connect["target_schema"] = bronze_schema
    connect["connect_output_table"] = connect_output
    connect["raw_table"] = connect_output
    connect["framework_bronze_table"] = final_bronze_fqn
    connect["bronze_writer"] = "framework_dlt"
    connect["connect_writer"] = "lakeflow_connect"
    if not connect.get("connection_name"):
        # Inherit from the source's connect identity rather than a hardcoded literal,
        # so refdata (SQL Server) / sales (Dynamics 365) sources resolve correctly.
        connect["connection_name"] = source_connect.get("connection_name")
    if "source_object" not in connect:
        connect["source_object"] = ent.get("source_object")
    load["lakeflow_connect_config"] = connect

    landing_subpath = load.get("landing_subpath") or f"raw/{source_key}/{entity_name}"
    archive_subpath = load.get("archive_subpath") or f"archive/{source_key}/{entity_name}"

    vol_catalog = vol.get("volume_catalog") or data_catalog
    if resolve_concrete and "{env}" in str(vol_catalog):
        vol_catalog = str(vol_catalog).replace("{env}", env)
    vol["volume_catalog"] = vol_catalog

    if resolve_concrete and not str(vol_catalog).startswith("{"):
        raw = resolve_raw_path(
            volume_catalog=vol_catalog,
            source_key=source_key,
            entity_name=entity_name,
            volume_schema=vol.get("volume_schema", "files"),
            volume_name=vol.get("volume_name", "landing"),
            landing_subpath=landing_subpath,
        )
        archive = resolve_archive_path(
            volume_catalog=vol_catalog,
            source_key=source_key,
            entity_name=entity_name,
            volume_schema=vol.get("volume_schema", "files"),
            volume_name=vol.get("volume_name", "landing"),
            archive_subpath=archive_subpath,
            archive_partitioning=load.get("archive_partitioning", "yyyy/MM/dd"),
        )
    else:
        raw = f"/Volumes/{{data_catalog}}/files/landing/{landing_subpath}"
        archive = f"/Volumes/{{data_catalog}}/files/landing/{archive_subpath}"

    load["landing_volume"] = vol
    load["landing_subpath"] = landing_subpath
    load["archive_subpath"] = archive_subpath
    load["landing_volume_path"] = raw
    load["archive_volume_path"] = archive
    load["bronze_table_fqn"] = final_bronze_fqn
    load["silver_table_fqn"] = silver_fqn
    ent["data_catalog"] = data_catalog
    ent["load_config"] = load
    return ent


def _load_sources_map() -> Dict[str, Dict[str, Any]]:
    """Merged source configs (base + env overlay) keyed by source_key.

    Used to inherit source-level connect identity / load defaults onto entities.
    """
    base_path = f"{CONFIG_ROOT}/sources"
    if not os.path.isdir(base_path):
        return {}
    files = glob.glob(f"{base_path}/**/*.yaml", recursive=True) + glob.glob(
        f"{base_path}/**/*.yml", recursive=True
    )
    bases: Dict[str, Dict[str, Any]] = {}
    overlays: List[Dict[str, Any]] = []
    for f in files:
        data = load_yaml_file(f)
        if not data or "source_key" not in data:
            continue
        if _is_env_overlay_filename(f):
            data["_env"] = _env_from_filename(f) or ENVIRONMENT
            overlays.append(data)
        else:
            bases[data["source_key"]] = data

    from lib.config_merge import deep_merge

    merged: Dict[str, Dict[str, Any]] = {}
    for source_key, base in bases.items():
        result = base
        for ov in overlays:
            if ov.get("source_key") == source_key and ov.get("_env") == ENVIRONMENT:
                result = deep_merge(base, ov)
                break
        merged[source_key] = result
    return merged


def apply_entities_and_load_configs() -> int:
    """Core of the apply job: merge base entities with env overlays and subject/source
    defaults, resolve each entity's landing/bronze/silver FQNs and derived Connect
    ``__src`` target, then upsert control.source_entities + entity_load_configs.
    Returns total rows applied across both tables.
    """
    from lib.config_merge import merge_entity_overlay, deep_merge

    base_path = f"{CONFIG_ROOT}/entities"
    if not os.path.isdir(base_path):
        return 0

    sources_map = _load_sources_map()

    files = glob.glob(f"{base_path}/**/*.yaml", recursive=True) + glob.glob(
        f"{base_path}/**/*.yml", recursive=True
    )

    # Group base docs by subject; collect overlays for current ENVIRONMENT
    bases: List[Dict[str, Any]] = []
    overlays_by_subject: Dict[str, Dict[str, Any]] = {}

    for f in files:
        data = load_yaml_file(f)
        if not data:
            continue
        if _is_env_overlay_filename(f):
            env = _env_from_filename(f)
            if env == ENVIRONMENT:
                subj = data.get("subject_area_key") or "unknown"
                overlays_by_subject[subj] = data
            continue
        bases.append(data)

    entity_rows: List[Dict[str, Any]] = []
    load_rows: List[Dict[str, Any]] = []

    for item in bases:
        subject = item.get("subject_area_key")
        overlay = overlays_by_subject.get(subject or "", {})
        subject_defaults = overlay.get("subject_defaults") if overlay else None
        base_entities = item.get("entities") or []
        overlay_entities = (overlay.get("entities") or []) if overlay else []

        merged_entities = merge_entity_overlay(
            base_entities, overlay_entities, subject_defaults
        )

        def _load_row(entity_key: str, env_name: str, ent_obj: Dict[str, Any], git_path: Any):
            load_cfg = ent_obj.get("load_config") or {}
            return {
                "entity_key": entity_key,
                "environment": env_name,
                "load_pattern": ent_obj.get("load_pattern"),
                "source_object": ent_obj.get("source_object"),
                "custom_extract_params": load_cfg.get("custom_extract_params")
                or (load_cfg.get("api") or {}).get("params"),
                "api_config": load_cfg.get("api"),
                "landing_volume": load_cfg.get("landing_volume"),
                "landing_volume_path": load_cfg.get("landing_volume_path"),
                "archive_volume_path": load_cfg.get("archive_volume_path"),
                "landing_subpath": load_cfg.get("landing_subpath"),
                "archive_subpath": load_cfg.get("archive_subpath"),
                "auto_loader_options": load_cfg.get("auto_loader_options"),
                "lakeflow_connect_config": load_cfg.get("lakeflow_connect_config"),
                "bronze_table_fqn": load_cfg.get("bronze_table_fqn"),
                "silver_table_fqn": load_cfg.get("silver_table_fqn"),
                "bronze_schema_evolution_mode": load_cfg.get(
                    "bronze_schema_evolution_mode", "addNewColumns"
                ),
                "git_path": git_path,
            }

        for ent in merged_entities:
            ent = _resolve_landing_for_entity(
                ent,
                subject or "unknown",
                subject_defaults,
                environment=ENVIRONMENT,
                source_cfg=sources_map.get(ent.get("source_key"), {}),
            )
            entity_key = ent["entity_key"]
            load = ent.get("load_config") or {}

            entity_rows.append(
                {
                    "entity_key": entity_key,
                    "subject_area_key": subject,
                    "source_key": ent.get("source_key"),
                    "entity_name": ent.get("entity_name", entity_key),
                    "source_object": ent.get("source_object"),
                    "load_pattern": ent.get("load_pattern"),
                    "primary_key_columns": ent.get("primary_key_columns"),
                    "watermark_column": ent.get("watermark_column"),
                    "target_bronze_table": ent.get("target_bronze_table"),
                    "target_silver_table": ent.get("target_silver_table"),
                    "data_catalog": ent.get("data_catalog"),
                    "restricted": bool(ent.get("restricted", False)),
                    "supports_full_reprocess": ent.get("supports_full_reprocess", True),
                    "reprocess_strategy": ent.get("reprocess_strategy", "full_refresh"),
                    "is_active": ent.get("is_active", True),
                    "git_path": item.get("_source_file"),
                }
            )

            load_rows.append(
                _load_row(
                    entity_key,
                    ENVIRONMENT,
                    ent,
                    item.get("_source_file")
                    or (overlay.get("_source_file") if overlay else None),
                )
            )

            # Portable environment=all row (placeholders, not baked to current env catalog)
            base_only = next(
                (e for e in base_entities if e.get("entity_key") == entity_key),
                {k: v for k, v in ent.items() if k != "load_config"},
            )
            base_only = deep_merge({}, base_only)
            base_only = _resolve_landing_for_entity(
                base_only,
                subject or "unknown",
                None,  # no env-specific subject_defaults for portable row
                environment="all",
            )
            load_rows.append(
                _load_row(entity_key, "all", base_only, item.get("_source_file"))
            )

    count1 = upsert_declarative_table("source_entities", entity_rows, ["entity_key"])
    count2 = upsert_declarative_table(
        "entity_load_configs", load_rows, ["entity_key", "environment"]
    )
    return count1 + count2


def apply_contracts() -> int:
    """Upsert config/contracts/*.yaml into control.data_contracts, one row per
    entity_key + version (the full contract is stored as JSON). Returns rows applied."""
    raw = load_all_yaml(f"{CONFIG_ROOT}/contracts")
    rows = []
    for item in raw:
        if "entity_key" not in item:
            continue
        rows.append(
            {
                "contract_id": f"{item['entity_key']}_v{item.get('version', 1)}",
                "entity_key": item["entity_key"],
                "version": item.get("version", 1),
                "git_path": item.get("_source_file"),
                "contract_json": json.dumps(
                    {k: v for k, v in item.items() if not k.startswith("_")}
                ),
                "effective_from": item.get("effective_from"),
            }
        )
    return upsert_declarative_table("data_contracts", rows, ["entity_key", "version"])


def apply_quality_rules() -> int:
    """Flatten config/quality_rules/*.yaml (per-entity and global defaults) into
    control.quality_rules, one row per rule keyed by rule_id. Returns rows applied."""
    raw = load_all_yaml(f"{CONFIG_ROOT}/quality_rules")
    rows = []
    for item in raw:
        entity = item.get("entity_key")
        layer = item.get("layer", "silver")
        for rule in item.get("rules", []):
            rlayer = rule.get("layer", layer)
            rows.append(
                {
                    "rule_id": f"{entity or 'global'}_{rlayer}_{rule['rule_name']}",
                    "entity_key": entity,
                    "layer": rlayer,
                    "rule_name": rule["rule_name"],
                    "rule_type": rule.get("rule_type", "expectation"),
                    "enforcement_method": rule.get(
                        "enforcement_method", "external_library"
                    ),
                    "expression": rule.get("expression"),
                    "library_reference": rule.get("library_reference"),
                    "action_on_failure": rule.get("action_on_failure", "warn"),
                    "severity": rule.get("severity", "medium"),
                    "git_path": item.get("_source_file"),
                    "is_default": rule.get("is_default", False),
                    "is_active": rule.get("is_active", True),
                }
            )
    return upsert_declarative_table("quality_rules", rows, ["rule_id"])


def apply_column_policies() -> int:
    """Upsert config/column_policies/*.yaml into control.column_policies (encrypt / mask /
    hash / tag directives per entity+column). Returns rows applied."""
    raw = load_all_yaml(f"{CONFIG_ROOT}/column_policies")
    rows = []
    for item in raw:
        entity = item.get("entity_key")
        for pol in item.get("policies", []):
            rows.append(
                {
                    "policy_id": f"{entity}_{pol['column_name']}",
                    "entity_key": entity,
                    "column_name": pol["column_name"],
                    "policy_type": pol.get("policy_type", "tag_only"),
                    "encryption_key_vault_ref": pol.get("encryption_key_vault_ref"),
                    "apply_starting_layer": pol.get("apply_starting_layer", "silver"),
                    "classification": pol.get("classification"),
                    "git_path": item.get("_source_file"),
                    "is_active": True,
                }
            )
    return upsert_declarative_table("column_policies", rows, ["entity_key", "column_name"])


def apply_pipeline_assets() -> int:
    """Load config/pipeline_assets/** into control.pipeline_assets."""
    raw = load_all_yaml(f"{CONFIG_ROOT}/pipeline_assets")
    rows = []
    for item in raw:
        subject = item.get("subject_area_key")
        for asset in item.get("assets") or []:
            asset_id = asset.get("asset_id")
            if not asset_id:
                continue
            rows.append(
                {
                    "asset_id": asset_id,
                    "entity_key": asset.get("entity_key"),
                    "subject_area_key": asset.get("subject_area_key") or subject,
                    "asset_type": asset.get("asset_type"),
                    "asset_name": asset.get("asset_name"),
                    "resource_name_in_bundle": asset.get("resource_name_in_bundle"),
                    "bundle_path": asset.get("bundle_path"),
                    "git_path": asset.get("git_path") or item.get("_source_file"),
                    "target_layer": asset.get("target_layer"),
                    "depends_on": asset.get("depends_on"),
                    "supports_reprocess": bool(asset.get("supports_reprocess", False)),
                    "default_schedule": asset.get("default_schedule"),
                    "compute_type": asset.get("compute_type"),
                    "parameters": asset.get("parameters"),
                    "is_active": bool(asset.get("is_active", True)),
                }
            )
    return upsert_declarative_table(
        "pipeline_assets", rows, ["asset_id"], array_columns=["depends_on"]
    )


def apply_reprocess_requests() -> int:
    """Upsert config/reprocess_requests/*.yaml into control.reprocess_requests with
    status 'submitted' (the dispatcher later approves/executes them). Returns rows applied."""
    raw = load_all_yaml(f"{CONFIG_ROOT}/reprocess_requests")
    rows = []
    for item in raw:
        if not item.get("request_id"):
            continue
        rows.append(
            {
                "request_id": item.get("request_id"),
                "subject_area_key": item.get("subject_area_key"),
                "requested_entities": item.get("requested_entities"),
                "reprocess_mode": item.get("reprocess_mode", "full"),
                "from_watermark": item.get("from_watermark"),
                "to_watermark": item.get("to_watermark"),
                "reason": item.get("reason"),
                "requested_by": item.get("requested_by"),
                "source_file_path": item.get("_source_file"),
                "git_commit_sha": GIT_COMMIT_SHA,
                "github_pr_url": item.get("github_pr_url"),
                "status": "submitted",
            }
        )
    return upsert_declarative_table(
        "reprocess_requests", rows, ["request_id"], array_columns=["requested_entities"]
    )


def main():
    """Entry point: apply every config section into the control catalog in dependency
    order, wrapping the run in a config_deployments audit row (success/failed)."""
    record_deployment_start()
    tables_applied: List[str] = []
    total_rows = 0

    try:
        total_rows += apply_subject_areas()
        tables_applied.append("subject_areas")

        total_rows += apply_sources()
        tables_applied.append("sources")

        total_rows += apply_contracts()
        tables_applied.append("data_contracts")

        total_rows += apply_quality_rules()
        tables_applied.append("quality_rules")

        total_rows += apply_column_policies()
        tables_applied.append("column_policies")

        total_rows += apply_entities_and_load_configs()
        tables_applied.extend(["source_entities", "entity_load_configs"])

        total_rows += apply_pipeline_assets()
        tables_applied.append("pipeline_assets")

        total_rows += apply_reprocess_requests()
        tables_applied.append("reprocess_requests")

        record_deployment_end("success", tables_applied)
        print(f"Config Apply completed successfully. Total rows affected: {total_rows}")

    except Exception as e:
        print(f"Config Apply FAILED: {e}")
        record_deployment_end("failed", tables_applied, str(e))
        raise


if __name__ == "__main__":
    main()
