# Reprocess and pipeline_assets (option 8)

## pipeline_assets (GitOps)

Declarative registry of DABs jobs / workflows / pipelines used by the reprocess dispatcher.

| Git | Control table |
|-----|----------------|
| `config/pipeline_assets/**/*.yaml` | `edw_platform_control_{env}.control.pipeline_assets` |

Applied by `apply_control_config` (same job as entities/sources).

### HR seed (`config/pipeline_assets/hr.yaml`)

Includes:

- Workflows: `hr_workday_orchestration`, `reprocess_hr_workday`, `hr_workday_api_fallback_orchestration`
- Pipelines: bronze / bronze_restricted / silver / silver_restricted
- Jobs: `apply_control_config`, `reprocess_orchestrator`, extract/archive (fallback)

Dispatcher prefers:

```sql
SELECT asset_name FROM pipeline_assets
WHERE subject_area_key = 'hr'
  AND asset_type = 'workflow'
  AND supports_reprocess = true
```

Falls back to name conventions if the table is empty.

---

## Reprocess flow

```text
1. Engineer adds config/reprocess_requests/<file>.yaml
2. PR + environment approval (GitHub)  OR  manual status update in SQL
3. apply_control_config upserts request (status submitted / approved per process)
4. reprocess_orchestrator job (scheduled every 15 min) runs reprocess_dispatcher.py
5. Dispatcher (`wait_mode=wait` by default):
     - picks status = approved
     - marks executing
     - forces watermark_state
     - triggers reprocess workflow via SDK (name from pipeline_assets)
     - **waits for job run to terminate**
     - success → status = completed; failure/timeout → failed
     - clears is_reprocessing flags
     - on trigger failure → status = failed
6. Pipelines re-run bronze/silver (full_refresh on reprocess_hr_workday)
```

Use `--wait_mode=fire_and_forget` only if you intentionally leave requests in `executing`.

### Sample request

See `config/reprocess_requests/hr/smoke-reprocess-location.yaml`.

### Manual approve (dev)

```sql
UPDATE edw_platform_control_dev.control.reprocess_requests
SET status = 'approved', updated_ts = current_timestamp()
WHERE request_id = 'hr-smoke-reprocess-001';
```

### Schedule

DABs job `reprocess_orchestrator` has a **quartz cron** schedule: every 15 minutes (`0 0/15 * * * ?`).

Pause in higher envs if you only want GH-triggered dispatch.

---

## Status lifecycle

| Status | Meaning |
|--------|---------|
| `submitted` | Loaded from Git, not yet approved |
| `approved` | Ready for dispatcher |
| `executing` | Dispatcher claimed; workflow triggered |
| `completed` | Pipeline/job marked complete (when hooks call `mark_reprocess_completed`) |
| `failed` | Trigger or execution failure recorded |

---

## Hardening included

- SQL-safe updates (escaped literals / DataFrame MERGE for watermarks)
- Failed trigger → `status = failed` + `result_summary`
- Exception during processing → failed
- Asset-driven workflow name lookup with convention fallback
- Scheduled orchestrator in `bundles/databricks.yml`

### Still optional later

- Completion callback from final orchestration task → `mark_reprocess_completed`
- Per-entity lease / concurrency limits
- PR comment with run URL
