# Reprocess requests (as code)

Place reprocess request YAML files in this directory (or subdirectories per subject area).

**Smoke sample:** [`hr/smoke-reprocess-location.yaml`](hr/smoke-reprocess-location.yaml)  
**Full docs:** [`docs/08-reprocess-and-pipeline-assets.md`](../../docs/08-reprocess-and-pipeline-assets.md)  
**Workspace proof:** [`docs/07-workspace-proof-runbook.md`](../../docs/07-workspace-proof-runbook.md)

## Flow

1. Engineer creates the YAML with entities, mode, optional watermark window, and reason.
2. Open a Pull Request **or** apply + manually set `status = approved` in dev.
3. GitHub Environment protection (prod) **or** SQL approve in lower envs.
4. Config Apply upserts into **`edw_platform_control_{env}.control.reprocess_requests`**.
5. **`reprocess_orchestrator`** (scheduled every 15 min, or `bundle run`) runs the dispatcher:
   - Claims `approved` → `executing`
   - Forces watermarks
   - Triggers workflow from **`pipeline_assets`** (`supports_reprocess=true`, e.g. `reprocess_hr_workday`)
   - On trigger failure → `failed`

## Control tables

| Table | Role |
|-------|------|
| `reprocess_requests` | Request lifecycle |
| `pipeline_assets` | Seeded from `config/pipeline_assets/` |
| `watermark_state` | Forced for reprocess window |

DDL: [`sql/control/02_control_tables.sql`](../../sql/control/02_control_tables.sql)

Status: `submitted` → `approved` → `executing` → `completed` | `failed`.

## HR note

Entity keys must match `config/entities/hr.yaml` (e.g. `hr_location`, `hr_current_employee_list`).
