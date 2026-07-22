# Workspace proof runbook (option 7)

Prove the HR path end-to-end in a **live** Databricks workspace.

**Happy path**

```text
DDL → Connect (__src) → bundle deploy → config apply → orchestration → verify
```

Target example: environment **dev** (`edw_platform_control_dev`, `edw_hr_dev`),
deployed with the DAB target **`dev_personal`** (your personal sandbox — the
default). Use `dev_shared` for a shared/CI deploy. See
[docs/09 — Declarative Automation Bundles](09-databricks-asset-bundles.md).

---

## Prerequisites

| # | Item | Check |
|---|------|--------|
| 1 | UC metastore + workspace Premium | Workspace UI |
| 2 | Access Connector MI + storage credential + external location for HR container | UC → External locations |
| 3 | Secret scope `kv-hr-dev` with Workday/Connect-related secret **names** from `config/sources/workday.yaml` | `databricks secrets list-scopes` |
| 4 | Databricks CLI auth | `databricks auth profiles` / env `DATABRICKS_HOST` + `DATABRICKS_TOKEN` |
| 5 | Repo checkout at framework root | `framework/databricks-medallion-framework` |

Optional local preflight (no cluster):

```bash
cd framework/databricks-medallion-framework
python scripts/lint_framework_config.py
python scripts/workspace_proof_preflight.py --env dev
pytest tests/ -q
```

---

## Phase A — Control + HR catalog DDL

Run the control-plane DDL against the workspace. The storage account per
environment is set once in
[`config/environments.yaml`](../config/environments.yaml) — no per-file URL
edits. The `.sql` files are **templates** (they contain `{env}` and
`{storage_account}` tokens); the runner fills these in, so don't paste the raw
files. You can do this **without a SQL Warehouse** — pick whichever fits:

- **From VS Code (no warehouse):** run the helper, which executes the DDL on
  serverless compute via Databricks Connect (see [scripts/README.md](../scripts/README.md)):

  ```bash
  python scripts/run_control_sql.py --env dev
  ```

- **In a notebook / SQL Warehouse / SQL editor:** first print the rendered,
  paste-ready SQL, then paste it into a `%sql` cell or the SQL editor:

  ```bash
  python scripts/run_control_sql.py --env dev --dry-run
  ```

> **qat / prod:** the storage account names for `qat` and `prod` in
> `config/environments.yaml` are placeholders — confirm the real ADLS Gen2
> account names before running against those environments.

Files, in order:

```text
sql/control/00_create_catalogs.sql       # catalogs (admin-owned; may run outside DAB)
sql/control/01_create_schemas.sql        # schemas + external volumes
sql/control/02_control_tables.sql        # control tables + views
```

Verify — from VS Code (no warehouse):

```bash
python scripts/show_tables.py --env dev
```

…or with SQL (notebook / warehouse):

```sql
SHOW SCHEMAS IN edw_platform_control_dev;
SHOW TABLES IN edw_platform_control_dev.control;
SHOW SCHEMAS IN edw_hr_dev;
SHOW VOLUMES IN edw_hr_dev.files;
```

Expected schemas on `edw_hr_dev`:  
`bronze`, `bronze_restricted`, `silver`, `silver_restricted`, `gold`, `gold_restricted`, `files`.

---

## Phase B — Lakeflow Connect (Workday) → `__src` tables

1. Create / confirm Lakeflow Connect **connection** named consistently with config  
   (e.g. `workday_connect` / `workday_connect_dev` from env overlay).
2. For a **smoke test set** (start small), provision Connect to land into:

| Entity | Connect writes (staging) | Framework bronze owns |
|--------|--------------------------|------------------------|
| Current employees | `edw_hr_dev.bronze.workday_current_employee_list__src` | `...bronze.workday_current_employee_list` |
| Location (simple) | `edw_hr_dev.bronze.workday_location__src` | `...bronze.workday_location` |
| Payroll (restricted) | `edw_hr_dev.bronze_restricted.workday_payroll_employee_list__src` | `...bronze_restricted.workday_payroll_employee_list` |

3. Run Connect once so `__src` tables exist and have rows (or empty schema-ready tables).

Verify:

```sql
-- Adjust names if smoke test set differs
SELECT COUNT(*) AS n FROM edw_hr_dev.bronze.workday_current_employee_list__src;
SELECT COUNT(*) AS n FROM edw_hr_dev.bronze.workday_location__src;
```

**If Connect is not ready yet:** use API/Volume fallback for one entity only  
(`load_pattern: api_extract` overlay + `hr_workday_api_fallback_orchestration`) — not the primary proof.

---

## Phase C — Deploy + Config Apply

From framework root:

```bash
export DATABRICKS_HOST=...
export DATABRICKS_TOKEN=...

cd framework/databricks-medallion-framework

# Build wheel used by pipelines/jobs (also run by bundle artifact build)
python -m pip install build
python -m build --wheel -o dist

# From the bundle directory
cd bundles
databricks bundle validate --target dev_personal
databricks bundle deploy --target dev_personal
databricks bundle run apply_control_config --target dev_personal
```

**Deploy notes (P0):**

- Orchestration jobs live under `resources.jobs` (not `workflows`).
- Python jobs use `spark_python_task` + environment wheel (`medallion-framework`).
- Pipelines attach `../dist/*.whl` plus the entry pipeline `.py` file.
- `config_root` defaults to `${workspace.file_path}/config` (synced by the bundle).

Verify control plane:

```sql
SELECT COUNT(*) FROM edw_platform_control_dev.control.source_entities
WHERE subject_area_key = 'hr' AND is_active;

SELECT entity_key, load_pattern, target_bronze_table, restricted
FROM edw_platform_control_dev.control.source_entities
WHERE subject_area_key = 'hr'
ORDER BY entity_key;

SELECT entity_key, environment,
       get_json_object(lakeflow_connect_config, '$.connect_output_table') AS connect_src
FROM edw_platform_control_dev.control.entity_load_configs
WHERE environment = 'dev'
LIMIT 20;

SELECT asset_name, asset_type, subject_area_key, supports_reprocess, is_active
FROM edw_platform_control_dev.control.pipeline_assets
ORDER BY asset_name;

SELECT * FROM edw_platform_control_dev.control.config_deployments
ORDER BY started_ts DESC LIMIT 5;
```

Expect:

- 13 HR entities (or your smoke subset if filtered)
- `load_pattern = lakeflow_connect`
- `connect_output_table` ends with `__src` and uses `edw_hr_dev` after apply
- pipeline_assets rows for HR workflows/pipelines
- Latest deployment `status = success`

Also run SQL checks file: `sql/control/03_workspace_proof_checks.sql`.

---

## Phase D — Orchestration (Bronze → Silver)

```bash
# Full HR path (standard + restricted in parallel)
databricks bundle run hr_workday_orchestration --target dev_personal
```

Or stepwise:

```bash
databricks bundle run hr_workday_bronze --target dev_personal
databricks bundle run hr_workday_bronze_restricted --target dev_personal
databricks bundle run hr_workday_silver --target dev_personal
databricks bundle run hr_workday_silver_restricted --target dev_personal
```

Verify:

```sql
SELECT COUNT(*) FROM edw_hr_dev.bronze.workday_current_employee_list;
SELECT COUNT(*) FROM edw_hr_dev.silver.workday_current_employee_list;

SELECT COUNT(*) FROM edw_hr_dev.bronze_restricted.workday_payroll_employee_list;
SELECT COUNT(*) FROM edw_hr_dev.silver_restricted.workday_payroll_employee_list;

-- Tech columns present on bronze
DESCRIBE TABLE edw_hr_dev.bronze.workday_location;
-- expect _bronze_ingest_ts, _source_system, _entity, _ingest_method, ...
```

---

## Phase E — Reprocess proof (see [docs/08](08-reprocess-and-pipeline-assets.md))

1. Add / ensure a request under `config/reprocess_requests/` (see sample).
2. Apply config so row lands with status `submitted` (or update to `approved` for manual test).
3. For manual proof:

```sql
UPDATE edw_platform_control_dev.control.reprocess_requests
SET status = 'approved', updated_ts = current_timestamp()
WHERE request_id = 'hr-smoke-reprocess-001';
```

4. Run dispatcher (scheduled every 15 min in DABs, or manual):

```bash
databricks bundle run reprocess_orchestrator --target dev_personal
```

5. Verify:

```sql
SELECT request_id, status, execution_run_id, result_summary, updated_ts
FROM edw_platform_control_dev.control.reprocess_requests
WHERE request_id = 'hr-smoke-reprocess-001';

SELECT entity_key, is_reprocessing, reprocess_request_id, current_watermark
FROM edw_platform_control_dev.control.watermark_state
WHERE reprocess_request_id = 'hr-smoke-reprocess-001';
```

Expect: status moves `approved` → `executing` (then `completed` / `failed` depending on pipeline hooks and trigger success).

---

## Pass / fail checklist

| Gate | Pass criteria |
|------|----------------|
| DDL | Control + HR schemas/volumes exist |
| Connect | At least one `__src` table readable |
| Apply | `config_deployments.status = success`; entities + pipeline_assets populated |
| Bronze | Final `workday_*` tables exist with tech columns |
| Silver | Matching silver / silver_restricted tables |
| Restricted | Payroll (or similar) only in `*_restricted` schemas |
| Reprocess | Dispatcher picks approved request; watermark forced; workflow triggered |
| Lint | `python scripts/lint_framework_config.py` OK |

---

## Common failures

| Symptom | Likely cause |
|---------|----------------|
| Empty bronze shell | Connect `__src` missing or wrong FQN |
| Tables in wrong schema | `restricted_scope` pipeline not run / flag wrong on entity |
| Apply MERGE fails | Control DDL out of date vs job columns (`connect_json`, etc.) |
| Dispatcher no-op | No rows with `status = 'approved'` |
| Workflow not found | Bundle not deployed; job name mismatch vs `pipeline_assets` |
| Secret errors | Scope/name mismatch vs `config/sources/workday.yaml` |

---

## Related

- [02-ingestion-patterns-connect-vs-volumes.md](02-ingestion-patterns-connect-vs-volumes.md)
- [06-control-catalog-and-metadata.md](06-control-catalog-and-metadata.md)
- [08-reprocess-and-pipeline-assets.md](08-reprocess-and-pipeline-assets.md)
- `sql/control/03_workspace_proof_checks.sql`
- `scripts/workspace_proof_preflight.py`
