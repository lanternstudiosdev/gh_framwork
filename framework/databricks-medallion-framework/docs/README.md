# Framework documentation

Operator and design guides for the Databricks Medallion Ingestion Framework.

| Document | Purpose |
|----------|---------|
| [01-how-the-framework-works.md](01-how-the-framework-works.md) | End-to-end: GitOps, control plane, runtime path |
| [02-ingestion-patterns-connect-vs-volumes.md](02-ingestion-patterns-connect-vs-volumes.md) | **When to use Lakeflow Connect vs UC Volumes** (preferred path) |
| [03-declarative-pipelines-bronze-silver-gold.md](03-declarative-pipelines-bronze-silver-gold.md) | What layers pipelines implement today |
| [04-how-to-add-a-source.md](04-how-to-add-a-source.md) | Step-by-step: new source system |
| [05-how-to-add-an-entity.md](05-how-to-add-an-entity.md) | Step-by-step: new table/report under a source |
| [06-control-catalog-and-metadata.md](06-control-catalog-and-metadata.md) | Where metadata lives and how apply works |
| [08-workspace-proof-runbook.md](08-workspace-proof-runbook.md) | **Live workspace proof** (DDL → Connect → deploy → orch) |
| [09-reprocess-and-pipeline-assets.md](09-reprocess-and-pipeline-assets.md) | Reprocess flow, schedule, pipeline_assets seed |

**Naming (locked):** data schemas only `bronze` / `bronze_restricted` / `silver` / `silver_restricted` / `gold` / `gold_restricted`. Tables use source prefix (e.g. `workday_current_employee_list`). Never `bronze_connect`.

**Restricted entities:** `restricted: true` → pipelines `hr_workday_*_restricted` (DABs `restricted_scope: "true"`).

**CI guards:** `scripts/lint_framework_config.py` + `tests/test_pipeline_registration.py` (registration plan without DLT).

**Deploy (P0):** wheel package `medallion-framework`; jobs use `spark_python_task`; orchestration under `resources.jobs` (see `bundles/databricks.yml`).

**P1 ops:** reprocess dispatcher waits for job completion → `completed`/`failed`; CI runs on all framework PRs; smoke hard-fails; Connect `__src` ownership enforced at apply.

Related (not under `docs/`):

- [../sql/control/README.md](../sql/control/README.md) — DDL bootstrap  
- [../design/uc-volume-landing.md](../design/uc-volume-landing.md) — volume path layout (file path only)  
- [../design/implementation-plan.md](../design/implementation-plan.md) — roadmap  
