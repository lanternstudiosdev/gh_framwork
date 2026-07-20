# Control catalog and metadata

## Catalog

```text
edw_platform_control_{env}
  └── control
        ├── sources
        ├── subject_areas
        ├── source_entities
        ├── entity_load_configs
        ├── data_contracts
        ├── quality_rules
        ├── column_policies
        ├── pipeline_assets
        ├── reprocess_requests
        ├── watermark_state          -- runtime
        └── config_deployments       -- audit
```

DDL: [`sql/control/`](../sql/control/).

**Why separate from `edw_hr_{env}`?**  
One GitOps control plane for all subjects; domain data stays in subject catalogs.

---

## Git → control mapping

| Git path | Control table(s) |
|----------|------------------|
| `config/sources/*.yaml` | `sources` |
| `config/subject_areas/*.yaml` | `subject_areas` |
| `config/entities/*.yaml` (+ env overlays) | `source_entities`, `entity_load_configs` |
| `config/contracts/` | `data_contracts` |
| `config/quality_rules/` | `quality_rules` |
| `config/column_policies/` | `column_policies` |
| `config/reprocess_requests/` | `reprocess_requests` |

Apply job: `src/jobs/apply_control_config.py`  
Run: `databricks bundle run apply_control_config --target {env}`

---

## Runtime readers

| Consumer | Reads |
|----------|--------|
| Bronze (Connect) | entities + `lakeflow_connect_config` / `raw_table` |
| `api_extract` (fallback) | `sources`, entities, load configs, Volume paths |
| Bronze / Silver pipelines | entities, load configs, quality_rules, column_policies, watermarks |
| `archive_landing` | Volume entity paths (fallback only) |
| `reprocess_dispatcher` | `reprocess_requests`, watermarks, optional `pipeline_assets` |

Helpers: `src/lib/metadata.py`, `src/lib/volumes.py` (paths for **file** landing only).

---

## Provenance

Declarative rows get:

- `last_applied_git_commit_sha`
- `last_applied_ts`
- `last_applied_deployment_id`

Each apply also writes `config_deployments`.

`watermark_state` is **runtime** (pipelines/dispatcher), not bulk-replaced by apply.

---

## See also

- [01-how-the-framework-works.md](01-how-the-framework-works.md)  
- [design/metadata/table-updates.md](../design/metadata/table-updates.md)  
