# How the framework works

## Purpose

A **config-driven** medallion platform on Azure Databricks:

1. **Declare** sources, entities, quality rules, and policies in Git (`config/`).
2. **Apply** that config into a shared **platform control catalog**.
3. **Ingest** to Bronze — **Lakeflow Connect for Workday by default**; API/file → Volume only as fallback.
4. **Transform** with modular Spark Declarative Pipelines for Silver (Gold optional per subject).
5. **Operate** reprocess, watermarks, and audit via control tables + DABs jobs.

---

## Two catalogs

| Catalog | Role |
|---------|------|
| `edw_platform_control_{env}` | Metadata: sources, entities, load configs, rules, watermarks, reprocess |
| `edw_hr_{env}` (etc.) | **Data** schemas only: `bronze`, `bronze_restricted`, `silver`, `silver_restricted`, `gold`, `gold_restricted` (+ optional `files` for volumes). Tables use source prefix (e.g. `workday_current_employee_list`). |

---

## Runtime path

### Primary — Workday / Lakeflow Connect

```text
Git config/  →  apply_control_config  →  control.*  (resolves {data_catalog} FQNs)
                                              │
Workday → Connect → bronze.workday_*__src  (Connect owns)
                 → Bronze DLT → bronze.workday_*  (framework owns)
                 → Silver DLT → silver.workday_*
```

Orchestration: **`hr_workday_orchestration`** = bronze → silver.

### Fallback — API / file

```text
API/file → api_extract → UC Volume raw/ → Bronze Auto Loader → archive → Silver
```

Orchestration: **`hr_workday_api_fallback_orchestration`**.

Details: [02-ingestion-patterns-connect-vs-volumes.md](02-ingestion-patterns-connect-vs-volumes.md).

---

## GitOps apply

1. Change YAML under `config/`.
2. Deploy + run **`apply_control_config`**.
3. Pipelines/jobs read **control tables** (not Git files) at runtime.

---

## Orchestration resources (HR)

| Resource | Role |
|----------|------|
| `hr_workday_bronze` | Bronze (`restricted=false`) → schema `bronze` |
| `hr_workday_bronze_restricted` | Bronze (`restricted=true`) → schema `bronze_restricted` |
| `hr_workday_silver` | Silver standard → schema `silver` |
| `hr_workday_silver_restricted` | Silver restricted → schema `silver_restricted` |
| `hr_workday_orchestration` | **Primary:** bronze(+restricted) → silver(+restricted) |
| `hr_workday_api_extract` | Optional API extract job |
| `hr_archive_landing` | Optional post-Volume archive |
| `hr_workday_api_fallback_orchestration` | Optional full file path |

---

## Where metadata lives

| What | Git path |
|------|----------|
| Source + Connect defaults | `config/sources/{source}.yaml` |
| Entities | `config/entities/{subject}.yaml` |
| Env overlays | `*.{env}.yaml` |
| Subject catalogs | `config/subject_areas/` |
| Rules / policies / contracts | `config/quality_rules/`, `column_policies/`, `contracts/` |

- [04-how-to-add-a-source.md](04-how-to-add-a-source.md)  
- [05-how-to-add-an-entity.md](05-how-to-add-an-entity.md)  

---

## Conventions the apply job enforces

- **Derived Connect target:** entities do **not** author `connect_output_table`.
  Config Apply derives and stores it as
  `{data_catalog}.{bronze|bronze_restricted}.{table}__src` (the Connect `__src`
  staging table); framework DLT owns the final bronze table.
- **Source-level inheritance:** `connect.connection_name`, `connect.defaults`, and
  `load_defaults.auto_loader_options` on the source flow down to every entity
  (entity values win). Keeps entity YAML lean — see
  [04-how-to-add-a-source.md](04-how-to-add-a-source.md).
- **No hardcoded subject fallback:** catalog resolution requires a concrete
  `data_catalog` or `subject_area_key`; it raises rather than silently defaulting
  to `hr`. New subjects (refdata, sales) must set their subject.

## Security note

Column policies (`config/column_policies/`) apply encryption / masking / hashing to
sensitive columns during Silver. If the encryption-key secret cannot be resolved
(`lib/security.py`), the pipeline **fails the table build** instead of emitting
cleartext — policy failures are never silently skipped.

---

## Status snapshot

| Capability | State |
|------------|--------|
| Workday default = Connect | **Yes** (config + bronze + primary orchestration) |
| API/Volume fallback | Supported, optional orchestration |
| HR Silver | Yes |
| HR Gold | Not production-wired yet |
| Live workspace E2E | Requires Connect connection + DDL + deploy |
