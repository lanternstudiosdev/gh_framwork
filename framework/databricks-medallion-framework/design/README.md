# Design artifacts

Architecture and roadmap for the Databricks Medallion Ingestion Framework.

## Locked decisions

| Topic | Decision |
|-------|----------|
| Landing | **UC External Volumes** under `{subject_catalog}.files.landing` |
| Path layout | `raw/{source_key}/{entity_name}/` and `archive/{source_key}/{entity_name}/[yyyy/MM/dd]/` |
| No subject under volume | Subject is already the **catalog** (`edw_hr_{env}`) |
| Control plane | **Separate catalog** `edw_platform_control_{env}` (not a schema under HR) |
| First subject | **HR** (13 Workday entities) → Bronze + Silver |
| Workday default | **`lakeflow_connect`** → Bronze Delta → Silver |
| API / Volume | Fallback via entity/env `load_pattern: api_extract` |
| Restricted data | Entity flag `restricted: true` → `bronze_restricted` / `silver_restricted` / `gold_restricted` only |
| Table naming | `{source_key}_{entity_name}` e.g. `workday_current_employee_list` (no `bronze_connect` schema) |
| Published volume | Present in UC layout; **ignored by framework v1** |
| Secrets | Scope + names in Git; values in Key Vault only |

## Operator docs (preferred entry)

Step-by-step how the framework works, Connect vs Volumes, and how to add sources/entities:

→ **[../docs/README.md](../docs/README.md)**

## Design documents

| Document | Description |
|----------|-------------|
| [uc-volume-landing.md](uc-volume-landing.md) | Landing volume hierarchy (file/API path only) |
| [metadata/table-updates.md](metadata/table-updates.md) | Control table field notes (see also executable DDL) |
| [implementation-plan.md](implementation-plan.md) | Phased roadmap and remaining work |
| [config-apply/README.md](config-apply/README.md) | Points to runtime apply job (stale sketch archived) |
| [_archive/README.md](_archive/README.md) | Stale design files — do not use at runtime |
| [../sql/control/README.md](../sql/control/README.md) | **Executable** control + HR catalog DDL |

## Code map

| Path | Role |
|------|------|
| `config/sources/` | Connection + variable extract defaults |
| `config/entities/hr.yaml` | Production HR entity list |
| `config/entities/hr.dev.yaml` | Dev overlay (catalogs, optional Connect) |
| `config/subject_areas/hr.yaml` | Subject defaults |
| `sql/control/` | DDL bootstrap |
| `src/lib/volumes.py` | Path resolution |
| `src/lib/config_merge.py` | Env overlay merge |
| `src/jobs/apply_control_config.py` | GitOps upsert into control tables |
| `src/jobs/api_extract.py` | Config-driven HTTP extract to Volume |
| `src/jobs/archive_landing.py` | Post-Bronze archive move |
| `src/pipelines/hr/*` | Dynamic HR Bronze + Silver |
| `bundles/databricks.yml` | DABs resources |

## Historical design examples

`design/config-examples/` and `design/pipelines/` retain earlier hr-oriented sketches. Prefer production paths under `config/` and `src/` for current behavior. Bronze no longer targets ADLS container URLs in application code; storage is behind External Location + Volume.
