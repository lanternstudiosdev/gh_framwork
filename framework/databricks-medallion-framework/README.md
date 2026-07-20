# Databricks Medallion Ingestion Framework

GitOps-driven medallion (Bronze → Silver → Gold) platform for Azure Databricks.

**First production subject area: HR (Workday).**  
Landing uses **Unity Catalog External Volumes** (not ad-hoc `abfss://` paths in pipeline code).  
Platform metadata lives in a **separate control catalog**.

## Quick facts

| Topic | Convention |
|-------|------------|
| Control catalog | `edw_platform_control_{env}` |
| HR data catalog | `edw_hr_{env}` |
| Data schemas only | `bronze`, `bronze_restricted`, `silver`, `silver_restricted`, `gold`, `gold_restricted` |
| Table naming | `{source_key}_{entity_name}` e.g. `workday_current_employee_list` |
| Landing volume | `/Volumes/edw_hr_{env}/files/landing/` (API/file fallback only) |
| Raw path | `raw/{source_key}/{entity_name}/` |
| Archive path | `archive/{source_key}/{entity_name}/yyyy/MM/dd/` |
| Default Workday ingest | **Lakeflow Connect** → Bronze Delta → Silver |
| Alternate / fallback | `api_extract` → UC Volume → Auto Loader (env/entity override) |
| Secrets in Git | Secret scope + secret **names** only |
| Secrets in Key Vault | Passwords, tokens, client secrets, refresh tokens |

## Repository layout

```text
config/                 # GitOps YAML (sources, entities, quality, contracts, …)
sql/control/            # DDL for control + HR catalog skeleton
src/lib/                # metadata, volumes, security, expectations
src/jobs/               # apply_control_config, api_extract, archive_landing, reprocess
src/pipelines/hr/       # Production HR Bronze + Silver (dynamic multi-entity)
src/pipelines/…         # Generic bronze/silver entry modules (sales, refdata)
bundles/databricks.yml  # DABs: HR orchestration, jobs, pipelines
samples/                # Runnable demos (same volume + control model)
design/                 # Architecture notes and roadmap
tests/                  # Unit tests (volume helpers, libs)
```

## End-to-end flow (HR / Workday — Connect default)

```text
1. DDL             sql/control/*.sql
2. Connect setup   Lakeflow Connect connection + objects for Workday
3. Deploy          databricks bundle deploy --target dev
4. Config apply    databricks bundle run apply_control_config --target dev
5. Bronze          hr_workday_bronze (+ hr_workday_bronze_restricted)
6. Silver          hr_workday_silver (+ hr_workday_silver_restricted)
```

Primary orchestration: **`hr_workday_orchestration`**  
(standard + restricted bronze in parallel → matching silver).  
API/Volume fallback: **`hr_workday_api_fallback_orchestration`**.

## Ingestion preference

**Workday default: Lakeflow Connect → Bronze Delta → Silver** (no Volume).  
UC Volumes + Auto Loader only when an entity/env uses `api_extract` / file patterns.

See [docs/02-ingestion-patterns-connect-vs-volumes.md](docs/02-ingestion-patterns-connect-vs-volumes.md).

## Config-driven sources

- **Sources** (`config/sources/`): connection, secret **names**, extract defaults.
- **Entities** (`config/entities/hr.yaml`): tables/reports, `load_pattern`, Connect or API config.
- **Env overlays**: switch Connect vs API per environment.

Adding an entity under an existing dynamic subject is usually **config only** (no new pipeline file).

## Documentation

**Start here — operator guides (`docs/`):**

| Doc | Contents |
|-----|----------|
| [docs/README.md](docs/README.md) | Doc index |
| [docs/01-how-the-framework-works.md](docs/01-how-the-framework-works.md) | End-to-end how it works |
| [docs/02-ingestion-patterns-connect-vs-volumes.md](docs/02-ingestion-patterns-connect-vs-volumes.md) | Connect vs Volumes |
| [docs/03-declarative-pipelines-bronze-silver-gold.md](docs/03-declarative-pipelines-bronze-silver-gold.md) | Bronze / Silver / Gold wiring |
| [docs/04-how-to-add-a-source.md](docs/04-how-to-add-a-source.md) | Add a source (step-by-step) |
| [docs/05-how-to-add-an-entity.md](docs/05-how-to-add-an-entity.md) | Add a table/entity (step-by-step) |
| [docs/06-control-catalog-and-metadata.md](docs/06-control-catalog-and-metadata.md) | Control catalog map |
| [docs/08-workspace-proof-runbook.md](docs/08-workspace-proof-runbook.md) | Live workspace proof |
| [docs/09-reprocess-and-pipeline-assets.md](docs/09-reprocess-and-pipeline-assets.md) | Reprocess + pipeline_assets |

**Also:**

| Doc | Contents |
|-----|----------|
| [design/README.md](design/README.md) | Design index |
| [design/uc-volume-landing.md](design/uc-volume-landing.md) | Volume path layout (file path) |
| [sql/control/README.md](sql/control/README.md) | Control DDL runbook |
| [design/implementation-plan.md](design/implementation-plan.md) | Roadmap |
| [samples/README.md](samples/README.md) | Samples |
| [config/reprocess_requests/README.md](config/reprocess_requests/README.md) | Reprocess-as-code |

## Prerequisites

1. Unity Catalog + Access Connector (storage credential) + external location for HR container.
2. Control and HR catalogs/volumes from `sql/control/`.
3. Key Vault–backed secret scope (e.g. `kv-hr-dev`) with Workday credential secret **names** matching `config/sources/workday.yaml`.
4. Databricks CLI + GitHub secrets `DATABRICKS_HOST` / `DATABRICKS_TOKEN` for CI deploy.

## Local tests

```bash
cd framework/databricks-medallion-framework
pip install pytest pyyaml
python -m pytest tests/test_volumes.py -v
```

## Version

Bundle version **0.4.2** — Connect-first HR, control catalog, restricted schemas, wheel-based DAB deploy (P0).
