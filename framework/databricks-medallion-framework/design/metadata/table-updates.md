# Control metadata model

All tables live in **`{control_catalog}.control`** where  
`control_catalog = edw_platform_control_{env}`.

**Executable DDL (source of truth for column lists):**

- Framework: [`sql/control/02_control_tables.sql`](../../sql/control/02_control_tables.sql)
- Samples: [`samples/sql/control/02_control_tables.sql`](../../samples/sql/control/02_control_tables.sql)

This document summarizes purpose and usage. Prefer the SQL files when provisioning.

---

## Table inventory

| Table | Kind | Purpose |
|-------|------|---------|
| `config_deployments` | Audit | Each GitOps apply run |
| `subject_areas` | Declarative | Catalog/volume defaults per domain |
| `sources` | Declarative | Connection + extract defaults (secret names) |
| `source_entities` | Declarative | Logical entities, PKs, restricted flag, load_pattern |
| `entity_load_configs` | Declarative | Per-env volume paths, API config, Auto Loader, Connect |
| `data_contracts` | Declarative | Contract JSON documents |
| `quality_rules` | Declarative | Hybrid expectations metadata |
| `column_policies` | Declarative | Sparse encrypt/hash/mask policies |
| `pipeline_assets` | Declarative | Entity/subject → DABs pipeline/workflow routing |
| `reprocess_requests` | Declarative + runtime | Reprocess-as-code + status |
| `watermark_state` | **Runtime** | Incremental watermarks / reprocess flags |

Complex fields are stored as **JSON STRING** so Config Apply can upsert easily; `lib.metadata` deserializes them.

---

## entity_load_configs (highlights)

Per entity + environment:

- `landing_volume` (JSON): `volume_catalog`, `volume_schema`, `volume_name`
- `landing_volume_path`: resolved `/Volumes/.../raw/{source}/{entity}`
- `archive_volume_path` / `archive_subpath`: post-Bronze archive target
- `api_config` (JSON): `endpoint_path`, `http_method`, free-form `params` map
- `auto_loader_options` (JSON): `cloudFiles.*`
- `lakeflow_connect_config` (JSON): when `load_pattern = lakeflow_connect`

Env overlays (`hr.dev.yaml`) merge onto base entities and write `environment = dev` (plus `all` fallback).

---

## pipeline_assets

Maps logical flows to DABs resources for reprocess dispatch:

- `asset_type`: `lakeflow_pipeline` | `workflow` | `job`
- `asset_name`: e.g. `hr_workday_orchestration`
- `supports_reprocess`, `depends_on`, `parameters` (JSON)

Populate via future config YAML or seed after first deploy. Dispatcher falls back to name conventions (`hr` → `hr_workday_orchestration`) until rows exist.

---

## Landing path resolution

Hybrid model (see `lib.volumes`):

1. Subject/env supplies `volume_catalog` / `volume_schema` / `volume_name`
2. Entity supplies `source_key` + `entity_name` (or explicit `landing_subpath`)
3. Apply job writes full `landing_volume_path` into control tables
4. Extract + Bronze read that path (never raw container URLs)

---

## Provenance

Declarative tables receive on every apply:

- `last_applied_git_commit_sha`
- `last_applied_ts`
- `last_applied_deployment_id`

`config_deployments` records overall success/failure.
