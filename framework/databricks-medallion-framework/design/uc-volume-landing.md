# UC Volume Landing Architecture

## Frozen conventions

### Platform control (shared)

```text
edw_platform_control_{env}.control.*
```

Declarative + runtime metadata for all subject areas (sources, entities, load configs,
quality rules, watermarks, reprocess, deployments).

**DDL:** [`sql/control/`](../sql/control/)

### HR data catalog (first production subject)

```text
edw_hr_{env}
  bronze | bronze_restricted      ← tables e.g. workday_current_employee_list
  silver | silver_restricted
  gold | gold_restricted
  files                           ← volumes only (not a data layer schema for tables)
    landing   (external volume)  → storage .../landing/
      raw/{source_key}/{entity_name}/
      archive/{source_key}/{entity_name}/[yyyy/MM/dd]/
    published (external volume)  → out of scope for v1 pipelines
```

Do **not** create schemas like `bronze_connect`. Connect lands into `bronze` or `bronze_restricted`.

Example (dev):

```text
/Volumes/edw_hr_dev/files/landing/raw/workday/current_employee_list/
/Volumes/edw_hr_dev/files/landing/archive/workday/current_employee_list/2026/07/13/
```

Storage credential / external location wrap the container root; **pipelines never use `abfss://`**.

### Storage mapping (example dev)

```text
abfss://edw-hr-dev@<storage>.dfs.core.windows.net/
  catalog/          ← managed tables (bronze/silver/gold schemas)
  landing/          ← files.landing volume root
    raw/
    archive/
  published/        ← files.published (unused in v1)
```

Metastore objects (example):

- Storage credential: Access Connector MI
- External location: container root
- Catalog: `edw_hr_dev` (managed location under `catalog/`)
- External volume: `edw_hr_dev.files.landing` → `/landing`

### Why no subject segment under landing

Subject is already the **catalog**. Under `landing`, organize by:

`lifecycle → source_key → entity_name`

## When this document applies

**Workday production default is Lakeflow Connect** (no Volume).  
Use this landing hierarchy only for **API/file fallback** entities (`load_pattern: api_extract` / file_*).

## Extract modes

| load_pattern | Behavior |
|--------------|----------|
| `lakeflow_connect` (**Workday default**) | Connect → Bronze Delta; **no Volume** |
| `api_extract` (fallback) | `api_extract.py` → UC Volume `raw/` → Auto Loader Bronze → optional archive |

## Archive

After successful Bronze, `archive_landing.py` moves files **entity-by-entity** from `raw/` to date-partitioned `archive/`.

## Restricted tables

Entity flag `restricted: true` → target `bronze_restricted` / `silver_restricted` (config-driven).  
Landing still uses the same `files.landing` volume unless compliance later requires a split volume.

## Secrets

```yaml
connection:
  secret_scope: "kv-hr-dev"
  secrets:
    client_id: "workday-client-id"
    client_secret: "workday-client-secret"
```

- **In Git:** scope name, secret names, endpoints, params, volume identity.
- **In Key Vault only:** passwords, tokens, client secrets, refresh tokens.

## Related SQL

| Script | Purpose |
|--------|---------|
| `sql/control/00_create_catalog_and_schema.sql` | Control catalog |
| `sql/control/01_control_tables.sql` | All control tables |
| `sql/control/02_hr_data_catalog_skeleton.sql` | HR schemas + volumes |
