# How to add a new source

A **source** is a system connection (Workday, SQL Server, Dynamics, etc.): auth, base URLs, secret names, and default extract behavior. Entities (tables/reports) hang off a `source_key`.

---

## Prerequisites

- Control catalog DDL applied (`sql/control/`).
- Subject area exists or will be created (`config/subject_areas/`).
- Secret scope in Databricks (Key Vault–backed); **values** only in Key Vault; **names** in Git.

---

## Steps

### 1. Choose keys

| Field | Example | Notes |
|-------|---------|--------|
| `source_key` | `workday`, `erp_sql_server` | Stable id used by entities |
| `source_type` | `rest_api`, `sql`, `file` | Documentation + future routing |
| `subject_area_key` | `hr`, `sales` | Owning domain |
| `default_load_pattern` | `lakeflow_connect` (Workday default) or `api_extract` | Connect first |

### 2. Create source YAML

Path: `config/sources/{source_key}.yaml`

```yaml
source_key: my_source
source_type: rest_api          # or sql, etc.
subject_area_key: hr
description: Short description
default_load_pattern: lakeflow_connect

connect:
  connection_name: my_source_connect
  connector_type: workday   # or other Connect connector type
  defaults:
    mode: incremental

connection:
  # API fallback only (optional if Connect-only forever)
  base_url: "https://api.example.com"
  auth_type: oauth2_client_credentials
  secret_scope: "kv-hr-dev"
  secrets:
    client_id: "mysource-client-id"
    client_secret: "mysource-client-secret"
  token_url: "https://api.example.com/oauth/token"

extract_defaults:
  http_method: GET
  response_format: json
  timeout_seconds: 120
  common_params: {}
```

Connect credentials are usually configured on the Databricks **connection** object; still put secret **names** in Git when the framework or jobs need them.

### Source-level defaults (inheritance)

Config Apply merges source-level defaults into every entity under the source so
entities stay lean (entity values always win on conflict):

| Source YAML block | Inherited by entities | Entity may still… |
|-------------------|-----------------------|--------------------|
| `connect.connection_name` | Used when an entity omits `connection_name` | Override per entity |
| `connect.defaults` (e.g. `mode: incremental`) | Merged into each `lakeflow_connect_config` | Override any key |
| `load_defaults.auto_loader_options` | Merged into each entity `load_config.auto_loader_options` | Override any key |

```yaml
connect:
  connection_name: my_source_connect   # entities can omit connection_name and inherit this
  defaults:
    mode: incremental                  # merged into every entity's lakeflow_connect_config

# Optional: shared Auto Loader options for file/api entities under this source
load_defaults:
  auto_loader_options:
    cloudFiles.format: json
    cloudFiles.schemaEvolutionMode: addNewColumns
```

> `connect_output_table` is **not** authored on entities — Config Apply derives it as
> `{data_catalog}.{bronze|bronze_restricted}.{table}__src`. See
> [02-ingestion-patterns-connect-vs-volumes.md](02-ingestion-patterns-connect-vs-volumes.md).

### 3. Optional env overlay

Path: `config/sources/{source_key}.dev.yaml` (also `.qat.yaml`, `.prod.yaml`)

```yaml
source_key: my_source
connection:
  secret_scope: "kv-hr-dev"
  base_url: "https://dev-api.example.com"
default_load_pattern: lakeflow_connect
connect:
  connection_name: my_source_connect_dev
```

### 4. Create Key Vault secrets

Create secrets whose **names** match `connection.secrets.*`.  
Do not put secret **values** in Git.

### 5. Apply config

```bash
databricks bundle deploy --target dev
databricks bundle run apply_control_config --target dev
```

Verify:

```sql
SELECT * FROM edw_platform_control_dev.control.sources
WHERE source_key = 'my_source';
```

### 6. Add entities

Each table/report under this source is an entity with `source_key: my_source`.  
See [05-how-to-add-an-entity.md](05-how-to-add-an-entity.md).

### 7. Wire orchestration (if new subject)

- New subject area → subject catalog DDL + new `resources.pipelines` in `bundles/databricks.yml` that **reuse the generic entry modules** `src/pipelines/bronze_entry.py` / `src/pipelines/silver_entry.py` (set `subject_area_key` / `source_key` in each pipeline's `configuration`) + an orchestration job. **No new pipeline `.py` file is needed** — see the `sales_*` (Dynamics 365) and `refdata_*` (SQL) resources for worked examples.
- Existing subject (e.g. another HR source) → often **no new pipeline** if Bronze/Silver already register by `source_key` / subject; confirm pipeline filters.

---

## Checklist

- [ ] `config/sources/{source_key}.yaml`
- [ ] Env overlay if endpoints/scopes differ by env
- [ ] Secret scope + secret names in KV
- [ ] Prefer `lakeflow_connect` when possible
- [ ] Config Apply succeeded
- [ ] Row visible in `control.sources`
- [ ] At least one entity YAML + smoke ingest

---

## Common mistakes

| Mistake | Fix |
|---------|-----|
| Secret **values** in YAML | Only names; values in Key Vault |
| New source but no entities | Sources alone do not create tables |
| Assuming Volume is required | Connect path skips Volumes ([02-…](02-ingestion-patterns-connect-vs-volumes.md)) |
| Forgetting env overlay | Prod Connect / dev API needs `*.dev.yaml` / `*.prod.yaml` |
