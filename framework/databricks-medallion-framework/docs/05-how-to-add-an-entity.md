# How to add an entity (table / report) under a source

An **entity** is one logical dataset: a Workday report, a SQL table, a file feed, etc.  
For HR under Workday, this is usually “one more report” with **config only**—no new pipeline file if the subject’s Bronze/Silver pipelines already load entities dynamically.

---

## Prerequisites

- Source exists in `config/sources/` (see [04-how-to-add-a-source.md](04-how-to-add-a-source.md)).
- Subject area YAML exists (`config/subject_areas/`).
- You know **load pattern**: Connect vs API/file ([02-ingestion-patterns-connect-vs-volumes.md](02-ingestion-patterns-connect-vs-volumes.md)).

---

## Steps (existing source, e.g. Workday)

### 1. Choose stable names

| Field | Example | Notes |
|-------|---------|--------|
| `entity_key` | `hr_worker_skills` | Globally unique in control |
| `entity_name` | `worker_skills` | Path segment under Volume `raw/{source_key}/` |
| `source_object` | `WorkerSkills` | Report/table name in source system |
| `source_key` | `workday` | Must match sources YAML |
| `primary_key_columns` | `["worker_id", "skill_id"]` | Used in Silver dedupe/expectations |

### 2. Pick load_pattern

| Prefer | Set |
|--------|-----|
| **Workday / Connect (default)** | `load_pattern: lakeflow_connect` + `lakeflow_connect_config` |
| API / file fallback only | `load_pattern: api_extract` + `api` + auto_loader options |

### 3. Add entity block to subject entities file

Path: `config/entities/{subject}.yaml` (e.g. `hr.yaml`)

**Naming rules**

| Rule | Example |
|------|---------|
| Schemas only | `bronze`, `bronze_restricted`, `silver`, `silver_restricted`, `gold`, `gold_restricted` |
| No intermediate schemas | Never `bronze_connect`, `raw`, etc. |
| Table names | `{source_key}_{entity_name}` → `workday_current_employee_list` |
| Restricted data | Same table name; schema `bronze_restricted` / `silver_restricted` |

**Connect example (default for Workday):**

```yaml
  - entity_key: hr_new_table
    source_key: workday
    entity_name: new_table
    source_object: NewTable
    load_pattern: lakeflow_connect
    primary_key_columns: ["id"]
    watermark_column: "_source_extract_ts"
    target_bronze_table: workday_new_table
    target_silver_table: workday_new_table
    restricted: false
    supports_full_reprocess: true
    is_active: true
    load_config:
      lakeflow_connect_config:
        mode: incremental
        source_object: NewTable
        target_schema: bronze
        connection_name: workday_connect   # optional — inherited from source connect.connection_name if omitted
        bronze_writer: framework_dlt
        connect_writer: lakeflow_connect
        # connect_output_table is DERIVED by Config Apply
        # ({data_catalog}.bronze.workday_new_table__src) — do not author it here.
```

**API / Volume fallback example:**

```yaml
  - entity_key: hr_new_table
    source_key: workday
    entity_name: new_table
    source_object: NewTable
    load_pattern: api_extract
    primary_key_columns: ["id"]
    target_bronze_table: workday_new_table
    target_silver_table: workday_new_table
    is_active: true
    load_config:
      api:
        endpoint_path: "/customreport2/{tenant}/NewTable"
        http_method: GET
        params:
          format: json
      auto_loader_options:
        cloudFiles.format: json
        cloudFiles.schemaEvolutionMode: addNewColumns
```

Volume paths apply **only** on the fallback path:

```text
/Volumes/edw_hr_{env}/files/landing/raw/{source_key}/{entity_name}/
```

### 4. Optional env overlay

In `config/entities/hr.dev.yaml` (example):

```yaml
entities:
  - entity_key: hr_new_table
    load_pattern: api_extract    # override prod Connect
    load_config:
      api:
        params:
          format: json
```

### 5. Optional quality rules / policies / contract

- `config/quality_rules/hr/new_table.yaml` — Silver expectations  
- `config/column_policies/...` — PII hash/encrypt/mask  
- `config/contracts/...` — published schema contract  

Not required for a minimal Bronze land.

### 6. Apply config

```bash
databricks bundle deploy --target dev_personal
databricks bundle run apply_control_config --target dev_personal
```

Verify:

```sql
SELECT entity_key, source_key, load_pattern, restricted
FROM edw_platform_control_dev.control.source_entities
WHERE entity_key = 'hr_new_table';

SELECT entity_key, environment, landing_volume_path, api_config
FROM edw_platform_control_dev.control.entity_load_configs
WHERE entity_key = 'hr_new_table';
```

### 7. Run ingest

**Connect path (default Workday)**

1. Provision Lakeflow Connect for the object; set `raw_table` FQN correctly.  
2. Run **`hr_workday_orchestration`** (or bronze then silver).  

**API / Volume fallback**

1. Set `load_pattern: api_extract` (entity or overlay).  
2. Run **`hr_workday_api_fallback_orchestration`** (or extract → bronze → archive → silver).

### 8. Confirm no new pipeline code (usual case)

HR Bronze/Silver loop entities from control for the subject.  
**New entity under the same subject/source ⇒ config only.**

New **subject area** or non-dynamic pipeline design ⇒ new pipeline modules + DAB entries (see HR as template).

---

## Checklist

- [ ] `entity_key` unique; `source_key` exists  
- [ ] `load_pattern` chosen (Connect preferred)  
- [ ] PKs and target bronze/silver table names set  
- [ ] `restricted: true` if data goes to `*_restricted` schemas  
- [ ] API params or Connect config complete  
- [ ] Optional rules/policies  
- [ ] Config Apply + control table verification  
- [ ] Smoke Bronze (+ Silver)  

---

## Common mistakes

| Mistake | Fix |
|---------|-----|
| New `.py` pipeline per Workday report | Prefer config; use dynamic HR pipelines |
| Forcing Volume when Connect works | Keep `lakeflow_connect`; use primary orchestration |
| Wrong `entity_name` | Breaks Volume path and clarity; keep stable, filesystem-safe |
| Only YAML, never Apply | Pipelines read **control tables**, not Git files at runtime |
| Expecting Gold automatically | Gold not wired for HR yet ([03-…](03-declarative-pipelines-bronze-silver-gold.md)) |
