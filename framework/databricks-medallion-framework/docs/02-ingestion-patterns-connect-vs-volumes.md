# Ingestion patterns: Lakeflow Connect vs UC Volumes

## Decision (current)

**Workday / HR uses Lakeflow Connect by default.**

### Ownership (no double-write on the same table)

```text
Connect owns:       {data_catalog}.{bronze|bronze_restricted}.{workday_*}__src
Framework DLT owns: {data_catalog}.{bronze|bronze_restricted}.{workday_*}     ← final bronze
Silver DLT owns:    {data_catalog}.{silver|silver_restricted}.{workday_*}
```

- `{data_catalog}` is resolved at **Config Apply** (`edw_hr_dev`, `edw_hr_qat`, …) from placeholders — not hardcoded in base YAML.
- Staging uses `__src` suffix in the **same medallion schema** (never a `bronze_connect` schema).
- **Connect product config must target `__src` tables**, not the final bronze name.  
  Config Apply rewrites final-name destinations to `__src` when it detects a conflict.
- If Connect is pointed at `bronze.workday_foo` (final), DLT and Connect will fight for the same table.

```text
Workday → Connect → bronze.workday_*__src → Bronze DLT → bronze.workday_* → Silver
```

**No UC Volume** on the primary path. Volumes remain a **fallback** when `load_pattern: api_extract` / file_* or Connect is unavailable.

---

## Pattern comparison

| | **Lakeflow Connect (default for Workday)** | **UC Volume + Auto Loader (fallback)** |
|--|--------------------------------------------|----------------------------------------|
| **When** | Workday via Connect (and other supported connectors) | API/RaaS dump, partner files, Connect unavailable |
| **Landing** | `{table}__src` in bronze / bronze_restricted | `/Volumes/.../raw/{source}/{entity}/` |
| **Bronze final** | Framework DLT owns `{table}` | Framework DLT owns `{table}` |
| **Config** | `lakeflow_connect_config` (Connect `__src` target auto-derived by Config Apply) | `api` + auto_loader options |
| **Archive** | N/A | `raw/` → `archive/` after successful Bronze |
| **Orchestration** | `hr_workday_orchestration` (bronze → silver) | `hr_workday_api_fallback_orchestration` |

---

## Recommended decision flow

```text
Does Lakeflow Connect support this source in this environment?
  │
  ├─ YES → load_pattern: lakeflow_connect   ← Workday default
  │         Connect → Bronze Delta → Silver (+ Gold later)
  │
  └─ NO  → load_pattern: api_extract | file_*
            Extract → UC Volume → Auto Loader Bronze → Silver
```

Use **env overlays** only when you must diverge (e.g. emergency API fallback for one entity).

---

## Are files a general best practice first?

**No.** Prefer Connect → Bronze Delta when available.  
Staging files is appropriate for true file sources or when Connect cannot be used—not a mandatory hop for Workday.

UC Volumes govern the *file* path (permissions, catalog-scoped paths)—they do not replace Connect.

---

## How the framework implements this

| Component | Connect (Workday default) | Volume / API fallback |
|-----------|---------------------------|------------------------|
| `config/sources/workday.yaml` | `default_load_pattern: lakeflow_connect` | `connection` + `extract_defaults` retained |
| `config/entities/hr.yaml` | All entities `lakeflow_connect` + `lakeflow_connect_config` | Optional `api` + auto_loader still in YAML |
| `src/pipelines/bronze_entry.py` | Reads `__src` → owns final bronze | Auto Loader if pattern is file/api |
| `src/jobs/api_extract.py` | Skips Connect entities | Writes Volume `raw/` |
| `hr_workday_orchestration` | bronze → silver | — |
| `hr_workday_api_fallback_orchestration` | — | extract → bronze → archive → silver |

---

## Config snippets

**Primary (default):**

```yaml
load_pattern: lakeflow_connect
target_bronze_table: workday_current_employee_list
target_silver_table: workday_current_employee_list
load_config:
  lakeflow_connect_config:
    mode: incremental
    source_object: CurrentEmployeeList
    target_schema: bronze
    connection_name: workday_connect   # optional — inherited from the source's connect.connection_name if omitted
    bronze_writer: framework_dlt
    connect_writer: lakeflow_connect
    # NOTE: connect_output_table is NOT authored here. Config Apply DERIVES and
    # stores it as {data_catalog}.{bronze|bronze_restricted}.{table}__src
    # (e.g. edw_hr_dev.bronze.workday_current_employee_list__src).
```

**Fallback overlay (only if needed):**

```yaml
# config/entities/hr.dev.yaml
entities:
  - entity_key: hr_current_employee_list
    load_pattern: api_extract
```

---

## Summary

| Question | Answer |
|----------|--------|
| Workday default? | **Lakeflow Connect** |
| Skip Volumes on Connect path? | **Yes** |
| Must write files first? | **No** |
| When Volumes? | API/file fallback only |
