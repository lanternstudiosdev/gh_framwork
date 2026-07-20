# Medallion framework samples

Demos aligned with **production framework conventions**:

| Convention | Value |
|------------|--------|
| Control catalog | `edw_platform_control_dev` |
| HR catalog | `edw_hr_dev` |
| Schemas | `bronze` / `bronze_restricted` / `silver` / `silver_restricted` / `gold` / `gold_restricted` |
| Table names | Source prefix: `workday_*`, `dynamics365_*` |
| HR default ingest | **Lakeflow Connect** → `{table}__src` → framework bronze `{table}` → silver |
| Volume path | Fallback / file demos only: `/Volumes/.../files/landing/raw/{source}/{entity}/` |

---

## Layout

```text
samples/
  config/           # sources, entities (Connect-first for Workday)
  sql/control/      # Same control DDL shape as production
  data/landing/     # Seeds; generator writes raw/{source}/{entity}/
  pipelines/        # Bronze / Silver / Gold examples
  scripts/          # generate_sample_data.py, dynamics_api_simulator.py
  tests/            # DW checks (edw_hr_dev / edw_sales_dev)
```

---

## Prerequisites

1. Unity Catalog + Access Connector + external location for HR (and sales if used).
2. Run `samples/sql/control/00` → `01` → `02` (edit storage LOCATION URLs).
3. Secret scope for demos (`kv-hr-dev` names match config).
4. For **Connect path**: provision Lakeflow Connect to write  
   `edw_hr_dev.bronze.workday_employees__src` (and departments).  
   Framework bronze owns `edw_hr_dev.bronze.workday_employees`.
5. For **Volume fallback demos**: copy seed CSVs into  
   `/Volumes/edw_hr_dev/files/landing/raw/workday/employees/` etc.

---

## Ownership model (matches framework)

```text
Lakeflow Connect  →  bronze.workday_employees__src   (Connect owns)
Framework DLT     →  bronze.workday_employees        (framework owns)
Silver DLT        →  silver.workday_employees
```

Do **not** use a `bronze_connect` schema.

---

## Run options

### A) Connect-first (production-like)

1. DDL + Config Apply (`config_root=samples/config` or production `config/`).
2. Run Connect for Workday objects into `__src` tables.
3. Run bronze then silver (or subject orchestration).

### B) Volume fallback (local file demo)

1. `python samples/scripts/generate_sample_data.py`
2. Copy `data/landing/raw/workday/...` into the UC Volume.
3. Run `pipelines/bronze/hr_employees_bronze.py` then silver.

### C) Optional DW star-schema demos (not framework parity)

`pipelines/gold/` are **optional analytics samples** (SCD2, facts, calendar gap tests).  
They are **not** the production medallion path and are **not** registered in DABs.  
Use only after loading generator data; do not treat as required framework steps.

---

## Generator paths

```bash
cd samples/scripts
pip install pandas faker
python generate_sample_data.py
```

Outputs under `data/landing/raw/workday/...` and `raw/dynamics365/...` for volume copy.

---

## Related docs

- Framework: [../README.md](../README.md)
- Operator guides: [../docs/README.md](../docs/README.md)
- Connect vs Volumes: [../docs/02-ingestion-patterns-connect-vs-volumes.md](../docs/02-ingestion-patterns-connect-vs-volumes.md)
- Control DDL: [sql/control/README.md](sql/control/README.md)
