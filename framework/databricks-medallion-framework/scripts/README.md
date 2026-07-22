# Framework scripts

Helper scripts for local (VS Code / terminal) workflows. They are **operator
tools**, not part of the deployed wheel.

| Script | What it does |
|--------|--------------|
| [lint_framework_config.py](lint_framework_config.py) | Static lint of the declarative YAML in `config/` (no cluster needed). |
| [workspace_proof_preflight.py](workspace_proof_preflight.py) | Local readiness check before the live workspace proof (see [docs/07](../docs/07-workspace-proof-runbook.md)). |
| [run_control_sql.py](run_control_sql.py) | Run the control-plane DDL (`sql/control/00–02`) **without a SQL Warehouse**. |
| [show_tables.py](show_tables.py) | `SHOW SCHEMAS / TABLES / VOLUMES` from your terminal **without a SQL Warehouse**. |
| [_db_session.py](_db_session.py) | Shared helper that opens a Databricks Connect (serverless) Spark session. |

## Running SQL / inspecting the catalog without a SQL Warehouse

You do **not** need a SQL Warehouse to run the control DDL or to look at what
exists in Unity Catalog. `run_control_sql.py` and `show_tables.py` use
[**Databricks Connect**](https://learn.microsoft.com/azure/databricks/dev-tools/databricks-connect/python/):
a thin client that sends your `spark.sql(...)` calls to Databricks
[**serverless compute**](https://learn.microsoft.com/azure/databricks/compute/serverless/)
and streams results back to VS Code. No local Java/Spark is required.

### One-time setup

```bash
# 1. Install the client (match your workspace's Databricks Runtime major version)
pip install "databricks-connect>=15.4"

# 2. Authenticate the Databricks CLI to your workspace (OAuth U2M recommended)
databricks auth login --host https://<your-workspace>.azuredatabricks.net
```

(Or set `DATABRICKS_HOST` + `DATABRICKS_TOKEN`, or select a profile with
`DATABRICKS_CONFIG_PROFILE=<name>`.)

### Everyday commands

Run all commands from the framework root
(`framework/databricks-medallion-framework`):

```bash
# Create/refresh the DEV control catalog + tables (no warehouse)
python scripts/run_control_sql.py --env dev

# Preview the rendered statements without executing
python scripts/run_control_sql.py --env dev --dry-run

# Same DDL against QAT ({env} + storage account come from config/environments.yaml)
python scripts/run_control_sql.py --env qat

# See what exists (replaces manual "SHOW TABLES" in the UI)
python scripts/show_tables.py --env dev
python scripts/show_tables.py --catalog edw_hr_dev --schema bronze
```

## Do I ever need a SQL Warehouse?

For this framework's operational tasks — **no**. Serverless compute via
Databricks Connect covers running the DDL and inspecting the catalog.

A [SQL Warehouse](https://learn.microsoft.com/azure/databricks/compute/sql-warehouse/)
is worth deploying only if your team also wants an always-on SQL endpoint for
BI tools (Power BI, dashboards) or ad-hoc analyst SQL in the Databricks SQL
editor. That is a separate, optional decision from running this framework.

## Other ways to run the SQL

The files under `sql/control/` are **templates** with two tokens — `{env}` and
`{storage_account}` — that the runner fills in from `--env` and
[`config/environments.yaml`](../config/environments.yaml). To run them elsewhere:

1. Print the rendered (paste-ready) SQL with
   `python scripts/run_control_sql.py --env dev --dry-run`, then paste it into a
   Databricks **notebook** `%sql` cell or the **SQL editor**.
2. The storage account is set per environment in `config/environments.yaml` —
   update it in one place (or override with `--storage-account`). The `qat` and
   `prod` account names there are placeholders; confirm the real ones first.
