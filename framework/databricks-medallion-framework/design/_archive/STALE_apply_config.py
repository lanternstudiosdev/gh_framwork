"""
Config Apply step for the Databricks Medallion Ingestion Framework.

This script (or notebook) is executed as part of a Databricks Asset Bundle deployment
(GitHub Action → `databricks bundle run` or a task inside the bundle).

Responsibilities:
- Read declarative configuration from the checked-out `config/` directory.
- Connect to the TARGET environment's platform_control catalog (dev/qat/prod).
- Upsert declarative config tables (contracts, quality_rules, column_policies, entities, etc.).
- Record the deployment in config_deployments for full auditability.
- Update provenance columns (last_applied_git_commit_sha, etc.) on every affected row.
- NEVER touch runtime state tables (watermark_state, active reprocess execution fields).

Inputs (passed via DABs job parameters or environment variables):
- TARGET_CONTROL_CATALOG (e.g. "prod_platform_control")
- GIT_COMMIT_SHA
- GIT_BRANCH
- TRIGGERED_BY (GitHub actor or service principal)
- CONFIG_ROOT (path to the config/ folder, default "config")

The script should be idempotent and safe to re-run for the same commit.
"""

import os
from datetime import datetime
from typing import Any, Dict, List
# from databricks.sdk import WorkspaceClient   # Recommended for production
# import yaml

# =============================================================================
# CONFIGURATION (in real code these come from DABs parameters / env)
# =============================================================================
TARGET_CONTROL_CATALOG = os.getenv("TARGET_CONTROL_CONTROL_CATALOG", "dev_platform_control")
GIT_COMMIT_SHA = os.getenv("GIT_COMMIT_SHA", "local-dev")
GIT_BRANCH = os.getenv("GIT_BRANCH", "main")
TRIGGERED_BY = os.getenv("TRIGGERED_BY", "local")
CONFIG_ROOT = os.getenv("CONFIG_ROOT", "config")

CONTROL_SCHEMA = "control"

# =============================================================================
# Helper functions (implement with your preferred client: spark.sql, databricks-sql-connector, SDK)
# =============================================================================
def get_spark_or_sql_client():
    """Return a client that can execute SQL against the target catalog."""
    # In a DABs notebook task you usually have `spark` already.
    # For a Python wheel task or outside, use Databricks SQL connector or SDK.
    # Example:
    # from databricks import sql
    # connection = sql.connect(...)
    pass

def execute_sql(sql: str, params: Dict = None):
    """Execute SQL with optional parameters."""
    print(f"[SQL] {sql}  params={params}")
    # client.execute(sql, params)
    pass

def upsert_table(table_name: str, rows: List[Dict[str, Any]], key_columns: List[str]):
    """
    Idempotent upsert into a declarative config table.
    Uses MERGE or DELETE+INSERT strategy.
    Also updates provenance columns.
    """
    full_table = f"{TARGET_CONTROL_CATALOG}.{CONTROL_SCHEMA}.{table_name}"
    print(f"Upserting {len(rows)} rows into {full_table}")

    # Example MERGE pattern (adapt to your SQL dialect / client)
    # for row in rows:
    #     row["last_applied_git_commit_sha"] = GIT_COMMIT_SHA
    #     row["last_applied_ts"] = datetime.utcnow()
    #     row["last_applied_deployment_id"] = deployment_id
    #
    # Then run a MERGE statement using the key_columns.
    pass

def load_yaml_files(base_path: str) -> List[Dict]:
    """Recursively load all .yaml/.yml files under a directory."""
    # Use glob + yaml.safe_load
    print(f"Loading YAML files from {base_path}")
    return []  # placeholder

# =============================================================================
# Main apply logic
# =============================================================================
def main():
    print(f"Starting Config Apply for catalog={TARGET_CONTROL_CATALOG} commit={GIT_COMMIT_SHA}")

    deployment_id = f"deploy-{GIT_COMMIT_SHA[:8]}-{int(datetime.utcnow().timestamp())}"

    # 1. Record start of deployment (append-only audit)
    execute_sql(f"""
        INSERT INTO {TARGET_CONTROL_CATALOG}.{CONTROL_SCHEMA}.config_deployments
        (deployment_id, git_commit_sha, git_branch, triggered_by, target_control_catalog,
         status, started_ts)
        VALUES ('{deployment_id}', '{GIT_COMMIT_SHA}', '{GIT_BRANCH}', '{TRIGGERED_BY}',
                '{TARGET_CONTROL_CATALOG}', 'running', current_timestamp())
    """)

    tables_applied = []

    try:
        # -----------------------------------------------------------------
        # 2. Load and apply each declarative area
        # -----------------------------------------------------------------

        # Contracts
        contracts = load_yaml_files(f"{CONFIG_ROOT}/contracts")
        # Transform list of dicts into rows suitable for data_contracts table
        contract_rows = []  # ... build rows with git_path, version, contract_json, etc.
        upsert_table("data_contracts", contract_rows, ["entity_key", "version"])
        tables_applied.append("data_contracts")

        # Quality Rules (global + per-subject)
        quality_rules = load_yaml_files(f"{CONFIG_ROOT}/quality_rules")
        # Merge global defaults with entity-specific overrides.
        # Each rule gets git_path populated from its source file.
        quality_rule_rows = []
        upsert_table("quality_rules", quality_rule_rows, ["rule_id"])
        tables_applied.append("quality_rules")

        # Column Policies (sparse)
        column_policies = load_yaml_files(f"{CONFIG_ROOT}/column_policies")
        policy_rows = []
        upsert_table("column_policies", policy_rows, ["entity_key", "column_name"])
        tables_applied.append("column_policies")

        # Entities + load configs (source_entities + entity_load_configs)
        entities = load_yaml_files(f"{CONFIG_ROOT}/entities")
        # ... upsert source_entities, entity_load_configs, sources, etc.
        tables_applied.extend(["source_entities", "entity_load_configs"])

        # Reprocess requests (the "as code" part)
        # Note: Only the initial request + provenance. Status changes are runtime.
        reprocess_files = load_yaml_files(f"{CONFIG_ROOT}/reprocess_requests")
        reprocess_rows = []
        upsert_table("reprocess_requests", reprocess_rows, ["request_id"])
        tables_applied.append("reprocess_requests")

        # Pipeline assets (if you keep some pipeline metadata in YAML)
        # tables_applied.append("pipeline_assets")

        # -----------------------------------------------------------------
        # 3. Record successful deployment
        # -----------------------------------------------------------------
        execute_sql(f"""
            UPDATE {TARGET_CONTROL_CATALOG}.{CONTROL_SCHEMA}.config_deployments
            SET status = 'success',
                completed_ts = current_timestamp(),
                tables_applied = array{tables_applied},
                rows_updated = 123   -- compute real counts
            WHERE deployment_id = '{deployment_id}'
        """)

        print("Config Apply completed successfully.")

    except Exception as e:
        execute_sql(f"""
            UPDATE {TARGET_CONTROL_CATALOG}.{CONTROL_SCHEMA}.config_deployments
            SET status = 'failed',
                completed_ts = current_timestamp(),
                error_details = '{str(e)[:4000]}'
            WHERE deployment_id = '{deployment_id}'
        """)
        raise

if __name__ == "__main__":
    main()
