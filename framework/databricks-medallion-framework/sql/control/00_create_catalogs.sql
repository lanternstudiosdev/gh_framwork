-- =============================================================================
-- Catalog creation — control plane + subject data catalogs
-- =============================================================================
-- OWNERSHIP: Unity Catalog CATALOGS (and CONNECTIONS) are typically owned by a
-- metastore admin and may be created OUTSIDE the DAB (Declarative Automation
-- Bundle) deployment. This script is intentionally separate from schema/table
-- creation so an admin can run just the catalog DDL, while engineers / the DAB
-- own schemas + volumes (01_create_schemas.sql) and control tables
-- (02_control_tables.sql).
--
-- TEMPLATED — this file is rendered by scripts/run_control_sql.py, which
-- substitutes two tokens (do NOT hardcode env or storage per file):
--   {env}              -> dev | qat | prod   (from --env)
--   {storage_account}  -> config/environments.yaml -> environments.<env>.storage_account
-- Update the storage account for an environment in ONE place: config/environments.yaml.
--
-- Catalog naming:
--   Control : edw_platform_control_{env}
--   Data    : edw_{subject}_{env}          (e.g. edw_hr_{env}, edw_sales_{env}, edw_refdata_{env})
--
-- Prereqs for data catalogs with a MANAGED LOCATION:
--   Storage credential + external location must already exist for the
--   container root abfss://edw-{subject}-{env}@{storage_account}.dfs.core.windows.net/
--
-- Run once per environment. No SQL Warehouse required — pick ONE:
--   1. From VS Code:  python scripts/run_control_sql.py --env dev
--   2. Notebook:      run with --dry-run to print rendered SQL, then paste into a %sql cell
--   3. SQL Warehouse / SQL editor (paste the rendered SQL)
-- Docs: Unity Catalog  https://learn.microsoft.com/azure/databricks/catalogs/
--       External locations
--       https://learn.microsoft.com/azure/databricks/connect/unity-catalog/external-locations
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Control catalog (platform GitOps metadata + runtime state)
-- ---------------------------------------------------------------------------
CREATE CATALOG IF NOT EXISTS edw_platform_control_{env}
COMMENT 'Platform control plane (GitOps metadata + runtime state) for medallion framework';

-- Optional grants (uncomment + adjust group/SP names for your workspace):
-- GRANT USE CATALOG ON CATALOG edw_platform_control_{env} TO `account users`;
-- GRANT USE SCHEMA ON SCHEMA edw_platform_control_{env}.control TO `account users`;
-- GRANT SELECT ON SCHEMA edw_platform_control_{env}.control TO `data-engineers`;
-- GRANT MODIFY ON SCHEMA edw_platform_control_{env}.control TO `platform-apply-sp`;

-- ---------------------------------------------------------------------------
-- Subject data catalogs — one CREATE CATALOG per subject area.
--
-- TO ADD A SUBJECT AREA (Sales, RefData, …):
--   1. Copy the HR block below, replace "hr" with the new subject key. The
--      {env} / {storage_account} tokens stay as-is (the runner fills them in).
--   2. Add the matching schemas + volumes block in 01_create_schemas.sql.
-- ---------------------------------------------------------------------------

-- === HR (active) ===========================================================
CREATE CATALOG IF NOT EXISTS edw_hr_{env}
MANAGED LOCATION 'abfss://edw-hr-{env}@{storage_account}.dfs.core.windows.net/catalog/'
COMMENT 'HR subject data catalog';

-- === Sales (template — uncomment when onboarding the Sales subject area) ====
-- CREATE CATALOG IF NOT EXISTS edw_sales_{env}
-- MANAGED LOCATION 'abfss://edw-sales-{env}@{storage_account}.dfs.core.windows.net/catalog/'
-- COMMENT 'Sales subject data catalog';

-- === RefData (template — uncomment when onboarding the RefData subject area) =
-- CREATE CATALOG IF NOT EXISTS edw_refdata_{env}
-- MANAGED LOCATION 'abfss://edw-refdata-{env}@{storage_account}.dfs.core.windows.net/catalog/'
-- COMMENT 'Reference data subject catalog';

-- ---------------------------------------------------------------------------
-- Unity Catalog CONNECTIONS (Lakeflow Connect / federation) are also
-- admin-owned and created outside DAB. Define them here or via the UC UI, e.g.:
--   CREATE CONNECTION IF NOT EXISTS workday_connect TYPE workday
--   OPTIONS (host '...', ...);
-- See https://learn.microsoft.com/azure/databricks/connect/unity-catalog/connections
-- ---------------------------------------------------------------------------
