-- =============================================================================
-- SAMPLES: Catalog creation — control + HR data catalogs
-- =============================================================================
-- Catalogs (and UC connections) are typically admin-owned and may be created
-- OUTSIDE the DAB deployment. Split from schema creation
-- (01_create_schemas.sql) so an admin can run just the catalog DDL.
--
-- TEMPLATED — rendered by scripts/run_control_sql.py, which substitutes:
--   {env}              -> dev | qat | prod   (from --env)
--   {storage_account}  -> config/environments.yaml -> environments.<env>.storage_account
-- Update the storage account for an environment in ONE place: config/environments.yaml.
--
-- Samples share the same control model as production framework SQL.
-- Default: edw_platform_control_dev (same catalog production config apply uses in dev).
-- For an isolated sandbox you may use edw_platform_control_sample instead —
-- then point sample jobs' control_catalog parameter at that catalog.
--
-- How to run (no SQL Warehouse required): python scripts/run_control_sql.py --env dev
-- (or run with --dry-run to print rendered SQL for a notebook %sql cell).
-- See scripts/README.md.
-- =============================================================================

-- Control catalog (platform metadata)
CREATE CATALOG IF NOT EXISTS edw_platform_control_{env}
COMMENT 'Platform control plane for medallion framework (samples + dev)';

-- HR subject data catalog
-- To add another subject area, copy this block and replace "hr" with the subject key.
CREATE CATALOG IF NOT EXISTS edw_hr_{env}
MANAGED LOCATION 'abfss://edw-hr-{env}@{storage_account}.dfs.core.windows.net/catalog/'
COMMENT 'HR subject data catalog (dev / samples)';
