-- =============================================================================
-- SAMPLES: Schema + volume creation — control + HR data catalog
-- =============================================================================
-- Run AFTER 00_create_catalogs.sql. Aligns sample pipelines with production
-- path conventions: /Volumes/edw_hr_{env}/files/landing/raw/workday/{entity_name}/
--
-- TEMPLATED — rendered by scripts/run_control_sql.py, which substitutes:
--   {env}              -> dev | qat | prod   (from --env)
--   {storage_account}  -> config/environments.yaml -> environments.<env>.storage_account
-- Update the storage account for an environment in ONE place: config/environments.yaml.
--
-- How to run (no SQL Warehouse required): python scripts/run_control_sql.py --env dev
-- (or run with --dry-run to print rendered SQL for a notebook %sql cell).
-- See scripts/README.md.
-- Seed CSVs under samples/data/landing/ can be copied into the volume, e.g.:
--   dbutils.fs.cp("file:/.../samples/data/landing/hr/employees/",
--                 "/Volumes/edw_hr_{env}/files/landing/raw/workday/employees/", True)
-- =============================================================================

-- Control schema (tables created by 02_control_tables.sql)
CREATE SCHEMA IF NOT EXISTS edw_platform_control_{env}.control
COMMENT 'Control tables for declarative config and runtime state';

-- === HR subject schemas + volumes ==========================================
-- To add a subject area, copy this block and replace "hr" with the subject key.
CREATE SCHEMA IF NOT EXISTS edw_hr_{env}.bronze
MANAGED LOCATION 'abfss://edw-hr-{env}@{storage_account}.dfs.core.windows.net/catalog/bronze/';

CREATE SCHEMA IF NOT EXISTS edw_hr_{env}.silver
MANAGED LOCATION 'abfss://edw-hr-{env}@{storage_account}.dfs.core.windows.net/catalog/silver/';

CREATE SCHEMA IF NOT EXISTS edw_hr_{env}.gold
MANAGED LOCATION 'abfss://edw-hr-{env}@{storage_account}.dfs.core.windows.net/catalog/gold/';

CREATE SCHEMA IF NOT EXISTS edw_hr_{env}.bronze_restricted
MANAGED LOCATION 'abfss://edw-hr-{env}@{storage_account}.dfs.core.windows.net/catalog/bronze_restricted/';

CREATE SCHEMA IF NOT EXISTS edw_hr_{env}.silver_restricted
MANAGED LOCATION 'abfss://edw-hr-{env}@{storage_account}.dfs.core.windows.net/catalog/silver_restricted/';

CREATE SCHEMA IF NOT EXISTS edw_hr_{env}.gold_restricted
MANAGED LOCATION 'abfss://edw-hr-{env}@{storage_account}.dfs.core.windows.net/catalog/gold_restricted/';

CREATE SCHEMA IF NOT EXISTS edw_hr_{env}.files
COMMENT 'External volumes for landing and published files';

CREATE EXTERNAL VOLUME IF NOT EXISTS edw_hr_{env}.files.landing
LOCATION 'abfss://edw-hr-{env}@{storage_account}.dfs.core.windows.net/landing/'
COMMENT 'Sample + prod-shaped landing: raw/ and archive/';

CREATE EXTERNAL VOLUME IF NOT EXISTS edw_hr_{env}.files.published
LOCATION 'abfss://edw-hr-{env}@{storage_account}.dfs.core.windows.net/published/'
COMMENT 'Optional published volume (unused by samples v1)';
