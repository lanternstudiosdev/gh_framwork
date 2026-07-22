-- =============================================================================
-- Schema + volume creation — control plane + subject data catalogs
-- =============================================================================
-- Run AFTER 00_create_catalogs.sql. Catalogs (and UC connections) are
-- admin-owned (see 00); schemas, external volumes, and control tables
-- (02_control_tables.sql) are owned by the framework / DAB deployment.
--
-- TEMPLATED — rendered by scripts/run_control_sql.py, which substitutes:
--   {env}              -> dev | qat | prod   (from --env)
--   {storage_account}  -> config/environments.yaml -> environments.<env>.storage_account
-- Update the storage account for an environment in ONE place: config/environments.yaml.
--
-- Run once per environment. No SQL Warehouse required — pick ONE:
--   1. From VS Code:  python scripts/run_control_sql.py --env dev
--   2. Notebook:      run with --dry-run to print rendered SQL, then paste into a %sql cell
--   3. SQL Warehouse / SQL editor (paste the rendered SQL)
-- Docs: schemas  https://learn.microsoft.com/azure/databricks/schemas/
--       volumes  https://learn.microsoft.com/azure/databricks/volumes/
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Control schema (platform metadata tables live here — see 02_control_tables.sql)
-- ---------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS edw_platform_control_{env}.control
COMMENT 'Declarative config, watermarks, reprocess, and deployment audit tables';

-- ---------------------------------------------------------------------------
-- Subject data schemas + volumes — one block per subject area.
--
-- Each subject catalog gets medallion schemas (bronze | silver | gold and
-- *_restricted variants) plus a `files` schema holding landing/published
-- external volumes.
--
-- TO ADD A SUBJECT AREA (Sales, RefData, …):
--   1. Copy the HR block below, replace "hr" with the new subject key. The
--      {env} / {storage_account} tokens stay as-is (the runner fills them in).
--   2. The matching CREATE CATALOG lives in 00_create_catalogs.sql.
-- ---------------------------------------------------------------------------

-- === HR (active) ===========================================================
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

-- files schema has no managed location; volumes point at container folders
CREATE SCHEMA IF NOT EXISTS edw_hr_{env}.files
COMMENT 'File assets: external volumes for landing and published';

-- External volumes (paths are relative to the external location / container root)
-- If volumes already exist, these statements can be skipped.
CREATE EXTERNAL VOLUME IF NOT EXISTS edw_hr_{env}.files.landing
LOCATION 'abfss://edw-hr-{env}@{storage_account}.dfs.core.windows.net/landing/'
COMMENT 'HR landing volume: raw/{source_key}/{entity_name}/ and archive/...';

CREATE EXTERNAL VOLUME IF NOT EXISTS edw_hr_{env}.files.published
LOCATION 'abfss://edw-hr-{env}@{storage_account}.dfs.core.windows.net/published/'
COMMENT 'Outbound published files (framework v1 ignores this volume)';

-- Create expected raw/archive folders (optional; extract job also mkdirs)
-- In a notebook you can also:
--   dbutils.fs.mkdirs("/Volumes/edw_hr_{env}/files/landing/raw/workday")
--   dbutils.fs.mkdirs("/Volumes/edw_hr_{env}/files/landing/archive/workday")

-- === Sales (template — uncomment when onboarding) ==========================
-- CREATE SCHEMA IF NOT EXISTS edw_sales_{env}.bronze
-- MANAGED LOCATION 'abfss://edw-sales-{env}@{storage_account}.dfs.core.windows.net/catalog/bronze/';
-- CREATE SCHEMA IF NOT EXISTS edw_sales_{env}.silver
-- MANAGED LOCATION 'abfss://edw-sales-{env}@{storage_account}.dfs.core.windows.net/catalog/silver/';
-- CREATE SCHEMA IF NOT EXISTS edw_sales_{env}.gold
-- MANAGED LOCATION 'abfss://edw-sales-{env}@{storage_account}.dfs.core.windows.net/catalog/gold/';
-- (repeat *_restricted as needed) + files schema + landing/published volumes:
-- CREATE SCHEMA IF NOT EXISTS edw_sales_{env}.files
-- COMMENT 'File assets: external volumes for landing and published';
-- CREATE EXTERNAL VOLUME IF NOT EXISTS edw_sales_{env}.files.landing
-- LOCATION 'abfss://edw-sales-{env}@{storage_account}.dfs.core.windows.net/landing/'
-- COMMENT 'Sales landing volume';

-- === RefData (template — uncomment when onboarding) ========================
-- CREATE SCHEMA IF NOT EXISTS edw_refdata_{env}.bronze
-- MANAGED LOCATION 'abfss://edw-refdata-{env}@{storage_account}.dfs.core.windows.net/catalog/bronze/';
-- CREATE SCHEMA IF NOT EXISTS edw_refdata_{env}.silver
-- MANAGED LOCATION 'abfss://edw-refdata-{env}@{storage_account}.dfs.core.windows.net/catalog/silver/';
-- CREATE SCHEMA IF NOT EXISTS edw_refdata_{env}.gold
-- MANAGED LOCATION 'abfss://edw-refdata-{env}@{storage_account}.dfs.core.windows.net/catalog/gold/';
-- (add files schema + volumes only if RefData lands files)
