-- =============================================================================
-- HR subject data catalog skeleton (DEV example)
-- =============================================================================
-- Matches design/uc-volume-landing.md and your UC layout:
--   edw_hr_dev
--     bronze | silver | gold | *_restricted
--     files.landing (external volume) → raw/ + archive/
--     files.published (optional; out of scope for v1 pipelines)
--
-- Storage credential + external location must already exist, e.g.:
--   Storage Credential : edw_access_connector_dev_cred
--   External Location  : azrcedwdevsto001-edw-hr-dev
--   Container root     : abfss://edw-hr-dev@azrcedwdevsto001.dfs.core.windows.net/
--
-- Adjust LOCATION URLs for your storage account/container before running.
-- =============================================================================

CREATE CATALOG IF NOT EXISTS edw_hr_dev
MANAGED LOCATION 'abfss://edw-hr-dev@azrcedwdevsto001.dfs.core.windows.net/catalog/'
COMMENT 'HR subject data catalog (dev)';

CREATE SCHEMA IF NOT EXISTS edw_hr_dev.bronze
MANAGED LOCATION 'abfss://edw-hr-dev@azrcedwdevsto001.dfs.core.windows.net/catalog/bronze/';

CREATE SCHEMA IF NOT EXISTS edw_hr_dev.silver
MANAGED LOCATION 'abfss://edw-hr-dev@azrcedwdevsto001.dfs.core.windows.net/catalog/silver/';

CREATE SCHEMA IF NOT EXISTS edw_hr_dev.gold
MANAGED LOCATION 'abfss://edw-hr-dev@azrcedwdevsto001.dfs.core.windows.net/catalog/gold/';

CREATE SCHEMA IF NOT EXISTS edw_hr_dev.bronze_restricted
MANAGED LOCATION 'abfss://edw-hr-dev@azrcedwdevsto001.dfs.core.windows.net/catalog/bronze_restricted/';

CREATE SCHEMA IF NOT EXISTS edw_hr_dev.silver_restricted
MANAGED LOCATION 'abfss://edw-hr-dev@azrcedwdevsto001.dfs.core.windows.net/catalog/silver_restricted/';

CREATE SCHEMA IF NOT EXISTS edw_hr_dev.gold_restricted
MANAGED LOCATION 'abfss://edw-hr-dev@azrcedwdevsto001.dfs.core.windows.net/catalog/gold_restricted/';

-- files schema has no managed location; volumes point at container folders
CREATE SCHEMA IF NOT EXISTS edw_hr_dev.files
COMMENT 'File assets: external volumes for landing and published';

-- External volumes (paths are relative to the external location / container root)
-- If volumes already exist, these statements can be skipped.
CREATE EXTERNAL VOLUME IF NOT EXISTS edw_hr_dev.files.landing
LOCATION 'abfss://edw-hr-dev@azrcedwdevsto001.dfs.core.windows.net/landing/'
COMMENT 'HR landing volume: raw/{source_key}/{entity_name}/ and archive/...';

CREATE EXTERNAL VOLUME IF NOT EXISTS edw_hr_dev.files.published
LOCATION 'abfss://edw-hr-dev@azrcedwdevsto001.dfs.core.windows.net/published/'
COMMENT 'Outbound published files (framework v1 ignores this volume)';

-- Create expected raw/archive folders (optional; extract job also mkdirs)
-- In a notebook you can also:
--   dbutils.fs.mkdirs("/Volumes/edw_hr_dev/files/landing/raw/workday")
--   dbutils.fs.mkdirs("/Volumes/edw_hr_dev/files/landing/archive/workday")
