-- =============================================================================
-- SAMPLES: HR data catalog + UC Volume landing (dev)
-- =============================================================================
-- Aligns sample pipelines with production path conventions:
--   /Volumes/edw_hr_dev/files/landing/raw/workday/{entity_name}/
--
-- Update LOCATION URLs for your storage account before running.
-- Seed CSVs under samples/data/landing/ can be copied into the volume, e.g.:
--   dbutils.fs.cp("file:/.../samples/data/landing/hr/employees/",
--                 "/Volumes/edw_hr_dev/files/landing/raw/workday/employees/", True)
-- =============================================================================

CREATE CATALOG IF NOT EXISTS edw_hr_dev
MANAGED LOCATION 'abfss://edw-hr-dev@azrcedwdevsto001.dfs.core.windows.net/catalog/'
COMMENT 'HR subject data catalog (dev / samples)';

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

CREATE SCHEMA IF NOT EXISTS edw_hr_dev.files
COMMENT 'External volumes for landing and published files';

CREATE EXTERNAL VOLUME IF NOT EXISTS edw_hr_dev.files.landing
LOCATION 'abfss://edw-hr-dev@azrcedwdevsto001.dfs.core.windows.net/landing/'
COMMENT 'Sample + prod-shaped landing: raw/ and archive/';

CREATE EXTERNAL VOLUME IF NOT EXISTS edw_hr_dev.files.published
LOCATION 'abfss://edw-hr-dev@azrcedwdevsto001.dfs.core.windows.net/published/'
COMMENT 'Optional published volume (unused by samples v1)';
