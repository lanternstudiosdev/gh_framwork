-- =============================================================================
-- Platform control catalog + schema bootstrap
-- =============================================================================
-- Catalog naming: edw_platform_control_{env}  (dev | qat | prod)
--
-- Run once per environment (Databricks SQL warehouse or notebook %sql).
-- Replace the catalog name below, or parameterize via your deploy tooling.
--
-- Subject data catalogs (e.g. edw_hr_dev) are provisioned separately with
-- bronze/silver/gold/files + external volumes. See design/uc-volume-landing.md.
-- =============================================================================

-- Example for DEV — change for qat/prod:
CREATE CATALOG IF NOT EXISTS edw_platform_control_dev
COMMENT 'Platform control plane (GitOps metadata + runtime state) for medallion framework';

CREATE SCHEMA IF NOT EXISTS edw_platform_control_dev.control
COMMENT 'Declarative config, watermarks, reprocess, and deployment audit tables';

-- Optional: grant platform apply identity + pipeline read roles here.
-- GRANT USE CATALOG ON CATALOG edw_platform_control_dev TO `account users`;
-- GRANT USE SCHEMA ON SCHEMA edw_platform_control_dev.control TO `account users`;
-- GRANT SELECT ON SCHEMA edw_platform_control_dev.control TO `data-engineers`;
-- GRANT MODIFY ON SCHEMA edw_platform_control_dev.control TO `platform-apply-sp`;
