-- =============================================================================
-- SAMPLES: Platform control catalog + schema (dev)
-- =============================================================================
-- Samples share the same control model as production framework SQL.
-- Default: edw_platform_control_dev (same catalog production config apply uses in dev).
--
-- For an isolated sandbox you may use edw_platform_control_sample instead —
-- then point sample jobs' control_catalog parameter at that catalog.
-- =============================================================================

CREATE CATALOG IF NOT EXISTS edw_platform_control_dev
COMMENT 'Platform control plane for medallion framework (samples + dev)';

CREATE SCHEMA IF NOT EXISTS edw_platform_control_dev.control
COMMENT 'Control tables for declarative config and runtime state';
