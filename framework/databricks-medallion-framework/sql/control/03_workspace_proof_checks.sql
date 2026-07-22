-- =============================================================================
-- Workspace proof verification queries
-- Run after DDL + apply + orchestration.
--
-- TEMPLATED — the {env} token is rendered by scripts/run_control_sql.py
-- (--env dev|qat|prod). Run with --dry-run to print paste-ready SQL.
-- No SQL Warehouse needed to browse objects: python scripts/show_tables.py --env dev
-- =============================================================================

-- A) Control plane health
SELECT 'deployments' AS check_name, *
FROM edw_platform_control_{env}.control.config_deployments
ORDER BY started_ts DESC
LIMIT 5;

SELECT 'hr_entities' AS check_name, COUNT(*) AS cnt
FROM edw_platform_control_{env}.control.source_entities
WHERE subject_area_key = 'hr' AND is_active = true;

SELECT entity_key, load_pattern, target_bronze_table, restricted
FROM edw_platform_control_{env}.control.source_entities
WHERE subject_area_key = 'hr'
ORDER BY entity_key;

SELECT asset_name, asset_type, supports_reprocess, is_active, default_schedule
FROM edw_platform_control_{env}.control.pipeline_assets
WHERE subject_area_key = 'hr'
ORDER BY asset_type, asset_name;

-- B) Connect staging present (smoke examples — expect tables to exist)
-- SELECT COUNT(*) FROM edw_hr_{env}.bronze.workday_location__src;
-- SELECT COUNT(*) FROM edw_hr_{env}.bronze.workday_current_employee_list__src;
-- SELECT COUNT(*) FROM edw_hr_{env}.bronze_restricted.workday_payroll_employee_list__src;

-- C) Framework bronze / silver
-- SELECT COUNT(*) FROM edw_hr_{env}.bronze.workday_location;
-- SELECT COUNT(*) FROM edw_hr_{env}.silver.workday_location;
-- SELECT COUNT(*) FROM edw_hr_{env}.bronze_restricted.workday_payroll_employee_list;
-- SELECT COUNT(*) FROM edw_hr_{env}.silver_restricted.workday_payroll_employee_list;

-- D) Reprocess
SELECT request_id, status, execution_run_id, result_summary, updated_ts
FROM edw_platform_control_{env}.control.reprocess_requests
ORDER BY COALESCE(updated_ts, created_ts) DESC
LIMIT 20;

SELECT entity_key, current_watermark, is_reprocessing, reprocess_request_id
FROM edw_platform_control_{env}.control.watermark_state
ORDER BY updated_ts DESC NULLS LAST
LIMIT 20;
