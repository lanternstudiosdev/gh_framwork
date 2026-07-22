-- =============================================================================
-- SAMPLES: Control tables (identical shape to framework sql/control/01_*)
-- =============================================================================
-- Keep in sync with: framework/databricks-medallion-framework/sql/control/02_control_tables.sql
-- Samples use the same tables so config under samples/config can be applied
-- with the production apply_control_config job (config_root=samples/config) or
-- a notebook that loads sample YAML.
-- =============================================================================

USE CATALOG edw_platform_control_{env};
USE SCHEMA control;

CREATE TABLE IF NOT EXISTS config_deployments (
    deployment_id              STRING NOT NULL,
    git_commit_sha             STRING,
    git_branch                 STRING,
    triggered_by               STRING,
    target_control_catalog     STRING,
    dab_target                 STRING,              -- DAB target: dev_personal | dev_shared | qat | prod
    status                     STRING,
    started_ts                 TIMESTAMP,
    completed_ts               TIMESTAMP,
    tables_applied             ARRAY<STRING>,
    error_details              STRING,
    CONSTRAINT pk_config_deployments PRIMARY KEY (deployment_id)
) USING DELTA;

CREATE TABLE IF NOT EXISTS subject_areas (
    subject_area_key           STRING NOT NULL,
    description                STRING,
    catalogs_json              STRING,
    schemas_json               STRING,
    landing_volume_json        STRING,
    git_path                   STRING,
    is_active                  BOOLEAN,
    last_applied_git_commit_sha STRING,
    last_applied_ts            TIMESTAMP,
    last_applied_deployment_id STRING,
    CONSTRAINT pk_subject_areas PRIMARY KEY (subject_area_key)
) USING DELTA;

CREATE TABLE IF NOT EXISTS sources (
    source_key                 STRING NOT NULL,
    environment                STRING NOT NULL,
    source_type                STRING,
    subject_area_key           STRING,
    description                STRING,
    default_load_pattern       STRING,
    connection_json            STRING,
    connect_json               STRING,
    extract_defaults_json      STRING,
    secret_scope               STRING,
    git_path                   STRING,
    is_active                  BOOLEAN,
    last_applied_git_commit_sha STRING,
    last_applied_ts            TIMESTAMP,
    last_applied_deployment_id STRING,
    CONSTRAINT pk_sources PRIMARY KEY (source_key, environment)
) USING DELTA;

CREATE TABLE IF NOT EXISTS source_entities (
    entity_key                 STRING NOT NULL,
    subject_area_key           STRING,
    source_key                 STRING,
    entity_name                STRING,
    source_object              STRING,
    load_pattern               STRING,
    primary_key_columns        STRING,
    watermark_column           STRING,
    target_bronze_table        STRING,
    target_silver_table        STRING,
    data_catalog               STRING,
    restricted                 BOOLEAN,
    supports_full_reprocess    BOOLEAN,
    reprocess_strategy         STRING,
    is_active                  BOOLEAN,
    git_path                   STRING,
    last_applied_git_commit_sha STRING,
    last_applied_ts            TIMESTAMP,
    last_applied_deployment_id STRING,
    CONSTRAINT pk_source_entities PRIMARY KEY (entity_key)
) USING DELTA;

CREATE TABLE IF NOT EXISTS entity_load_configs (
    entity_key                 STRING NOT NULL,
    environment                STRING NOT NULL,
    load_pattern               STRING,
    source_object              STRING,
    custom_extract_params      STRING,
    api_config                 STRING,
    landing_volume             STRING,
    landing_volume_path        STRING,
    archive_volume_path        STRING,
    landing_subpath            STRING,
    archive_subpath            STRING,
    auto_loader_options        STRING,
    lakeflow_connect_config    STRING,
    bronze_schema_evolution_mode STRING,
    git_path                   STRING,
    last_applied_git_commit_sha STRING,
    last_applied_ts            TIMESTAMP,
    last_applied_deployment_id STRING,
    CONSTRAINT pk_entity_load_configs PRIMARY KEY (entity_key, environment)
) USING DELTA;

CREATE TABLE IF NOT EXISTS data_contracts (
    contract_id                STRING NOT NULL,
    entity_key                 STRING NOT NULL,
    version                    INT NOT NULL,
    git_path                   STRING,
    contract_json              STRING,
    effective_from             STRING,
    last_applied_git_commit_sha STRING,
    last_applied_ts            TIMESTAMP,
    last_applied_deployment_id STRING,
    CONSTRAINT pk_data_contracts PRIMARY KEY (entity_key, version)
) USING DELTA;

CREATE TABLE IF NOT EXISTS quality_rules (
    rule_id                    STRING NOT NULL,
    entity_key                 STRING,
    layer                      STRING,
    rule_name                  STRING,
    rule_type                  STRING,
    enforcement_method         STRING,
    expression                 STRING,
    library_reference          STRING,
    action_on_failure          STRING,
    severity                   STRING,
    git_path                   STRING,
    is_default                 BOOLEAN,
    is_active                  BOOLEAN,
    last_applied_git_commit_sha STRING,
    last_applied_ts            TIMESTAMP,
    last_applied_deployment_id STRING,
    CONSTRAINT pk_quality_rules PRIMARY KEY (rule_id)
) USING DELTA;

CREATE TABLE IF NOT EXISTS column_policies (
    policy_id                  STRING NOT NULL,
    entity_key                 STRING NOT NULL,
    column_name                STRING NOT NULL,
    policy_type                STRING,
    encryption_key_vault_ref   STRING,
    apply_starting_layer       STRING,
    classification             STRING,
    git_path                   STRING,
    is_active                  BOOLEAN,
    last_applied_git_commit_sha STRING,
    last_applied_ts            TIMESTAMP,
    last_applied_deployment_id STRING,
    CONSTRAINT pk_column_policies PRIMARY KEY (entity_key, column_name)
) USING DELTA;

CREATE TABLE IF NOT EXISTS pipeline_assets (
    asset_id                   STRING NOT NULL,
    entity_key                 STRING,
    subject_area_key           STRING,
    asset_type                 STRING NOT NULL,
    asset_name                 STRING NOT NULL,
    resource_name_in_bundle    STRING,
    bundle_path                STRING,
    git_path                   STRING,
    target_layer               STRING,
    depends_on                 ARRAY<STRING>,
    supports_reprocess         BOOLEAN,
    default_schedule           STRING,
    compute_type               STRING,
    parameters                 STRING,
    is_active                  BOOLEAN,
    last_applied_git_commit_sha STRING,
    last_applied_ts            TIMESTAMP,
    last_applied_deployment_id STRING,
    CONSTRAINT pk_pipeline_assets PRIMARY KEY (asset_id)
) USING DELTA;

CREATE TABLE IF NOT EXISTS reprocess_requests (
    request_id                 STRING NOT NULL,
    subject_area_key           STRING,
    requested_entities         ARRAY<STRING>,
    reprocess_mode             STRING,
    from_watermark             STRING,
    to_watermark               STRING,
    reason                     STRING,
    requested_by               STRING,
    source_file_path           STRING,
    git_commit_sha             STRING,
    github_pr_url              STRING,
    status                     STRING,
    execution_run_id           STRING,
    executed_at                TIMESTAMP,
    result_summary             STRING,
    created_ts                 TIMESTAMP,
    updated_ts                 TIMESTAMP,
    last_applied_git_commit_sha STRING,
    last_applied_ts            TIMESTAMP,
    last_applied_deployment_id STRING,
    CONSTRAINT pk_reprocess_requests PRIMARY KEY (request_id)
) USING DELTA;

CREATE TABLE IF NOT EXISTS watermark_state (
    entity_key                 STRING NOT NULL,
    current_watermark          STRING,
    last_successful_run_id     STRING,
    last_successful_ts         TIMESTAMP,
    last_row_count             BIGINT,
    is_reprocessing            BOOLEAN,
    reprocess_request_id       STRING,
    updated_ts                 TIMESTAMP,
    CONSTRAINT pk_watermark_state PRIMARY KEY (entity_key)
) USING DELTA;

CREATE OR REPLACE VIEW v_active_hr_entities AS
SELECT e.*
FROM source_entities e
WHERE e.subject_area_key = 'hr' AND e.is_active = true;

CREATE OR REPLACE VIEW v_entity_landing_paths AS
SELECT
    e.entity_key,
    e.subject_area_key,
    e.source_key,
    e.entity_name,
    e.load_pattern,
    e.restricted,
    e.data_catalog,
    l.environment,
    l.landing_volume_path,
    l.archive_volume_path,
    l.landing_subpath,
    l.api_config
FROM source_entities e
LEFT JOIN entity_load_configs l ON e.entity_key = l.entity_key;
