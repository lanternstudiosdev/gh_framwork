-- =============================================================================
-- Control plane tables — medallion ingestion framework
-- =============================================================================
-- Target: {control_catalog}.control.*
-- TEMPLATED — the {env} token below is rendered by scripts/run_control_sql.py
-- (--env dev|qat|prod). Run with --dry-run to print paste-ready SQL.
--
-- HOW TO RUN THIS FILE (no SQL Warehouse required) — pick ONE:
--   1. From VS Code, no warehouse:
--        python scripts/run_control_sql.py --env dev
--      (uses Databricks Connect on serverless compute; see scripts/README.md)
--   2. In a Databricks notebook: paste into a %sql cell, or use a SQL notebook.
--   3. In the Databricks SQL editor / a SQL Warehouse (if you have one).
--   Databricks Connect: https://learn.microsoft.com/azure/databricks/dev-tools/databricks-connect/python/
--
-- Complex / free-form config is stored as STRING (JSON) so Config Apply can
-- upsert via Spark createDataFrame without MAP type friction. Readers in
-- lib.metadata deserialize JSON fields at runtime.
--
-- Provenance columns (last_applied_*) are written by apply_control_config.
-- =============================================================================

-- USE CATALOG / USE SCHEMA set the "current" catalog + schema so the unqualified
-- table names below resolve. The {env} token is filled in by the runner
-- (--env qat / --env prod); for a notebook, run with --dry-run and paste.
USE CATALOG edw_platform_control_{env};
USE SCHEMA control;

-- ---------------------------------------------------------------------------
-- Audit: config deployments (append-only style; status updated in place)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS config_deployments (
    deployment_id              STRING NOT NULL,
    git_commit_sha             STRING,
    git_branch                 STRING,
    triggered_by               STRING,
    target_control_catalog     STRING,
    dab_target                 STRING,              -- DAB target: dev_personal | dev_shared | qat | prod
    status                     STRING,              -- running | success | failed
    started_ts                 TIMESTAMP,
    completed_ts               TIMESTAMP,
    tables_applied             ARRAY<STRING>,
    error_details              STRING,
    CONSTRAINT pk_config_deployments PRIMARY KEY (deployment_id)
)
USING DELTA
COMMENT 'GitOps config apply audit log';

-- ---------------------------------------------------------------------------
-- Subject areas (catalog patterns, landing volume defaults)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS subject_areas (
    subject_area_key           STRING NOT NULL,    -- hr, sales, refdata
    description                STRING,
    catalogs_json              STRING,              -- JSON: data_catalog_pattern, control_catalog_pattern
    schemas_json               STRING,              -- JSON: bronze, silver, gold, *_restricted, files
    landing_volume_json        STRING,              -- JSON: volume_schema, volume_name, raw/archive prefixes
    git_path                   STRING,
    is_active                  BOOLEAN,
    last_applied_git_commit_sha STRING,
    last_applied_ts            TIMESTAMP,
    last_applied_deployment_id STRING,
    CONSTRAINT pk_subject_areas PRIMARY KEY (subject_area_key)
)
USING DELTA
COMMENT 'Subject-area defaults (HR first: edw_hr_{env}, files.landing)';

-- ---------------------------------------------------------------------------
-- Sources (Workday, Dynamics, ERP, etc.) — connection + extract defaults
-- Secret *names* and scope only; values live in Key Vault
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sources (
    source_key                 STRING NOT NULL,
    environment                STRING NOT NULL,    -- all | dev | qat | prod
    source_type                STRING,              -- rest_api | sql | file | lakeflow_connect
    subject_area_key           STRING,
    description                STRING,
    default_load_pattern       STRING,              -- lakeflow_connect (preferred) | api_extract | ...
    connection_json            STRING,              -- JSON: base_url, auth_type, secret_scope, secrets{} (API fallback)
    connect_json               STRING,              -- JSON: connection_name, connector_type, Connect defaults
    extract_defaults_json      STRING,              -- JSON: http_method, pagination, common_params (API fallback)
    secret_scope               STRING,              -- denormalized for quick lookup
    git_path                   STRING,
    is_active                  BOOLEAN,
    last_applied_git_commit_sha STRING,
    last_applied_ts            TIMESTAMP,
    last_applied_deployment_id STRING,
    CONSTRAINT pk_sources PRIMARY KEY (source_key, environment)
)
USING DELTA
COMMENT 'Source systems: endpoints and secret names (no secret values)';

-- ---------------------------------------------------------------------------
-- Source entities (logical tables / Workday reports)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS source_entities (
    entity_key                 STRING NOT NULL,
    subject_area_key           STRING,
    source_key                 STRING,
    entity_name                STRING,              -- path segment under raw/{source_key}/
    source_object              STRING,              -- Workday report name, SQL table, etc.
    load_pattern               STRING,              -- api_extract | lakeflow_connect | file_incremental | cdc
    primary_key_columns        STRING,              -- JSON array of column names
    watermark_column           STRING,
    target_bronze_table        STRING,
    target_silver_table        STRING,
    data_catalog               STRING,              -- e.g. edw_hr_dev
    restricted                 BOOLEAN,             -- true → bronze_restricted / silver_restricted
    supports_full_reprocess    BOOLEAN,
    reprocess_strategy         STRING,              -- full_refresh | time_window | key_based
    is_active                  BOOLEAN,
    git_path                   STRING,
    last_applied_git_commit_sha STRING,
    last_applied_ts            TIMESTAMP,
    last_applied_deployment_id STRING,
    CONSTRAINT pk_source_entities PRIMARY KEY (entity_key)
)
USING DELTA
COMMENT 'Logical entities mapped to bronze/silver tables and extract modes';

-- ---------------------------------------------------------------------------
-- Per-entity, per-environment load configuration (UC Volume paths, API, Connect)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS entity_load_configs (
    entity_key                 STRING NOT NULL,
    environment                STRING NOT NULL,    -- all | dev | qat | prod
    load_pattern               STRING,
    source_object              STRING,
    custom_extract_params      STRING,              -- JSON map (variable params)
    api_config                 STRING,              -- JSON: endpoint_path, method, params, format
    landing_volume             STRING,              -- JSON: volume_catalog, volume_schema, volume_name
    landing_volume_path        STRING,              -- resolved /Volumes/.../raw/{source}/{entity}
    archive_volume_path        STRING,              -- resolved archive base (date partition at move time)
    landing_subpath            STRING,              -- e.g. raw/workday/current_employee_list
    archive_subpath            STRING,              -- e.g. archive/workday/current_employee_list
    auto_loader_options        STRING,              -- JSON cloudFiles.* options
    lakeflow_connect_config    STRING,              -- JSON Connect settings when used
    bronze_schema_evolution_mode STRING,
    git_path                   STRING,
    last_applied_git_commit_sha STRING,
    last_applied_ts            TIMESTAMP,
    last_applied_deployment_id STRING,
    CONSTRAINT pk_entity_load_configs PRIMARY KEY (entity_key, environment)
)
USING DELTA
COMMENT 'Extract + UC Volume landing config; env overlays supported';

-- ---------------------------------------------------------------------------
-- Data contracts (JSON document per version)
-- ---------------------------------------------------------------------------
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
)
USING DELTA
COMMENT 'Published data contracts as JSON from config/contracts';

-- ---------------------------------------------------------------------------
-- Quality rules (global defaults + per-entity)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS quality_rules (
    rule_id                    STRING NOT NULL,
    entity_key                 STRING,              -- NULL/empty = global default
    layer                      STRING,              -- bronze | silver | gold
    rule_name                  STRING,
    rule_type                  STRING,
    enforcement_method         STRING,              -- native_lakeflow | external_library | both
    expression                 STRING,
    library_reference          STRING,
    action_on_failure          STRING,              -- fail | drop | warn | quarantine
    severity                   STRING,
    git_path                   STRING,
    is_default                 BOOLEAN,
    is_active                  BOOLEAN,
    last_applied_git_commit_sha STRING,
    last_applied_ts            TIMESTAMP,
    last_applied_deployment_id STRING,
    CONSTRAINT pk_quality_rules PRIMARY KEY (rule_id)
)
USING DELTA
COMMENT 'Hybrid expectations metadata for Silver (and other layers)';

-- ---------------------------------------------------------------------------
-- Sparse column policies (encrypt / hash / mask / tag)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS column_policies (
    policy_id                  STRING NOT NULL,
    entity_key                 STRING NOT NULL,
    column_name                STRING NOT NULL,
    policy_type                STRING,              -- encrypt | hash | mask | tag_only
    encryption_key_vault_ref   STRING,              -- secret name / key ref (not the key value)
    apply_starting_layer       STRING,              -- silver | gold
    classification             STRING,              -- pii | sensitive | public
    git_path                   STRING,
    is_active                  BOOLEAN,
    last_applied_git_commit_sha STRING,
    last_applied_ts            TIMESTAMP,
    last_applied_deployment_id STRING,
    CONSTRAINT pk_column_policies PRIMARY KEY (entity_key, column_name)
)
USING DELTA
COMMENT 'Sparse physical column policies applied in Silver+';

-- ---------------------------------------------------------------------------
-- Pipeline / workflow asset registry (for reprocess routing)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pipeline_assets (
    asset_id                   STRING NOT NULL,
    entity_key                 STRING,
    subject_area_key           STRING,
    asset_type                 STRING NOT NULL,    -- lakeflow_pipeline | workflow | job | notebook
    asset_name                 STRING NOT NULL,
    resource_name_in_bundle    STRING,
    bundle_path                STRING,
    git_path                   STRING,
    target_layer               STRING,              -- bronze | silver | gold | cross
    depends_on                 ARRAY<STRING>,
    supports_reprocess         BOOLEAN,
    default_schedule           STRING,
    compute_type               STRING,
    parameters                 STRING,              -- JSON map
    is_active                  BOOLEAN,
    last_applied_git_commit_sha STRING,
    last_applied_ts            TIMESTAMP,
    last_applied_deployment_id STRING,
    CONSTRAINT pk_pipeline_assets PRIMARY KEY (asset_id)
)
USING DELTA
COMMENT 'Maps entities/subjects to DABs pipelines and workflows';

-- ---------------------------------------------------------------------------
-- Reprocess requests (GitOps + runtime status)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS reprocess_requests (
    request_id                 STRING NOT NULL,
    subject_area_key           STRING,
    requested_entities         ARRAY<STRING>,
    reprocess_mode             STRING,              -- full | window | keys
    from_watermark             STRING,
    to_watermark               STRING,
    reason                     STRING,
    requested_by               STRING,
    source_file_path           STRING,
    git_commit_sha             STRING,
    github_pr_url              STRING,
    status                     STRING,              -- submitted | approved | executing | completed | failed
    execution_run_id           STRING,
    executed_at                TIMESTAMP,
    result_summary             STRING,              -- JSON
    created_ts                 TIMESTAMP,
    updated_ts                 TIMESTAMP,
    last_applied_git_commit_sha STRING,
    last_applied_ts            TIMESTAMP,
    last_applied_deployment_id STRING,
    CONSTRAINT pk_reprocess_requests PRIMARY KEY (request_id)
)
USING DELTA
COMMENT 'Reprocess-as-code requests and execution status';

-- ---------------------------------------------------------------------------
-- Runtime: watermark + reprocess flags (NOT overwritten by full config apply
-- except via explicit reprocess dispatcher / pipeline helpers)
-- ---------------------------------------------------------------------------
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
)
USING DELTA
COMMENT 'Per-entity incremental watermarks and in-flight reprocess flags';

-- Optional convenience views
CREATE OR REPLACE VIEW v_active_hr_entities AS
SELECT e.*
FROM source_entities e
WHERE e.subject_area_key = 'hr'
  AND e.is_active = true;

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
LEFT JOIN entity_load_configs l
  ON e.entity_key = l.entity_key;
