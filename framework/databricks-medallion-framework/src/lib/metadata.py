"""
metadata.py

Central module for reading declarative configuration and runtime state
from the environment-specific platform_control catalog.

All functions are designed to be called from:
- Lakeflow pipeline code (at refresh time)
- The Config Apply job
- Reprocess dispatchers / Workflows

The control catalog is passed in or discovered from pipeline configuration / widgets.
"""

from typing import Any, Dict, List, Optional
from pyspark.sql import DataFrame, SparkSession
import json

# In a real Databricks job/notebook, `spark` is usually available globally.
# We also support passing it explicitly for testing.
try:
    from pyspark.sql import SparkSession
    spark: SparkSession = SparkSession.builder.getOrCreate()  # type: ignore
except Exception:
    spark = None  # type: ignore


def get_control_catalog() -> str:
    """
    Resolve the platform_control catalog for the current environment.
    Priority:
    1. Pipeline / job configuration parameter 'control_catalog'
    2. dbutils widget
    3. Default for local dev
    """
    try:
        # When running inside a Databricks job or notebook with DABs configuration
        catalog = spark.conf.get("spark.databricks.clusterUsageTags.clusterTargetCatalog", None)
        if catalog and "platform_control" in catalog:
            return catalog

        # Explicit pipeline configuration (recommended way with DABs)
        control_catalog = spark.conf.get("control_catalog", None)
        if control_catalog:
            return control_catalog

        # dbutils widget fallback (common in interactive notebooks)
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
        widget_val = dbutils.widgets.get("control_catalog")
        if widget_val:
            return widget_val
    except Exception:
        pass

    # Local dev / unit test default
    return "edw_platform_control_dev"


def _query_control(table: str, where: Optional[str] = None, limit: Optional[int] = None) -> DataFrame:
    """Internal helper to query a table in the current control catalog."""
    from lib.sql_safe import sql_ident, sql_int, qualified_table

    catalog = get_control_catalog()
    # table must be a simple identifier (not user-composed FQN)
    full_name = qualified_table(catalog, "control", table)
    sql = f"SELECT * FROM {full_name}"
    if where:
        # Callers must pass already-escaped predicates via _eq() helpers below
        sql += f" WHERE {where}"
    if limit is not None:
        sql += f" LIMIT {sql_int(limit)}"
    return spark.sql(sql)


def _eq(column: str, value: str) -> str:
    from lib.sql_safe import safe_where_eq

    return safe_where_eq(column, value)


def _parse_json_field(value: Any) -> Any:
    """Deserialize JSON string columns from control tables when present."""
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text[0] in "{[":
            try:
                return json.loads(text)
            except Exception:
                return value
    return value


def get_entity_config(entity_key: str, environment: Optional[str] = None) -> Dict[str, Any]:
    """
    Return combined entity + load config for one entity.

    Prefer environment-specific load_config row when present, else environment='all'.
    JSON columns (api, auto_loader_options, landing_volume, etc.) are deserialized.
    """
    entities_df = _query_control(
        "source_entities",
        where=f"{_eq('entity_key', entity_key)} AND is_active = true",
    ).collect()

    if not entities_df:
        raise ValueError(f"No active source_entity found for {entity_key}")

    entity = entities_df[0].asDict()

    env = environment
    if not env:
        try:
            env = spark.conf.get("environment", None) or spark.conf.get("target", None)
        except Exception:
            env = None
    env = env or "all"

    from lib.sql_safe import sql_str

    load_df = _query_control(
        "entity_load_configs",
        where=(
            f"{_eq('entity_key', entity_key)} AND environment IN "
            f"({sql_str(env)}, {sql_str('all')})"
        ),
    ).collect()

    # Prefer exact env match over 'all'
    load = {}
    if load_df:
        by_env = {r.asDict().get("environment"): r.asDict() for r in load_df}
        load = by_env.get(env) or by_env.get("all") or load_df[0].asDict()

    # Deserialize free-form JSON config blobs
    for key in (
        "custom_extract_params",
        "auto_loader_options",
        "lakeflow_connect_config",
        "api_config",
        "landing_volume",
        "primary_key_columns",
    ):
        if key in load:
            load[key] = _parse_json_field(load[key])
        if key in entity:
            entity[key] = _parse_json_field(entity[key])

    # Normalize aliases used by pipelines
    if load.get("api_config") and not load.get("api"):
        load["api"] = load["api_config"]

    # Expand {data_catalog}/{env} placeholders left in portable rows
    from lib.volumes import expand_catalog_placeholders, resolve_data_catalog

    env_name = env if env != "all" else "dev"
    try:
        env_name = spark.conf.get("environment", env_name) or env_name
    except Exception:
        pass

    catalog = (
        entity.get("data_catalog")
        or load.get("data_catalog")
        or resolve_data_catalog(
            {**entity, **load, "subject_area_key": entity.get("subject_area_key")},
            environment=env_name,
            subject_area_key=entity.get("subject_area_key"),
        )
    )
    if isinstance(catalog, str) and ("{" in catalog or catalog.endswith("_{env}")):
        catalog = catalog.replace("{env}", env_name)
        if "{data_catalog}" in catalog:
            subject = entity.get("subject_area_key")
            if not subject:
                raise ValueError(
                    f"Cannot resolve data_catalog for entity {entity_key!r}: "
                    "unresolved {data_catalog} placeholder and missing subject_area_key"
                )
            catalog = f"edw_{subject}_{env_name}"

    def _expand(val):
        if isinstance(val, str) and ("{data_catalog}" in val or "{env}" in val or "{catalog}" in val):
            return expand_catalog_placeholders(val, catalog, env_name)
        if isinstance(val, dict):
            return {k: _expand(v) for k, v in val.items()}
        return val

    load = _expand(load)
    entity = {**entity, "data_catalog": catalog}
    for key in ("landing_volume_path", "archive_volume_path", "bronze_table_fqn", "silver_table_fqn"):
        if key in load:
            load[key] = _expand(load[key])

    connect = load.get("lakeflow_connect_config")
    if isinstance(connect, dict):
        load["lakeflow_connect_config"] = _expand(connect)
        # Prefer nested connect config on entity root for pipelines
        load["lakeflow_connect_config"] = load["lakeflow_connect_config"]

    merged = {**entity, **load, "load_config": load, "data_catalog": catalog}
    if load.get("lakeflow_connect_config"):
        merged["lakeflow_connect_config"] = load["lakeflow_connect_config"]
    return merged


def get_source_config(source_key: str, environment: Optional[str] = None) -> Dict[str, Any]:
    """Return source connection + extract defaults from control.sources."""
    env = environment or "all"
    from lib.sql_safe import sql_str

    rows = _query_control(
        "sources",
        where=(
            f"{_eq('source_key', source_key)} AND environment IN "
            f"({sql_str(env)}, {sql_str('all')})"
        ),
    ).collect()
    if not rows:
        # Table may not exist yet in early deploys
        return {"source_key": source_key}
    by_env = {r.asDict().get("environment"): r.asDict() for r in rows}
    src = by_env.get(env) or by_env.get("all") or rows[0].asDict()
    for key in ("connection_json", "extract_defaults_json", "connection", "extract_defaults"):
        if key in src:
            src[key] = _parse_json_field(src[key])
    if src.get("connection_json") and not src.get("connection"):
        src["connection"] = src["connection_json"]
    if src.get("extract_defaults_json") and not src.get("extract_defaults"):
        src["extract_defaults"] = src["extract_defaults_json"]
    return src


def get_subject_area_config(subject_area_key: str) -> Dict[str, Any]:
    """Return subject_areas row (catalog patterns, landing volume defaults)."""
    rows = _query_control(
        "subject_areas",
        where=_eq("subject_area_key", subject_area_key),
    ).collect()
    if not rows:
        return {"subject_area_key": subject_area_key}
    row = rows[0].asDict()
    for key in ("catalogs_json", "schemas_json", "landing_volume_json", "catalogs", "schemas", "landing_volume"):
        if key in row:
            row[key] = _parse_json_field(row[key])
    return row


def get_entities_for_subject(subject_area_key: str) -> List[Dict[str, Any]]:
    """Active entities for a subject area (full entity configs)."""
    from lib.sql_safe import qualified_table

    catalog = get_control_catalog()
    full = qualified_table(catalog, "control", "source_entities")
    sql = f"""
        SELECT entity_key
        FROM {full}
        WHERE {_eq("subject_area_key", subject_area_key)} AND is_active = true
        ORDER BY entity_key
    """
    keys = [r.entity_key for r in spark.sql(sql).collect()]
    return [get_entity_config(k) for k in keys]


def get_quality_rules(entity_key: str, layer: str, include_defaults: bool = True) -> List[Dict[str, Any]]:
    """
    Return quality rules for an entity + layer.
    Includes global defaults (is_default=true) if requested.
    """
    catalog = get_control_catalog()

    # Entity-specific rules
    entity_rules = _query_control(
        "quality_rules",
        where=(
            f"{_eq('entity_key', entity_key)} AND {_eq('layer', layer)} "
            f"AND is_active = true"
        ),
    ).collect()

    rules = [r.asDict() for r in entity_rules]

    if include_defaults:
        from lib.sql_safe import sql_str

        default_rules = _query_control(
            "quality_rules",
            where=(
                f"(entity_key IS NULL OR entity_key = '') AND "
                f"layer = {sql_str(layer)} AND is_default = true AND is_active = true"
            ),
        ).collect()
        rules.extend([r.asDict() for r in default_rules])

    return rules


def get_column_policies(entity_key: str) -> List[Dict[str, Any]]:
    """Return sparse column policies for an entity (only columns with special treatment)."""
    policies_df = _query_control(
        "column_policies",
        where=f"{_eq('entity_key', entity_key)} AND is_active = true",
    )
    return [row.asDict() for row in policies_df.collect()]


def get_watermark_state(entity_key: str) -> Dict[str, Any]:
    """Current watermark and reprocess status for an entity."""
    df = _query_control(
        "watermark_state",
        where=_eq("entity_key", entity_key),
    ).collect()

    if not df:
        return {"entity_key": entity_key, "current_watermark": None, "is_reprocessing": False}

    return df[0].asDict()


def is_reprocess_requested(entity_key: str) -> Dict[str, Any]:
    """
    Check if there is an approved reprocess request for this entity.
    Returns the request details if one exists in 'approved' or 'executing' status.
    """
    from lib.sql_safe import qualified_table, sql_str

    catalog = get_control_catalog()
    full = qualified_table(catalog, "control", "reprocess_requests")
    sql = f"""
        SELECT request_id, reprocess_mode, from_watermark, to_watermark, reason, status
        FROM {full}
        WHERE array_contains(requested_entities, {sql_str(entity_key)})
          AND status IN ('approved', 'executing')
        ORDER BY created_ts DESC
        LIMIT 1
    """
    rows = spark.sql(sql).collect()
    if rows:
        req = rows[0].asDict()
        req["is_reprocess"] = True
        return req
    return {"is_reprocess": False}


def mark_reprocess_completed(request_id: str, run_id: str, summary: Dict[str, Any]) -> None:
    """Update a reprocess request after successful execution."""
    from lib.sql_safe import qualified_table, sql_str

    catalog = get_control_catalog()
    full = qualified_table(catalog, "control", "reprocess_requests")
    spark.sql(
        f"""
        UPDATE {full}
        SET status = 'completed',
            execution_run_id = {sql_str(run_id)},
            executed_at = current_timestamp(),
            result_summary = {sql_str(json.dumps(summary), max_len=8000)},
            updated_ts = current_timestamp()
        WHERE request_id = {sql_str(request_id)}
        """
    )


def update_watermark(entity_key: str, new_watermark: str, run_id: str, row_count: int) -> None:
    """Update watermark_state after a successful pipeline run (DataFrame MERGE)."""
    from lib.sql_safe import qualified_table, sql_int

    catalog = get_control_catalog()
    full = qualified_table(catalog, "control", "watermark_state")
    spark.createDataFrame(
        [
            {
                "entity_key": entity_key,
                "current_watermark": new_watermark,
                "last_successful_run_id": run_id,
                "last_row_count": int(row_count),
            }
        ]
    ).createOrReplaceTempView("__wm_upsert_src")
    spark.sql(
        f"""
        MERGE INTO {full} AS target
        USING __wm_upsert_src AS source
        ON target.entity_key = source.entity_key
        WHEN MATCHED THEN UPDATE SET
            current_watermark = source.current_watermark,
            last_successful_run_id = source.last_successful_run_id,
            last_successful_ts = current_timestamp(),
            last_row_count = source.last_row_count,
            is_reprocessing = false,
            reprocess_request_id = NULL,
            updated_ts = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (
            entity_key, current_watermark, last_successful_run_id,
            last_successful_ts, last_row_count, is_reprocessing, updated_ts
        ) VALUES (
            source.entity_key, source.current_watermark, source.last_successful_run_id,
            current_timestamp(), source.last_row_count, false, current_timestamp()
        )
        """
    )


def get_entities_for_pipeline(pipeline_asset_name: str) -> List[str]:
    """Helper: given a pipeline asset, return the list of entity_keys it owns."""
    return [pipeline_asset_name.replace("_silver", "").replace("_gold", "")]


def get_entities_for_source(source_key: str) -> List[str]:
    """Return list of active entity_keys for a given source."""
    from lib.sql_safe import qualified_table

    catalog = get_control_catalog()
    full = qualified_table(catalog, "control", "source_entities")
    sql = f"""
        SELECT entity_key
        FROM {full}
        WHERE {_eq("source_key", source_key)} AND is_active = true
        ORDER BY entity_key
    """
    rows = spark.sql(sql).collect()
    return [row.entity_key for row in rows]

def get_data_contract_columns(entity_key: str) -> List[str]:
    """Return list of columns from the latest contract for an entity."""
    from lib.sql_safe import qualified_table

    catalog = get_control_catalog()
    full = qualified_table(catalog, "control", "data_contracts")
    sql = f"""
        SELECT contract_json
        FROM {full}
        WHERE {_eq("entity_key", entity_key)}
        ORDER BY version DESC
        LIMIT 1
    """
    rows = spark.sql(sql).collect()
    if not rows:
        return []
    contract = json.loads(rows[0].contract_json)
    cols = []
    if "schema" in contract and "columns" in contract["schema"]:
        cols = [c["name"] for c in contract["schema"]["columns"]]
    elif "columns" in contract:
        cols = [c["name"] for c in contract["columns"]]
    return cols
