"""
UC Volume path helpers for landing + archive.

Convention (per-subject catalog, no subject segment under the volume):
  /Volumes/{volume_catalog}/{volume_schema}/{volume_name}/raw/{source_key}/{entity_name}/
  /Volumes/{volume_catalog}/{volume_schema}/{volume_name}/archive/{source_key}/{entity_name}/[yyyy/MM/dd]/

Pipelines and extract jobs must use these paths — never abfss:// container URLs.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional


def resolve_volume_root(
    volume_catalog: str,
    volume_schema: str = "files",
    volume_name: str = "landing",
) -> str:
    """Return /Volumes/{catalog}/{schema}/{volume} with no trailing slash."""
    cat = volume_catalog.strip("/")
    sch = volume_schema.strip("/")
    vol = volume_name.strip("/")
    return f"/Volumes/{cat}/{sch}/{vol}"


def resolve_raw_path(
    volume_catalog: str,
    source_key: str,
    entity_name: str,
    volume_schema: str = "files",
    volume_name: str = "landing",
    raw_prefix: str = "raw",
    landing_subpath: Optional[str] = None,
) -> str:
    """
    Full UC path for Auto Loader / extract write target.

    If landing_subpath is set (absolute under volume, e.g. raw/workday/employees),
    it wins; otherwise build from raw_prefix/source_key/entity_name.
    """
    root = resolve_volume_root(volume_catalog, volume_schema, volume_name)
    if landing_subpath:
        sub = landing_subpath.strip("/")
        return f"{root}/{sub}"
    return f"{root}/{raw_prefix.strip('/')}/{source_key}/{entity_name}"


def resolve_archive_path(
    volume_catalog: str,
    source_key: str,
    entity_name: str,
    volume_schema: str = "files",
    volume_name: str = "landing",
    archive_prefix: str = "archive",
    archive_subpath: Optional[str] = None,
    archive_partitioning: Optional[str] = "yyyy/MM/dd",
    as_of: Optional[datetime] = None,
) -> str:
    """Full UC path for post-Bronze archive destination (optionally date-partitioned)."""
    root = resolve_volume_root(volume_catalog, volume_schema, volume_name)
    if archive_subpath:
        base = f"{root}/{archive_subpath.strip('/')}"
    else:
        base = f"{root}/{archive_prefix.strip('/')}/{source_key}/{entity_name}"

    if archive_partitioning:
        ts = as_of or datetime.now(timezone.utc)
        # Support simple Java-style patterns used in config
        part = (
            archive_partitioning.replace("yyyy", f"{ts.year:04d}")
            .replace("MM", f"{ts.month:02d}")
            .replace("dd", f"{ts.day:02d}")
            .replace("HH", f"{ts.hour:02d}")
        )
        return f"{base}/{part}"
    return base


def landing_paths_from_entity_cfg(entity_cfg: Dict[str, Any]) -> Dict[str, str]:
    """
    Resolve raw + archive paths from a merged entity config dict
    (entity + load_config + subject defaults).
    """
    load = entity_cfg.get("load_config") or {}
    # load_config fields may already be flattened onto entity_cfg by get_entity_config
    vol = (
        load.get("landing_volume")
        or entity_cfg.get("landing_volume")
        or {}
    )
    if isinstance(vol, str):
        # Allow pre-serialized JSON from control tables
        import json
        vol = json.loads(vol) if vol.startswith("{") else {}

    volume_catalog = (
        vol.get("volume_catalog")
        or entity_cfg.get("data_catalog")
        or entity_cfg.get("volume_catalog")
    )
    if not volume_catalog:
        raise ValueError(
            "Cannot resolve landing volume catalog: entity has no "
            "landing_volume.volume_catalog / data_catalog "
            f"(entity_key={entity_cfg.get('entity_key')!r}, "
            f"source_key={entity_cfg.get('source_key')!r})"
        )
    volume_schema = vol.get("volume_schema", "files")
    volume_name = vol.get("volume_name", "landing")
    raw_prefix = vol.get("raw_prefix") or entity_cfg.get("raw_prefix") or "raw"
    archive_prefix = vol.get("archive_prefix") or entity_cfg.get("archive_prefix") or "archive"
    archive_partitioning = (
        load.get("archive_partitioning")
        or vol.get("archive_partitioning")
        or entity_cfg.get("archive_partitioning")
        or "yyyy/MM/dd"
    )

    source_key = entity_cfg.get("source_key", "unknown")
    entity_name = entity_cfg.get("entity_name") or entity_cfg.get("entity_key")

    landing_subpath = load.get("landing_subpath") or entity_cfg.get("landing_subpath")
    archive_subpath = load.get("archive_subpath") or entity_cfg.get("archive_subpath")

    # Prefer explicit resolved landing_volume_path if already applied to control tables
    explicit_raw = load.get("landing_volume_path") or entity_cfg.get("landing_volume_path")
    explicit_archive = load.get("archive_volume_path") or entity_cfg.get("archive_volume_path")

    raw = explicit_raw or resolve_raw_path(
        volume_catalog=volume_catalog,
        source_key=source_key,
        entity_name=entity_name,
        volume_schema=volume_schema,
        volume_name=volume_name,
        raw_prefix=raw_prefix,
        landing_subpath=landing_subpath,
    )
    archive = explicit_archive or resolve_archive_path(
        volume_catalog=volume_catalog,
        source_key=source_key,
        entity_name=entity_name,
        volume_schema=volume_schema,
        volume_name=volume_name,
        archive_prefix=archive_prefix,
        archive_subpath=archive_subpath,
        archive_partitioning=archive_partitioning,
    )
    return {
        "volume_root": resolve_volume_root(volume_catalog, volume_schema, volume_name),
        "raw_path": raw,
        "archive_path": archive,
        "volume_catalog": volume_catalog,
        "volume_schema": volume_schema,
        "volume_name": volume_name,
    }


ALLOWED_LAYER_SCHEMAS = (
    "bronze",
    "bronze_restricted",
    "silver",
    "silver_restricted",
    "gold",
    "gold_restricted",
)


def layer_schema_for_entity(entity_cfg: Dict[str, Any], layer: str) -> str:
    """
    Return medallion schema only:
      bronze | bronze_restricted | silver | silver_restricted | gold | gold_restricted
    Never invent intermediate schemas (e.g. bronze_connect).
    """
    restricted = bool(entity_cfg.get("restricted", False))
    base = layer.lower().replace("_restricted", "")
    if base not in ("bronze", "silver", "gold"):
        base = "bronze"
    schema = f"{base}_restricted" if restricted else base
    assert schema in ALLOWED_LAYER_SCHEMAS
    return schema


def table_name_with_source_prefix(
    source_key: str,
    entity_name: str,
    explicit: Optional[str] = None,
) -> str:
    """Prefer explicit target_*_table; else {source_key}_{entity_name}."""
    if explicit:
        return explicit
    return f"{source_key}_{entity_name}"


def resolve_data_catalog(
    entity_cfg: Dict[str, Any],
    environment: str = "dev",
    subject_area_key: Optional[str] = None,
) -> str:
    """
    Resolve subject data catalog for the environment.
    Prefers entity/load_config data_catalog; else edw_{subject}_{env}.
    """
    load = entity_cfg.get("load_config") or {}
    catalog = (
        entity_cfg.get("data_catalog")
        or load.get("data_catalog")
        or (entity_cfg.get("landing_volume") or {}).get("volume_catalog")
        or (load.get("landing_volume") or {}).get("volume_catalog")
    )
    if catalog and "{" not in str(catalog):
        # Replace env token if present
        return str(catalog).replace("{env}", environment)
    subject = subject_area_key or entity_cfg.get("subject_area_key")
    if not subject:
        raise ValueError(
            "Cannot resolve data catalog: no concrete data_catalog and no "
            "subject_area_key/subject provided "
            f"(entity_key={entity_cfg.get('entity_key')!r})"
        )
    return f"edw_{subject}_{environment}"


def resolve_medallion_table_fqn(
    entity_cfg: Dict[str, Any],
    layer: str = "bronze",
    environment: str = "dev",
    table_name: Optional[str] = None,
) -> str:
    """
    Fully-qualified medallion table:
      {catalog}.{bronze|bronze_restricted|...}.{source_entity_table}
    """
    catalog = resolve_data_catalog(entity_cfg, environment=environment)
    schema = layer_schema_for_entity(entity_cfg, layer)
    source_key = entity_cfg.get("source_key", "unknown")
    entity_name = entity_cfg.get("entity_name") or entity_cfg.get("entity_key", "table")
    if layer == "bronze":
        name = table_name or entity_cfg.get("target_bronze_table")
    elif layer == "silver":
        name = table_name or entity_cfg.get("target_silver_table")
    else:
        name = table_name or entity_cfg.get("target_gold_table")
    name = table_name_with_source_prefix(source_key, entity_name, explicit=name)
    return f"{catalog}.{schema}.{name}"


def resolve_connect_output_fqn(
    entity_cfg: Dict[str, Any],
    environment: str = "dev",
) -> str:
    """
    Where Lakeflow Connect lands data (single writer for this object).

    Ownership model:
      - Connect writes:  {catalog}.{bronze|bronze_restricted}.{workday_* }__src
      - Framework DLT bronze owns final: {catalog}.{schema}.{workday_* }
        and reads from the __src table, enriching technical columns.

    Both stay in medallion schemas only (no bronze_connect schema).
    """
    load = entity_cfg.get("load_config") or {}
    connect = entity_cfg.get("lakeflow_connect_config") or load.get("lakeflow_connect_config") or {}
    if isinstance(connect, str):
        import json
        try:
            connect = json.loads(connect)
        except Exception:
            connect = {}

    explicit = connect.get("connect_output_table") or connect.get("raw_table")
    if explicit and "{" not in str(explicit) and "edw_" in str(explicit):
        # Legacy full FQN in config — rewrite catalog segment if env template used
        return str(explicit).replace("{env}", environment).replace("{data_catalog}", resolve_data_catalog(entity_cfg, environment))

    final_bronze = resolve_medallion_table_fqn(entity_cfg, layer="bronze", environment=environment)
    # Connect staging suffix in same schema
    return f"{final_bronze}__src"


def expand_catalog_placeholders(value: str, data_catalog: str, environment: str) -> str:
    """Replace {data_catalog}, {env}, {catalog} in config strings."""
    if not value or not isinstance(value, str):
        return value
    return (
        value.replace("{data_catalog}", data_catalog)
        .replace("{catalog}", data_catalog)
        .replace("{env}", environment)
    )
