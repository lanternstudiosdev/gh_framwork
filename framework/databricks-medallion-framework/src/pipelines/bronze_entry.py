"""
Generic Bronze entry — config-driven Lakeflow Declarative Pipeline (subject-agnostic).

One file, reused by every subject area (hr, sales, refdata, ...). The DAB pipeline
resource selects the subject via Spark conf ``subject_area_key`` (and optional
``source_key``); everything else is read from the control plane.

Ownership (Connect path):
  - Lakeflow Connect writes:  {catalog}.{bronze|bronze_restricted}.{src_key_entity}__src
  - This DLT pipeline owns:   {catalog}.{bronze|bronze_restricted}.{src_key_entity}
    by reading __src and adding technical columns.

Fallback (API/file):
  - Auto Loader from UC Volume → same final bronze table names.

Schemas only: bronze | bronze_restricted (never bronze_connect).

Spark conf parameters (set by the DAB pipeline ``configuration`` block):
  subject_area_key  which subject's entities to register (required)
  source_key        optional default source key (entities normally carry their own)
  entity_keys       "*" or comma list to subset entities
  environment       dev | qat | prod
  restricted_scope  "true" targets *_restricted schemas; "false" targets base schemas
"""

from __future__ import annotations

import json
import dlt
from pyspark.sql import functions as F

# Ensure src/ is importable under DLT before the wheel is on sys.path.
import sys
from pathlib import Path

_SRC = Path(__file__).resolve().parents[1]
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from lib.bootstrap import ensure_src_on_path

ensure_src_on_path()

from lib.metadata import get_control_catalog, get_entities_for_subject
from lib.volumes import (
    landing_paths_from_entity_cfg,
    resolve_connect_output_fqn,
    table_name_with_source_prefix,
    layer_schema_for_entity,
)
from pipelines.registration import plan_bronze_registrations


def _conf(key: str, default: str = "") -> str:
    """Read a Spark conf value with a fallback (DLT sets these from the DAB)."""
    try:
        return spark.conf.get(key, default)
    except Exception:
        return default


CONTROL_CATALOG = get_control_catalog()
SUBJECT = _conf("subject_area_key", "")
DEFAULT_SOURCE_KEY = _conf("source_key", "")
ENTITY_KEYS_PARAM = _conf("entity_keys", "*")
ENVIRONMENT = _conf("environment", "dev")
# DAB sets restricted_scope: "true" | "false" so each pipeline targets one schema
RESTRICTED_SCOPE = _conf("restricted_scope", "false").lower() in ("1", "true", "yes")

print(
    f"Bronze starting subject={SUBJECT!r} control={CONTROL_CATALOG} env={ENVIRONMENT} "
    f"entities={ENTITY_KEYS_PARAM} restricted_scope={RESTRICTED_SCOPE} "
    f"(Connect __src → framework bronze)"
)
if not SUBJECT:
    print(
        "WARNING: no subject_area_key configured for this pipeline; "
        "set configuration.subject_area_key in the DAB. Registering no tables."
    )


def _source_key(entity_cfg: dict) -> str:
    return entity_cfg.get("source_key") or DEFAULT_SOURCE_KEY or "unknown"


def _entities():
    if not SUBJECT:
        return []
    try:
        all_ents = get_entities_for_subject(SUBJECT)
    except Exception as e:
        print(f"WARNING: could not load entities from control ({e}); using empty list")
        return []
    if ENTITY_KEYS_PARAM and ENTITY_KEYS_PARAM != "*":
        wanted = {k.strip() for k in ENTITY_KEYS_PARAM.split(",")}
        all_ents = [e for e in all_ents if e.get("entity_key") in wanted]
    # Split standard vs restricted so DABs schema matches UC target
    return [
        e
        for e in all_ents
        if bool(e.get("restricted", False)) == RESTRICTED_SCOPE
    ]


def _auto_loader_options(entity_cfg: dict) -> dict:
    opts = entity_cfg.get("auto_loader_options") or (entity_cfg.get("load_config") or {}).get(
        "auto_loader_options"
    )
    if isinstance(opts, str):
        opts = json.loads(opts)
    if not opts:
        opts = {
            "cloudFiles.format": "json",
            "cloudFiles.schemaEvolutionMode": "addNewColumns",
            "cloudFiles.inferColumnTypes": "true",
        }
    return {str(k): str(v) for k, v in opts.items()}


def _bronze_table_name(entity_cfg: dict) -> str:
    source_key = _source_key(entity_cfg)
    entity_name = entity_cfg.get("entity_name") or entity_cfg["entity_key"]
    return table_name_with_source_prefix(
        source_key,
        entity_name,
        explicit=entity_cfg.get("target_bronze_table"),
    )


def _connect_cfg(entity_cfg: dict) -> dict:
    connect = entity_cfg.get("lakeflow_connect_config") or (
        entity_cfg.get("load_config") or {}
    ).get("lakeflow_connect_config") or {}
    if isinstance(connect, str):
        connect = json.loads(connect)
    return connect or {}


def _register_file_bronze(entity_cfg: dict) -> None:
    """API/file fallback: Auto Loader from UC Volume → final bronze table."""
    entity_key = entity_cfg["entity_key"]
    table_name = _bronze_table_name(entity_cfg)
    paths = landing_paths_from_entity_cfg(entity_cfg)
    raw_path = paths["raw_path"]
    options = _auto_loader_options(entity_cfg)
    source_key = _source_key(entity_cfg)
    restricted = str(bool(entity_cfg.get("restricted", False))).lower()

    print(f"Registering Bronze Auto Loader (fallback): {entity_key} -> {table_name} <- {raw_path}")

    @dlt.table(
        name=table_name,
        comment=f"Bronze {table_name} from UC Volume (API/file fallback).",
        table_properties={
            "pipelines.quality": "bronze",
            "delta.autoOptimize.optimizeWrite": "true",
            "entity_key": entity_key,
            "load_pattern": "api_extract",
            "bronze_writer": "framework_dlt",
            "restricted": restricted,
        },
    )
    def _bronze(
        lp=raw_path,
        opts=options,
        ek=entity_key,
        src=source_key,
    ):
        return (
            spark.readStream.format("cloudFiles")
            .options(**opts)
            .load(lp)
            .withColumn("_bronze_ingest_ts", F.current_timestamp())
            .withColumn("_source_extract_ts", F.col("_bronze_ingest_ts"))
            .withColumn("_source_system", F.lit(src))
            .withColumn("_entity", F.lit(ek))
            .withColumn("_ingest_method", F.lit("api_extract"))
            .withColumn("_file_path", F.col("_metadata.file_path"))
            .withColumn("_file_modification_time", F.col("_metadata.file_modification_time"))
            .withColumn("_run_id", F.lit(spark.conf.get("pipeline_run_id", "unknown")))
        )

    _bronze.__name__ = f"bronze_{table_name}"
    globals()[f"bronze_{table_name}"] = _bronze


def _register_connect_bronze(entity_cfg: dict) -> None:
    """
    Framework owns final bronze table; Connect owns __src staging in same schema.
    """
    entity_key = entity_cfg["entity_key"]
    source_key = _source_key(entity_cfg)
    table_name = _bronze_table_name(entity_cfg)
    connect = _connect_cfg(entity_cfg)

    # Prefer apply-resolved connect_output_table / raw_table; else compute
    connect_src = (
        connect.get("connect_output_table")
        or connect.get("raw_table")
        or resolve_connect_output_fqn(entity_cfg, environment=ENVIRONMENT)
    )
    # If misconfigured to equal final table name only, force __src
    if connect_src.rstrip("/").endswith(f".{table_name}") and not connect_src.endswith(
        f"{table_name}__src"
    ):
        connect_src = f"{connect_src}__src"

    connection_name = connect.get("connection_name") or f"{source_key}_connect"
    restricted = str(bool(entity_cfg.get("restricted", False))).lower()
    target_schema = connect.get("target_schema") or layer_schema_for_entity(entity_cfg, "bronze")

    print(
        f"Registering Bronze (framework owns final): {table_name} "
        f"schema={target_schema} <- Connect {connect_src} (connection={connection_name})"
    )

    @dlt.table(
        name=table_name,
        comment=(
            f"Bronze {table_name}: framework DLT owns this table; "
            f"reads Lakeflow Connect staging {connect_src}."
        ),
        table_properties={
            "pipelines.quality": "bronze",
            "delta.autoOptimize.optimizeWrite": "true",
            "entity_key": entity_key,
            "load_pattern": "lakeflow_connect",
            "bronze_writer": "framework_dlt",
            "connect_writer": "lakeflow_connect",
            "connect_output_table": str(connect_src),
            "connection_name": str(connection_name),
            "restricted": restricted,
        },
    )
    def _bronze_from_connect(
        raw=connect_src,
        ek=entity_key,
        src=source_key,
    ):
        try:
            df = spark.read.table(raw)
        except Exception:
            print(
                f"Connect staging table {raw} not available; empty Bronze shell for {ek}. "
                "Provision Lakeflow Connect to write this __src table first."
            )
            df = spark.createDataFrame([], "dummy STRING").drop("dummy")

        out = (
            df.withColumn("_bronze_ingest_ts", F.current_timestamp())
            .withColumn("_source_system", F.lit(src))
            .withColumn("_entity", F.lit(ek))
            .withColumn("_ingest_method", F.lit("lakeflow_connect"))
            .withColumn("_connect_source_table", F.lit(raw))
            .withColumn("_run_id", F.lit(spark.conf.get("pipeline_run_id", "unknown")))
        )
        if "_source_extract_ts" not in df.columns:
            out = out.withColumn("_source_extract_ts", F.col("_bronze_ingest_ts"))
        return out

    _bronze_from_connect.__name__ = f"bronze_{table_name}"
    globals()[f"bronze_{table_name}"] = _bronze_from_connect


# Plan first (testable without DLT), then register with default-arg closures
_entity_list = _entities()
_plan = plan_bronze_registrations(
    _entity_list,
    environment=ENVIRONMENT,
    entity_keys=ENTITY_KEYS_PARAM,
    restricted_scope=RESTRICTED_SCOPE,
)
print(f"Bronze plan: {len(_plan)} table(s) subject={SUBJECT!r} restricted_scope={RESTRICTED_SCOPE}")
for _item in _plan:
    print(
        f"  plan {_item.entity_key} -> {_item.table_name} "
        f"kind={_item.load_kind} schema={_item.target_schema}"
    )

_by_key = {e["entity_key"]: e for e in _entity_list}
for _item in _plan:
    _ent = _by_key.get(_item.entity_key)
    if not _ent:
        continue
    if _item.load_kind == "lakeflow_connect":
        _register_connect_bronze(_ent)
    elif _item.load_kind in ("api_extract", "file"):
        _register_file_bronze(_ent)
