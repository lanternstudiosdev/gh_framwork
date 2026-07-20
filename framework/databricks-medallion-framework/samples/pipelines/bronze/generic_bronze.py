"""
Generic Bronze — metadata-driven.

Supports:
  - lakeflow_connect: read Connect staging {table}__src → final bronze table
  - file/api: Auto Loader from UC Volume

Parameters: source (source_key), tables (entity_keys or *)
"""

import json
import dlt
from pyspark.sql import functions as F

from lib.metadata import get_control_catalog, get_entities_for_source, get_entity_config
from lib.volumes import (
    landing_paths_from_entity_cfg,
    table_name_with_source_prefix,
    resolve_connect_output_fqn,
)

SOURCE = spark.conf.get("source", "workday")
TABLES_CSV = spark.conf.get("tables", "*")
CONTROL_CATALOG = get_control_catalog()
try:
    ENVIRONMENT = spark.conf.get("environment", "dev")
except Exception:
    ENVIRONMENT = "dev"


def _get_requested_entities():
    if TABLES_CSV.strip() == "*":
        return get_entities_for_source(SOURCE)
    requested = [t.strip() for t in TABLES_CSV.split(",") if t.strip()]
    all_for_source = get_entities_for_source(SOURCE)
    return [e for e in requested if e in all_for_source]


entities = _get_requested_entities()
print(f"Generic Bronze source={SOURCE} control={CONTROL_CATALOG} entities={entities}")

for entity_key in entities:
    cfg = get_entity_config(entity_key)
    source_key = cfg.get("source_key", SOURCE)
    entity_name = cfg.get("entity_name") or entity_key
    bronze_table_name = table_name_with_source_prefix(
        source_key, entity_name, explicit=cfg.get("target_bronze_table")
    )
    pattern = (cfg.get("load_pattern") or "lakeflow_connect").lower()

    if pattern in ("lakeflow_connect", "cdc", "connect"):
        connect = cfg.get("lakeflow_connect_config") or (cfg.get("load_config") or {}).get(
            "lakeflow_connect_config"
        ) or {}
        if isinstance(connect, str):
            connect = json.loads(connect)
        src_table = (
            connect.get("connect_output_table")
            or connect.get("raw_table")
            or resolve_connect_output_fqn(cfg, environment=ENVIRONMENT)
        )

        @dlt.table(
            name=bronze_table_name,
            comment=f"Bronze {bronze_table_name} from Connect staging {src_table}.",
            table_properties={"quality": "bronze", "bronze_writer": "framework_dlt"},
        )
        def _bronze_from_staging(raw=src_table, ek=entity_key, src=source_key, tn=bronze_table_name):
            try:
                df = spark.read.table(raw)
            except Exception:
                print(f"Connect staging missing: {raw}")
                df = spark.createDataFrame([], "dummy STRING").drop("dummy")
            return (
                df.withColumn("_bronze_ingest_ts", F.current_timestamp())
                .withColumn("_source_system", F.lit(src))
                .withColumn("_entity", F.lit(ek))
                .withColumn("_ingest_method", F.lit("lakeflow_connect"))
            )

        _bronze_from_staging.__name__ = f"bronze_{bronze_table_name}"
    else:
        paths = landing_paths_from_entity_cfg(cfg)
        landing_path = paths["raw_path"]
        auto_opts = cfg.get("auto_loader_options") or (cfg.get("load_config") or {}).get(
            "auto_loader_options"
        ) or {}
        if isinstance(auto_opts, str):
            auto_opts = json.loads(auto_opts)
        auto_opts = {str(k): str(v) for k, v in dict(auto_opts).items()}
        auto_opts.setdefault("cloudFiles.schemaEvolutionMode", "addNewColumns")

        @dlt.table(
            name=bronze_table_name,
            comment=f"Bronze {bronze_table_name} from UC Volume.",
            table_properties={"quality": "bronze", "bronze_writer": "framework_dlt"},
        )
        def _bronze_file(lp=landing_path, opts=auto_opts, ek=entity_key, src=source_key):
            return (
                spark.readStream.format("cloudFiles")
                .options(**opts)
                .load(lp)
                .withColumn("_bronze_ingest_ts", F.current_timestamp())
                .withColumn("_source_system", F.lit(src))
                .withColumn("_entity", F.lit(ek))
                .withColumn("_ingest_method", F.lit("file"))
                .withColumn("_file_path", F.col("_metadata.file_path"))
            )

        _bronze_file.__name__ = f"bronze_{bronze_table_name}"
