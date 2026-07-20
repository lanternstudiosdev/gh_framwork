"""
Thin Bronze example for Dynamics 365 / Dataverse / CRM sources.

Pattern (UC Volume landing):
1. Scheduled extract (api_extract job or custom notebook) lands JSON/Parquet under
   /Volumes/edw_sales_{env}/files/landing/raw/dynamics365/{entity}/
2. This Auto Loader Bronze pipeline ingests with standard technical metadata.

See config/entities/sales.yaml for declarative config.
"""

import json
import dlt
from pyspark.sql import functions as F
from lib.metadata import get_control_catalog, get_entity_config
from lib.volumes import landing_paths_from_entity_cfg

CONTROL_CATALOG = get_control_catalog()
ENTITY_KEY = "sales_opportunities"
LAYER = "bronze"

try:
    entity_cfg = get_entity_config(ENTITY_KEY)
except Exception as e:
    print(f"WARNING: metadata unavailable ({e})")
    entity_cfg = {
        "entity_key": ENTITY_KEY,
        "source_key": "dynamics365",
        "entity_name": "opportunities",
        "target_bronze_table": "opportunities",
        "landing_volume": {
            "volume_catalog": "edw_sales_dev",
            "volume_schema": "files",
            "volume_name": "landing",
        },
        "landing_subpath": "raw/dynamics365/opportunities",
        "auto_loader_options": {
            "cloudFiles.format": "json",
            "cloudFiles.schemaEvolutionMode": "addNewColumns",
        },
    }

BRONZE_TABLE = entity_cfg.get("target_bronze_table", "opportunities")
paths = landing_paths_from_entity_cfg(entity_cfg)
LANDING_PATH = paths["raw_path"]


@dlt.table(
    name=BRONZE_TABLE,
    comment="Bronze landing for Dynamics 365 sales data (UC Volume + Auto Loader).",
)
def bronze_sales_opportunities():
    """Bronze DLT table: stream Dynamics 365 opportunity files from the UC Volume
    landing path via Auto Loader, adding technical lineage columns
    (ingest timestamp, source system, entity, file path, run id)."""
    options = entity_cfg.get("auto_loader_options") or {
        "cloudFiles.format": "json",
        "cloudFiles.schemaEvolutionMode": "addNewColumns",
    }
    if isinstance(options, str):
        options = json.loads(options)
    options = {str(k): str(v) for k, v in options.items()}

    return (
        spark.readStream.format("cloudFiles")
        .options(**options)
        .load(LANDING_PATH)
        .withColumn("_bronze_ingest_ts", F.current_timestamp())
        .withColumn("_source_system", F.lit("dynamics365"))
        .withColumn("_entity", F.lit(ENTITY_KEY))
        .withColumn("_file_path", F.col("_metadata.file_path"))
        .withColumn("_run_id", F.lit(spark.conf.get("pipeline_run_id", "unknown")))
    )


print(f"Dynamics Bronze UC Volume path: {LANDING_PATH}")
