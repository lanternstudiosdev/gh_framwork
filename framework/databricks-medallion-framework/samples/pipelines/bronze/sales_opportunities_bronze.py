"""
Sample Bronze for Dynamics 365 opportunities — UC Volume + source-prefixed table name.
"""

import dlt
from pyspark.sql import functions as F

LANDING_PATH = "/Volumes/edw_sales_dev/files/landing/raw/dynamics365/opportunities/"


@dlt.table(
    name="dynamics365_opportunities",
    comment="Bronze dynamics365_opportunities from UC Volume landing.",
    table_properties={"quality": "bronze"},
)
def bronze_dynamics365_opportunities():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.header", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(LANDING_PATH)
        .withColumn("_bronze_ingest_ts", F.current_timestamp())
        .withColumn("_source_system", F.lit("dynamics365"))
        .withColumn("_entity", F.lit("sales_opportunities"))
        .withColumn("_file_path", F.col("_metadata.file_path"))
    )
