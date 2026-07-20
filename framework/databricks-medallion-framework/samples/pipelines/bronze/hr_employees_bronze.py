"""
Sample Bronze workday_employees.

Prefer Lakeflow Connect staging → this table (see config).
This file demonstrates Volume fallback Auto Loader path for local demos.
"""

import dlt
from pyspark.sql import functions as F

# Volume fallback path (api_extract / file demo)
LANDING_PATH = "/Volumes/edw_hr_dev/files/landing/raw/workday/employees/"


@dlt.table(
    name="workday_employees",
    comment="Bronze workday_employees (source-prefixed). Volume fallback demo.",
    table_properties={"quality": "bronze", "bronze_writer": "framework_dlt"},
)
def bronze_workday_employees():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.header", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(LANDING_PATH)
        .withColumn("_bronze_ingest_ts", F.current_timestamp())
        .withColumn("_source_system", F.lit("workday"))
        .withColumn("_entity", F.lit("hr_employees"))
        .withColumn("_ingest_method", F.lit("api_extract"))
        .withColumn("_file_path", F.col("_metadata.file_path"))
        .withColumn("_file_modification_time", F.col("_metadata.file_modification_time"))
        .withColumn("_run_id", F.lit(spark.conf.get("pipeline_run_id", "sample")))
    )


dlt.expect_or_drop("pk_not_null", "employee_id IS NOT NULL")
