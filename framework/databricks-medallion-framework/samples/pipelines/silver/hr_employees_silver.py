"""
Sample Silver workday_employees.
"""

import dlt
from pyspark.sql import functions as F

BRONZE_TABLE = "edw_hr_dev.bronze.workday_employees"


@dlt.table(
    name="workday_employees",
    comment="Silver workday_employees — cleaned with PII hash for sample.",
    table_properties={"quality": "silver"},
)
def silver_workday_employees():
    bronze = spark.read.table(BRONZE_TABLE)
    silver = (
        bronze.withColumn("email", F.sha2(F.col("email").cast("string"), 256))
        .withColumn("salary", F.sha2(F.col("salary").cast("string"), 256))
        .withColumn("silver_processed_ts", F.current_timestamp())
        .dropDuplicates(["employee_id"])
    )
    dlt.expect_or_drop("silver_email_hashed_not_null", "email IS NOT NULL")
    return silver
