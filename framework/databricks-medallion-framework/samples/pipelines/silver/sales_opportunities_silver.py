"""
Sample Silver for dynamics365_opportunities.
"""

import dlt
from pyspark.sql import functions as F

BRONZE_TABLE = "edw_sales_dev.bronze.dynamics365_opportunities"


@dlt.table(
    name="dynamics365_opportunities",
    comment="Silver dynamics365_opportunities.",
    table_properties={"quality": "silver"},
)
def silver_dynamics365_opportunities():
    bronze = spark.read.table(BRONZE_TABLE)
    silver = (
        bronze.withColumn("silver_processed_ts", F.current_timestamp())
        .dropDuplicates(["opportunity_id"])
    )
    dlt.expect_or_drop("silver_amount_positive", "amount > 0")
    dlt.expect_or_drop("silver_customer_not_null", "customer_id IS NOT NULL")
    return silver
