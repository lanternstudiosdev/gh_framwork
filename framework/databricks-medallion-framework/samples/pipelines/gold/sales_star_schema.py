"""
OPTIONAL analytics demo — Sales Gold star schema.

Not part of the production framework HR path. Standalone DW sample using edw_sales_dev.
Gold is not wired in DABs for production subjects yet.
"""

import dlt
from pyspark.sql import functions as F

SILVER_OPP = "edw_sales_dev.silver.dynamics365_opportunities"
SILVER_CUST = "edw_sales_dev.silver.dynamics365_customers"
SILVER_PROD = "edw_sales_dev.silver.dynamics365_products"

@dlt.table(name="dim_customer_scd1", comment="Gold SCD Type 1 - Customers")
def gold_dim_customer_scd1():
    return (
        spark.read.table(SILVER_CUST)
        .select(
            F.col("customer_id").alias("customer_key"),
            "company_name",
            "industry",
            "region",
            F.current_timestamp().alias("gold_processed_ts")
        )
        .distinct()
    )

@dlt.table(name="dim_product", comment="Gold Dimension - Products")
def gold_dim_product():
    return (
        spark.read.table(SILVER_PROD)
        .select(
            F.col("product_id").alias("product_key"),
            "product_name",
            "category",
            "list_price",
            F.current_timestamp().alias("gold_processed_ts")
        )
        .distinct()
    )

@dlt.table(name="dim_junk_status", comment="Gold Junk Dimension (shared)")
def gold_dim_junk_status_sales():
    return spark.read.table("edw_sales_dev.silver.junk_dimension_status")

@dlt.table(name="dim_calendar", comment="Gold Calendar (7 years, no gaps)")
def gold_dim_calendar_sales():
    return spark.read.table("edw_sales_dev.silver.calendar_dimension_2019_2025")

@dlt.table(name="fact_opportunity", comment="Gold Fact - Opportunities (transactional grain + accumulating elements)")
def gold_fact_opportunity():
    opp = spark.read.table(SILVER_OPP)
    return (
        opp
        .select(
            F.col("opportunity_id").alias("opportunity_key"),
            F.col("customer_id").alias("customer_key"),
            F.col("product_id").alias("product_key"),
            "amount",
            "stage",
            "close_date",
            "owner_id",
            F.date_trunc("day", "close_date").alias("close_day"),
            "junk_key",
            F.current_timestamp().alias("gold_processed_ts")
        )
    )

@dlt.table(name="fact_sales_line_item", comment="Gold Transactional Fact - Line item level (5k+ source)")
def gold_fact_sales_line_item():
    return (
        spark.read.table("edw_sales_dev.silver.sales_transactions_fact")
        .select(
            "transaction_id",
            F.col("opportunity_id").alias("opportunity_key"),
            F.col("customer_id").alias("customer_key"),
            F.col("product_id").alias("product_key"),
            "transaction_date",
            "amount",
            "quantity",
            "channel",
            "junk_key",
            F.current_timestamp().alias("gold_processed_ts")
        )
    )

@dlt.table(name="fact_opportunity_lifecycle", comment="Gold Accumulating Lifecycle Fact - Sales Lead to Opportunity (meaningful stages with timestamps)")
def gold_fact_opportunity_lifecycle():
    life = spark.read.table("edw_sales_dev.silver.sales_lead_opportunity_lifecycle")
    return (
        life
        .select(
            "lifecycle_id",
            F.col("opportunity_id").alias("opportunity_key"),
            "stage",  # Lead, Qualified, Proposal, Negotiation, Closed Won/Lost
            "entered_date",
            "exited_date",
            "duration_days",
            "amount_at_stage",
            "probability",
            "junk_key",
            F.current_timestamp().alias("gold_processed_ts")
        )
    )
