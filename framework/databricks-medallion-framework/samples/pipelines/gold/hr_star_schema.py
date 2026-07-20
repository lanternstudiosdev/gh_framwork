"""
OPTIONAL analytics demo — HR Gold star schema (SCD1/SCD2, junk, calendar, facts).

This is **not** part of the production HR framework path (Connect → bronze → silver).
It is a standalone DW-style sample for gap/SCD testing with generate_sample_data.py.

Catalogs use edw_hr_dev (aligned naming). Gold is not wired in DABs for HR production.
"""

import dlt
from pyspark.sql import functions as F

# Source tables (after Bronze/Silver)
# Catalog aligned with framework: edw_hr_dev (not legacy dev_hr)
SILVER_EMP = "edw_hr_dev.silver.workday_employees"
SILVER_DEPT = "edw_hr_dev.silver.workday_departments"
GOLD_EMP_HIST = "edw_hr_dev.silver.employee_history_scd2"
GOLD_DEPT_SCD2 = "edw_hr_dev.silver.departments_scd2"

@dlt.table(name="dim_department_scd2", comment="Gold SCD Type 2 - Departments with continuous effective periods")
def gold_dim_department_scd2():
    return (
        spark.read.table(GOLD_DEPT_SCD2)
        .select(
            F.col("department_id").alias("department_key"),
            "department_name",
            "manager_id",
            "location",
            "effective_from",
            "effective_to",
            "is_current",
            "version",
            "change_hash",  # for SCD2 comparison / audit
            F.current_timestamp().alias("gold_processed_ts")
        )
    )

@dlt.table(name="dim_employee_scd1", comment="Gold SCD Type 1 - Current employee state")
def gold_dim_employee_scd1():
    return (
        spark.read.table(SILVER_EMP)
        .select(
            F.col("employee_id").alias("employee_key"),
            "first_name",
            "last_name",
            "department_id",
            "hire_date",
            "status",
            F.current_timestamp().alias("gold_processed_ts")
        )
        .distinct()
    )

@dlt.table(name="dim_junk_status", comment="Gold Junk Dimension")
def gold_dim_junk_status():
    return spark.read.table("edw_hr_dev.silver.junk_dimension_status")

@dlt.table(name="dim_calendar", comment="Gold Calendar Dimension - 7 years daily, no gaps")
def gold_dim_calendar():
    return spark.read.table("edw_hr_dev.silver.calendar_dimension_2019_2025")

@dlt.table(name="fact_employee_headcount_snapshot", comment="Gold Periodic Snapshot Fact - Monthly headcount (periodic)")
def gold_fact_employee_headcount_snapshot():
    snap = spark.read.table("edw_hr_dev.silver.hr_monthly_headcount_snapshot")
    return (
        snap
        .join(spark.read.table("edw_hr_dev.silver.dim_department_scd2"), "department_id", "left")
        .select(
            F.col("snapshot_date").alias("date_key"),
            F.col("department_id").alias("department_key"),
            "headcount",
            "active_headcount",
            "avg_salary",
            "turnover_rate",
            F.current_timestamp().alias("gold_processed_ts")
        )
    )

@dlt.table(name="fact_employee_lifecycle", comment="Gold Accumulating Lifecycle Fact (sales lead/opportunity style for HR demo - promotions, transfers, terminations)")
def gold_fact_employee_lifecycle():
    # In this sample we repurpose the sales lifecycle pattern for HR employee events for demo purposes
    life = spark.read.table("edw_hr_dev.silver.sales_lead_opportunity_lifecycle")
    return (
        life
        .withColumnRenamed("opportunity_id", "employee_id")  # mapping for demo
        .select(
            "lifecycle_id",
            F.col("employee_id").alias("employee_key"),
            "stage",  # e.g. Hired, Promoted, Transferred, OnLeave, Terminated
            "entered_date",
            "exited_date",
            "duration_days",
            "junk_key",
            F.current_timestamp().alias("gold_processed_ts")
        )
    )
