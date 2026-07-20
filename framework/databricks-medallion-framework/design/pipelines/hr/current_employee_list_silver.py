"""
Sample Modular Lakeflow Declarative Pipeline - HR Current Employee List (Silver layer)

This is a focused, small pipeline (one domain flow in Silver).
It is defined in Git, deployed via DABs, and is one of many such modular pipelines.

Key characteristics of this design:
- Reads declarative configuration at runtime from the environment's platform_control.
- Applies column policies as **physical transforms** (per the model).
- Supports Hybrid expectations (native Lakeflow + external library).
- Handles incremental vs full reprocess based on watermark_state + reprocess_requests.
- Idempotent and safe for historical reprocessing.
- Bronze is treated as pure source; all cleansing/business logic lives here.

Assumptions:
- The pipeline is parameterized at runtime with CONTROL_CATALOG (e.g. "prod_platform_control")
- Shared libraries exist under src/lib/ (metadata reader, security, expectations)
- We are using the common Lakeflow / DLT-style Python API (adaptable to newer MV syntax).

Typical DABs resource reference:
  resources:
    pipelines:
      hr_current_employee_list_silver:
        name: hr_current_employee_list_silver
        libraries:
          - notebook:
              path: src/pipelines/hr/current_employee_list_silver.py
        configuration:
          control_catalog: ${var.control_catalog}
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

# --- Shared library imports (you will implement these) ---
# from lib.metadata import get_entity_config, get_quality_rules, get_column_policies, get_watermark, is_reprocess_requested
# from lib.security import apply_column_policies
# from lib.expectations import apply_hybrid_expectations   # handles both native and library-based rules

# In a real notebook/pipeline you would have spark and dlt available.

# =============================================================================
# Configuration & Metadata Loading (executed at the start of each refresh)
# =============================================================================
def get_control_catalog() -> str:
    """Read from pipeline configuration or job parameter."""
    # In DABs this comes from the 'configuration' block or widget.
    return spark.conf.get("control_catalog", "dev_platform_control")

CONTROL_CATALOG = get_control_catalog()
CONTROL_SCHEMA = "control"
SUBJECT_AREA = "hr"
ENTITY_KEY = "hr_current_employee_list"
LAYER = "silver"

# Load declarative config for this entity (called once per pipeline run)
entity_config = {}          # get_entity_config(CONTROL_CATALOG, ENTITY_KEY)
column_policies = []        # get_column_policies(CONTROL_CATALOG, ENTITY_KEY)
quality_rules = []          # get_quality_rules(CONTROL_CATALOG, ENTITY_KEY, LAYER)
watermark_info = {}         # get_watermark(CONTROL_CATALOG, ENTITY_KEY)
reprocess_info = {}         # is_reprocess_requested(CONTROL_CATALOG, ENTITY_KEY)

print(f"Running {ENTITY_KEY} Silver pipeline against {CONTROL_CATALOG}")
print(f"Reprocess requested: {reprocess_info.get('is_reprocess', False)}")
print(f"Active column policies: {len(column_policies)}")
print(f"Active quality rules: {len(quality_rules)}")

# =============================================================================
# Bronze Source (pure source layer - read as-is + technical metadata)
# =============================================================================
@dlt.table(
    name="bronze_hr_current_employee_list",
    comment="Raw source view of the Workday current employee list from Bronze (for lineage and debugging)",
    table_properties={"quality": "bronze"}
)
def bronze_hr_current_employee_list():
    """
    In a real setup this would be a live table or streaming table from Bronze.
    For this skeleton we read the managed Bronze table.
    """
    bronze_table = f"{CONTROL_CATALOG.replace('_platform_control', '')}.bronze.workday_current_employee_list"  # simplistic resolution
    return (
        spark.read.table(bronze_table)
        .select("*")  # In reality we would project only needed columns + technical columns
    )

# =============================================================================
# Main Silver Transformation
# =============================================================================
@dlt.table(
    name="silver_hr_current_employee_list",
    comment="Cleaned, conformed, and policy-applied employees. Business keys deduplicated. Column policies applied physically.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.zOrderCols": "hire_date"}
)
@dlt.expect_or_fail("silver_not_null_employee_id", "employee_id IS NOT NULL")   # Example of native Lakeflow expectation (simple/structural)
def silver_hr_current_employee_list():
    """
    Core Silver logic for the current employee list.

    Flow:
    1. Start from Bronze (or previous Silver for incremental).
    2. Determine full vs incremental based on reprocess + watermark.
    3. Apply column policies (physical transforms - encrypt, hash, mask).
    4. Apply cleansing / conformance rules.
    5. Register hybrid expectations (native + library).
    """
    bronze_df = dlt.read("bronze_hr_current_employee_list")

    # --- Reprocess / Watermark logic ---
    if reprocess_info.get("is_reprocess"):
        print("Full reprocess mode - reading all Bronze history")
        source_df = bronze_df
    else:
        # Typical incremental pattern using watermark
        last_watermark = watermark_info.get("current_watermark")
        if last_watermark:
            source_df = bronze_df.filter(F.col("_source_extract_ts") > F.lit(last_watermark))
        else:
            source_df = bronze_df

    # --- Apply physical column policies (the security requirement) ---
    # This function reads the column_policies list and applies encrypt/hash/mask UDFs
    # using keys from Key Vault (via the policy definitions).
    transformed_df = apply_column_policies(source_df, column_policies, CONTROL_CATALOG)

    # --- Business cleansing / conformance ---
    cleaned_df = (
        transformed_df
        .withColumn("status", F.upper(F.trim(F.col("status"))))   # example rule
        .withColumn("hire_date", F.to_date("hire_date"))
        .withColumn("processed_ts", F.current_timestamp())
        .dropDuplicates(["employee_id"])                          # business key dedup
    )

    # --- Register Hybrid Expectations ---
    # The apply_hybrid_expectations helper:
    #   - Adds native dlt.expect* for rules with enforcement_method in ("native_lakeflow", "both")
    #   - Calls external library functions for rules with "external_library"
    #   - Can quarantine bad rows or fail the pipeline depending on action_on_failure
    final_df = apply_hybrid_expectations(
        cleaned_df,
        quality_rules=quality_rules,
        layer=LAYER,
        entity_key=ENTITY_KEY,
        control_catalog=CONTROL_CATALOG
    )

    return final_df

# =============================================================================
# Optional: Additional derived tables in the same small modular pipeline
# (Keep the pipeline cohesive but small)
# =============================================================================
@dlt.table(
    name="silver_hr_current_employee_list_daily",
    comment="Daily headcount snapshot (still part of the same modular pipeline for tight coupling)"
)
def silver_hr_current_employee_list_daily():
    return (
        dlt.read("silver_hr_current_employee_list")
        .groupBy(F.date_trunc("day", "hire_date").alias("day"), "department", "status")
        .agg(
            F.count("*").alias("employee_count")
        )
    )

# =============================================================================
# Post-processing / Watermark update side effect (orchestrated outside or via workflow)
# =============================================================================
# In a full implementation you would have a small post-pipeline task or
# the Workflow that runs after this pipeline succeeds to update watermark_state
# and mark any reprocess_requests as completed.
#
# Example (pseudocode, usually in a separate notebook/task):
#
# if success:
#     update_watermark(CONTROL_CATALOG, ENTITY_KEY, new_watermark)
#     if reprocess_info.get("request_id"):
#         mark_reprocess_completed(CONTROL_CATALOG, reprocess_info["request_id"])
#

print("silver_hr_current_employee_list pipeline definition loaded.")
