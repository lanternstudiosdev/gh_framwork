"""
base.py

Core hybrid expectations application logic.

This was previously in the flat expectations.py. It has been moved here as part of
turning expectations into a proper package with many reusable rules.
"""

from typing import List, Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

try:
    import dlt  # type: ignore
except ImportError:  # unit tests / non-pipeline contexts
    dlt = None  # type: ignore

# Import the concrete rule implementations (they live in sibling modules)
from . import best_practices


def apply_hybrid_expectations(
    df: DataFrame,
    quality_rules: List[Dict[str, Any]],
    layer: str,
    entity_key: str,
    control_catalog: str,
    quarantine_table_name: str = None,
) -> DataFrame:
    """
    Hybrid expectations engine (Option 3).

    See the design README and original expectations.py for the full rationale.
    """
    clean_df = df

    native_rules = [r for r in quality_rules if r.get("enforcement_method") in ("native_lakeflow", "both")]
    library_rules = [r for r in quality_rules if r.get("enforcement_method") in ("external_library", "both")]

    # 1. Native expectations (only when running inside a DLT/Lakeflow pipeline)
    if dlt is None and native_rules:
        print("[NATIVE] dlt not available — skipping native expect registration")
    for rule in native_rules:
        if dlt is None:
            break
        rule_name = rule["rule_name"]
        expression = rule.get("expression")
        action = rule.get("action_on_failure", "fail")

        if not expression:
            continue
        try:
            if action == "fail":
                dlt.expect_or_fail(rule_name, expression)
            elif action == "warn":
                dlt.expect_or_warn(rule_name, expression)
            elif action == "drop":
                dlt.expect_or_drop(rule_name, expression)
            else:
                dlt.expect(rule_name, expression)
            print(f"[NATIVE] Registered: {rule_name}")
        except Exception as e:
            print(f"[NATIVE] Dynamic registration warning for {rule_name}: {e}")

    # 2. Library-based rules (dispatch to best_practices or domain modules)
    for rule in library_rules:
        rule_name = rule["rule_name"]
        lib_ref = rule.get("library_reference")
        action = rule.get("action_on_failure", "warn")
        params = rule.get("parameters") or {}

        print(f"[LIBRARY] {rule_name} -> {lib_ref}")

        try:
            if lib_ref == "best_practices.not_null":
                col = params.get("column", "id")
                clean_df = best_practices.not_null(clean_df, col)
            elif lib_ref == "best_practices.freshness_24h":
                clean_df = best_practices.freshness_24h(clean_df)
            elif lib_ref == "best_practices.no_future_dates":
                col = params.get("date_col", "date")
                clean_df = best_practices.no_future_dates(clean_df, col)
            else:
                print(f"  Unknown library_reference: {lib_ref}")

        except Exception as e:
            print(f"ERROR in library rule {rule_name}: {e}")
            if action == "fail":
                raise

    return clean_df
