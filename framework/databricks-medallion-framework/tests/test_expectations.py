"""
Unit tests for the expectations package (best_practices + hybrid apply)
"""

import pytest
from src.lib.expectations import best_practices, apply_hybrid_expectations


def test_best_practices_not_null():
    # This test would normally use a real DF; here we just sanity-check the function exists and is callable
    assert callable(best_practices.not_null)
    assert callable(best_practices.freshness_24h)
    assert callable(best_practices.no_future_dates)


def test_apply_hybrid_expectations_runs_without_crashing(spark, sample_quality_rules):
    df = spark.createDataFrame(
        [("E-1", "ACTIVE", 100.0), ("E-2", "TERMINATED", -5.0)],
        ["employee_id", "status", "salary"]
    )

    # The hybrid function should not raise even if some rules are not perfectly matched in this test DF
    result = apply_hybrid_expectations(
        df,
        quality_rules=sample_quality_rules,
        layer="silver",
        entity_key="hr_current_employee_list",
        control_catalog="dev_platform_control",
    )
    assert result is not None
    # P0.1 regression: hybrid apply must accept entity_key/control_catalog and
    # return a DataFrame (a missing-arg TypeError previously slipped past tests).
    assert hasattr(result, "columns")
    assert set(["employee_id", "status", "salary"]).issubset(set(result.columns))
