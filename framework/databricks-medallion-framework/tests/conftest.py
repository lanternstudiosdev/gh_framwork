"""
Pytest configuration and common fixtures for the medallion framework lib tests.

These mocks allow the unit tests to run without a real Databricks cluster / spark session.
"""

import pytest
from unittest.mock import MagicMock, patch

try:
    from pyspark.sql import SparkSession, DataFrame
except ImportError:  # local CI without Spark
    SparkSession = None  # type: ignore
    DataFrame = None  # type: ignore


@pytest.fixture(scope="session")
def spark():
    """Provide a local Spark session for tests that need real DataFrames (optional)."""
    if SparkSession is None:
        pytest.skip("pyspark not installed")
    try:
        return (
            SparkSession.builder.master("local[1]").appName("medallion-test").getOrCreate()
        )
    except Exception as exc:  # noqa: BLE001 - JVM/Java unavailable in local CI
        pytest.skip(f"Spark session unavailable (JVM/Java not found): {exc}")


@pytest.fixture
def mock_spark():
    """Mock the global spark object used inside lib/metadata.py etc."""
    mock = MagicMock()
    # Default behavior for .sql() calls
    mock.sql.return_value = MagicMock()
    mock.sql.return_value.collect.return_value = []
    mock.sql.return_value.createOrReplaceTempView = MagicMock()
    # Default control catalog: a valid SQL identifier so sql_safe validation passes.
    # Tests that exercise catalog resolution override this explicitly.
    mock.conf.get.return_value = "test_platform_control"
    return mock


@pytest.fixture
def sample_column_policies():
    return [
        {
            "column_name": "compensation",
            "policy_type": "encrypt",
            "encryption_key_vault_ref": "hr-pii-encryption-key",
            "apply_starting_layer": "silver",
            "classification": "sensitive",
        },
        {
            "column_name": "national_id",
            "policy_type": "hash",
            "apply_starting_layer": "silver",
            "classification": "pii",
        },
    ]


@pytest.fixture
def sample_quality_rules():
    return [
        {
            "rule_name": "silver_not_null_business_keys",
            "layer": "silver",
            "enforcement_method": "both",
            "expression": "employee_id IS NOT NULL",
            "library_reference": "best_practices.not_null",
            "parameters": {"column": "employee_id"},
            "action_on_failure": "fail",
            "severity": "critical",
            "is_default": True,
        },
        {
            "rule_name": "employee_active_has_hire_date",
            "layer": "silver",
            "enforcement_method": "native_lakeflow",
            "expression": "status = 'TERMINATED' OR hire_date IS NOT NULL",
            "action_on_failure": "fail",
            "severity": "critical",
            "is_default": False,
        },
    ]


@pytest.fixture
def sample_watermark():
    return {
        "entity_key": "hr_current_employee_list",
        "current_watermark": "2025-09-01T00:00:00Z",
        "is_reprocessing": False,
        "last_successful_run_id": "run-123",
    }
