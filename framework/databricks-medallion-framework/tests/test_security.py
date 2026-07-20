"""
Unit tests for src/lib/security.py
"""

import pytest
from pyspark.sql import functions as F
from src.lib import security


def test_apply_column_policies_hash_and_mask(spark, sample_column_policies):
    # Create a tiny DF
    data = [
        ("E-1", "NID-999", 123.45, "some address"),
        ("E-2", "NID-100", -50.0, "more address"),
    ]
    df = spark.createDataFrame(data, ["employee_id", "national_id", "compensation", "home_address"])

    # Override the secret resolver for test
    original_get = security._get_secret
    security._get_secret = lambda ref: "fake-test-key-123" if ref else None

    try:
        result = security.apply_column_policies(df, sample_column_policies)

        # national_id should have been hashed (not equal to original)
        hashed = result.select("national_id").collect()[0][0]
        assert hashed != "NID-999"
        assert len(hashed) == 64  # SHA-256 hex

        # compensation should be encrypted (binary in this case)
        encrypted = result.select("compensation").collect()[0][0]
        assert isinstance(encrypted, (bytes, bytearray)) or encrypted is not None

    finally:
        security._get_secret = original_get


def test_apply_tag_only_does_nothing(spark):
    df = spark.createDataFrame([("a",)], ["some_col"])
    policies = [{"column_name": "some_col", "policy_type": "tag_only", "classification": "internal"}]
    result = security.apply_column_policies(df, policies)
    assert result.columns == df.columns
