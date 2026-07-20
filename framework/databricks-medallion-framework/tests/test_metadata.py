"""
Unit tests for src/lib/metadata.py

We heavily mock the spark.sql layer so these tests run in any CI without Databricks.
"""

import pytest
from unittest.mock import patch, MagicMock
from src.lib import metadata


def test_get_control_catalog_from_conf(mock_spark):
    with patch.object(metadata, "spark", mock_spark):
        mock_spark.conf.get.return_value = "prod_platform_control"
        assert metadata.get_control_catalog() == "prod_platform_control"


def test_get_quality_rules_includes_defaults(mock_spark, sample_quality_rules):
    with patch.object(metadata, "spark", mock_spark):
        # First call returns entity-specific, second returns defaults
        mock_spark.sql.return_value.collect.side_effect = [
            [MagicMock(asDict=lambda: sample_quality_rules[1])],  # entity specific
            [MagicMock(asDict=lambda: sample_quality_rules[0])],  # defaults
        ]

        rules = metadata.get_quality_rules("hr_current_employee_list", "silver", include_defaults=True)
        assert len(rules) == 2
        assert any(r["is_default"] for r in rules)


def test_is_reprocess_requested(mock_spark):
    with patch.object(metadata, "spark", mock_spark):
        mock_row = MagicMock()
        mock_row.asDict.return_value = {
            "request_id": "reprocess-123",
            "reprocess_mode": "full",
            "status": "approved",
        }
        mock_spark.sql.return_value.collect.return_value = [mock_row]

        result = metadata.is_reprocess_requested("hr_current_employee_list")
        assert result["is_reprocess"] is True
        assert result["request_id"] == "reprocess-123"


def test_update_watermark_executes_merge(mock_spark):
    with patch.object(metadata, "spark", mock_spark):
        metadata.update_watermark("hr_current_employee_list", "2025-10-01T00:00:00Z", "run-456", 12345)
        # Just verify a MERGE was attempted
        assert "MERGE" in mock_spark.sql.call_args[0][0].upper()
