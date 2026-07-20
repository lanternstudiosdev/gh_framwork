"""
Integration-style tests for jobs/reprocess_dispatcher.py.

The dispatcher module builds SQL for control-plane status transitions and resolves
orchestration workflow names. It creates a SparkSession and WorkspaceClient at import
time, so both are mocked here. Each test swaps in a fresh mock ``spark`` and asserts on
the SQL text the function emits (status transitions, sql_safe escaping, MERGE shape).
"""

from __future__ import annotations

import importlib
import sys
import types
from pathlib import Path

import pytest
from unittest.mock import MagicMock, patch

# Ensure src/ (which holds the ``jobs`` package) is importable.
_SRC = Path(__file__).resolve().parents[1] / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

MODULE = "jobs.reprocess_dispatcher"


def _import_dispatcher():
    """Import the dispatcher with Spark + Databricks SDK mocked at import time.

    ``databricks-sdk`` may not be installed locally, so a stub module is injected
    into ``sys.modules`` before import.
    """
    sys.modules.pop(MODULE, None)
    fake_sdk = types.ModuleType("databricks.sdk")
    fake_sdk.WorkspaceClient = MagicMock()
    fake_databricks = types.ModuleType("databricks")
    fake_databricks.sdk = fake_sdk
    with patch.dict(
        sys.modules,
        {"databricks": fake_databricks, "databricks.sdk": fake_sdk},
    ), patch("pyspark.sql.SparkSession") as SS:
        # conf.get -> None so param resolution falls back to defaults (no MagicMock ints)
        SS.builder.getOrCreate.return_value.conf.get.return_value = None
        mod = importlib.import_module(MODULE)
    return mod


@pytest.fixture
def dispatcher():
    mod = _import_dispatcher()
    # Fresh spark mock per test so we can assert on emitted SQL
    mod.spark = MagicMock()
    mod.spark.sql.return_value.collect.return_value = []
    return mod


def _last_sql(mod) -> str:
    assert mod.spark.sql.called, "expected spark.sql to be called"
    return mod.spark.sql.call_args[0][0]


def test_update_request_to_executing_sets_status_and_escapes_id(dispatcher):
    dispatcher.update_request_to_executing("req-123", run_id="run-9")
    sql = _last_sql(dispatcher)
    assert "status = 'executing'" in sql
    assert "'run-9'" in sql
    assert "'req-123'" in sql


def test_update_request_status_completed_stamps_executed_at(dispatcher):
    dispatcher.update_request_status("req-1", "completed", run_id="run-1")
    sql = _last_sql(dispatcher)
    assert "status = 'completed'" in sql
    assert "executed_at = current_timestamp()" in sql
    assert "'req-1'" in sql


def test_update_request_status_non_completed_has_no_executed_at(dispatcher):
    dispatcher.update_request_status("req-2", "executing")
    sql = _last_sql(dispatcher)
    assert "status = 'executing'" in sql
    assert "executed_at" not in sql


def test_update_request_failed_marks_failed_with_error(dispatcher):
    dispatcher.update_request_failed("req-err", "boom happened")
    sql = _last_sql(dispatcher)
    assert "status = 'failed'" in sql
    assert "boom happened" in sql


def test_force_reprocess_watermark_merges_and_flags_reprocessing(dispatcher):
    dispatcher.force_reprocess_watermark(
        "hr_locations",
        {"request_id": "req-7", "from_watermark": "2024-01-01T00:00:00Z"},
    )
    # A temp view is created from the source row, then a MERGE is issued
    assert dispatcher.spark.createDataFrame.called
    sql = _last_sql(dispatcher)
    assert "MERGE INTO" in sql
    assert "is_reprocessing = true" in sql


def test_find_orchestration_workflow_name_falls_back_to_convention(dispatcher):
    # No pipeline_assets rows -> convention mapping
    dispatcher.spark.sql.return_value.collect.return_value = []
    assert (
        dispatcher.find_orchestration_workflow_name("hr", reprocess=True)
        == "reprocess_hr_workday"
    )
    assert (
        dispatcher.find_orchestration_workflow_name("hr", reprocess=False)
        == "hr_workday_orchestration"
    )


def test_find_orchestration_workflow_name_unknown_subject(dispatcher):
    dispatcher.spark.sql.return_value.collect.return_value = []
    assert (
        dispatcher.find_orchestration_workflow_name("sales", reprocess=True)
        == "reprocess_sales"
    )
    assert (
        dispatcher.find_orchestration_workflow_name("sales", reprocess=False)
        == "sales_orchestration"
    )


def test_find_orchestration_workflow_name_prefers_pipeline_assets(dispatcher):
    row = MagicMock()
    row.asset_name = "custom_reprocess_wf"
    dispatcher.spark.sql.return_value.collect.return_value = [row]
    assert (
        dispatcher.find_orchestration_workflow_name("hr", reprocess=True)
        == "custom_reprocess_wf"
    )
