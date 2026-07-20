"""
Integration-style tests for jobs/apply_control_config.py.

Config Apply loads declarative YAML and resolves landing paths + Connect ownership
before upserting to the control plane. It creates a SparkSession at import time, so
Spark is mocked here. Tests focus on the pure, high-value logic:
  - YAML loading + env-overlay filename detection
  - _resolve_landing_for_entity: Connect __src ownership, table naming, volume paths,
    and source-level connect identity inheritance (used by sales/refdata).
"""

from __future__ import annotations

import importlib
import os
import sys
import tempfile
from pathlib import Path

import pytest
from unittest.mock import MagicMock, patch

# Ensure src/ (which holds the ``jobs`` package) is importable.
_SRC = Path(__file__).resolve().parents[1] / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

MODULE = "jobs.apply_control_config"


def _import_apply():
    """Import the Config Apply job with Spark mocked at import time."""
    sys.modules.pop(MODULE, None)
    with patch("pyspark.sql.SparkSession") as SS:
        SS.builder.getOrCreate.return_value.conf.get.return_value = None
        mod = importlib.import_module(MODULE)
    return mod


@pytest.fixture(scope="module")
def apply_mod():
    return _import_apply()


def test_env_overlay_filename_detection(apply_mod):
    assert apply_mod._is_env_overlay_filename("config/sources/workday.dev.yaml") is True
    assert apply_mod._is_env_overlay_filename("config/sources/workday.yaml") is False
    assert apply_mod._env_from_filename("config/sources/workday.qat.yaml") == "qat"
    assert apply_mod._env_from_filename("config/sources/workday.yaml") is None


def test_json_or_none(apply_mod):
    assert apply_mod._json_or_none(None) is None
    assert apply_mod._json_or_none("already-str") == "already-str"
    assert apply_mod._json_or_none({"a": 1}) == '{"a": 1}'


def test_load_yaml_file_tags_source_file(apply_mod):
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "thing.yaml")
        with open(path, "w", encoding="utf-8") as fh:
            fh.write("source_key: demo\nvalue: 1\n")
        # CONFIG_ROOT is module-global; patch it so _source_file is relative to d
        with patch.object(apply_mod, "CONFIG_ROOT", d):
            data = apply_mod.load_yaml_file(path)
    assert data is not None
    assert data["source_key"] == "demo"
    assert data["_source_file"] == "thing.yaml"


def test_resolve_landing_connect_ownership_and_naming(apply_mod):
    """Connect entity resolves to __src staging + framework-owned final bronze,
    with connection_name inherited from the source's connect identity."""
    ent = {
        "entity_key": "refdata_country_codes",
        "source_key": "refdata_sql",
        "entity_name": "country_codes",
        "source_object": "dbo.CountryCodes",
        "load_pattern": "lakeflow_connect",
        "restricted": False,
        "load_config": {
            "lakeflow_connect_config": {
                "mode": "incremental",
                "source_object": "dbo.CountryCodes",
                # deliberately NO connection_name -> should inherit from source_cfg
                "target_schema": "bronze",
            }
        },
    }
    subject_defaults = {
        "landing_volume": {"volume_schema": "files", "volume_name": "landing"}
    }
    source_cfg = {
        "connect": {"connection_name": "refdata_sql_connect"},
        "load_defaults": {
            "auto_loader_options": {"cloudFiles.format": "json"}
        },
    }

    out = apply_mod._resolve_landing_for_entity(
        ent,
        "refdata",
        subject_defaults,
        environment="dev",
        source_cfg=source_cfg,
    )

    load = out["load_config"]
    connect = load["lakeflow_connect_config"]

    # Table naming: {source_key}_{entity_name}
    assert out["target_bronze_table"] == "refdata_sql_country_codes"

    # Connect ownership: staging table is __src; framework owns final bronze (no __src)
    assert connect["connect_output_table"].endswith("refdata_sql_country_codes__src")
    assert connect["framework_bronze_table"].endswith(".refdata_sql_country_codes")
    assert not connect["framework_bronze_table"].endswith("__src")
    assert connect["bronze_writer"] == "framework_dlt"
    assert connect["connect_writer"] == "lakeflow_connect"

    # connection_name inherited from source connect identity
    assert connect["connection_name"] == "refdata_sql_connect"

    # Env-aware catalog + volume landing paths resolved
    assert out["data_catalog"] == "edw_refdata_dev"
    assert load["landing_volume_path"].startswith("/Volumes/edw_refdata_dev/")
    assert load["bronze_table_fqn"].endswith(".refdata_sql_country_codes")


def test_resolve_landing_restricted_targets_restricted_schema(apply_mod):
    ent = {
        "entity_key": "refdata_cost_centers",
        "source_key": "refdata_sql",
        "entity_name": "cost_centers",
        "load_pattern": "lakeflow_connect",
        "restricted": True,
        "load_config": {"lakeflow_connect_config": {"target_schema": "bronze"}},
    }
    out = apply_mod._resolve_landing_for_entity(
        ent, "refdata", {}, environment="dev", source_cfg={}
    )
    connect = out["load_config"]["lakeflow_connect_config"]
    # Restricted entities land in bronze_restricted
    assert connect["target_schema"] == "bronze_restricted"
    assert ".bronze_restricted." in connect["framework_bronze_table"]
