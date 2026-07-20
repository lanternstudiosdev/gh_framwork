"""Unit tests for UC Volume path resolution."""

import sys
from pathlib import Path

# Import modules directly to avoid pulling pyspark via lib.__init__ when unavailable
_SRC = Path(__file__).resolve().parents[1] / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from lib.volumes import (  # noqa: E402
    resolve_volume_root,
    resolve_raw_path,
    resolve_archive_path,
    landing_paths_from_entity_cfg,
    layer_schema_for_entity,
)
from lib.config_merge import deep_merge, merge_entity_overlay  # noqa: E402
from datetime import datetime, timezone


def test_resolve_volume_root():
    assert resolve_volume_root("edw_hr_dev", "files", "landing") == "/Volumes/edw_hr_dev/files/landing"


def test_resolve_raw_path_convention():
    path = resolve_raw_path(
        volume_catalog="edw_hr_dev",
        source_key="workday",
        entity_name="current_employee_list",
    )
    assert path == "/Volumes/edw_hr_dev/files/landing/raw/workday/current_employee_list"


def test_resolve_raw_path_explicit_subpath():
    path = resolve_raw_path(
        volume_catalog="edw_hr_dev",
        source_key="workday",
        entity_name="ignored",
        landing_subpath="raw/workday/location",
    )
    assert path.endswith("/raw/workday/location")


def test_resolve_archive_partition():
    ts = datetime(2026, 7, 13, tzinfo=timezone.utc)
    path = resolve_archive_path(
        volume_catalog="edw_hr_dev",
        source_key="workday",
        entity_name="location",
        as_of=ts,
    )
    assert path == "/Volumes/edw_hr_dev/files/landing/archive/workday/location/2026/07/13"


def test_landing_paths_from_entity_cfg():
    cfg = {
        "source_key": "workday",
        "entity_name": "job_category",
        "data_catalog": "edw_hr_dev",
        "load_config": {
            "landing_volume": {
                "volume_catalog": "edw_hr_dev",
                "volume_schema": "files",
                "volume_name": "landing",
            }
        },
    }
    paths = landing_paths_from_entity_cfg(cfg)
    assert "raw/workday/job_category" in paths["raw_path"]
    assert paths["volume_catalog"] == "edw_hr_dev"


def test_layer_schema_restricted():
    assert layer_schema_for_entity({"restricted": True}, "bronze") == "bronze_restricted"
    assert layer_schema_for_entity({"restricted": False}, "silver") == "silver"
    assert layer_schema_for_entity({"restricted": True}, "gold") == "gold_restricted"
    # Never invent intermediate schemas
    assert layer_schema_for_entity({"restricted": False}, "bronze_connect") == "bronze"


def test_table_name_with_source_prefix():
    from lib.volumes import table_name_with_source_prefix

    assert (
        table_name_with_source_prefix("workday", "current_employee_list")
        == "workday_current_employee_list"
    )
    assert (
        table_name_with_source_prefix("workday", "x", explicit="workday_custom")
        == "workday_custom"
    )


def test_resolve_connect_output_and_catalog():
    from lib.volumes import (
        resolve_data_catalog,
        resolve_connect_output_fqn,
        resolve_medallion_table_fqn,
        expand_catalog_placeholders,
    )

    cfg = {
        "source_key": "workday",
        "entity_name": "current_employee_list",
        "subject_area_key": "hr",
        "target_bronze_table": "workday_current_employee_list",
        "restricted": False,
        "data_catalog": "edw_hr_dev",
    }
    assert resolve_data_catalog(cfg, environment="dev") == "edw_hr_dev"
    assert (
        resolve_medallion_table_fqn(cfg, layer="bronze", environment="dev")
        == "edw_hr_dev.bronze.workday_current_employee_list"
    )
    assert (
        resolve_connect_output_fqn(cfg, environment="dev")
        == "edw_hr_dev.bronze.workday_current_employee_list__src"
    )
    assert (
        expand_catalog_placeholders(
            "{data_catalog}.bronze.workday_x__src", "edw_hr_qat", "qat"
        )
        == "edw_hr_qat.bronze.workday_x__src"
    )

    restricted = {**cfg, "restricted": True, "target_bronze_table": "workday_payroll_employee_list", "entity_name": "payroll_employee_list"}
    assert "bronze_restricted" in resolve_connect_output_fqn(restricted, environment="dev")


def test_deep_merge_and_entity_overlay():
    base = [
        {
            "entity_key": "hr_location",
            "load_pattern": "api_extract",
            "load_config": {"api": {"params": {"a": "1"}}},
        }
    ]
    overlay = [
        {
            "entity_key": "hr_location",
            "load_pattern": "lakeflow_connect",
            "load_config": {"api": {"params": {"b": "2"}}},
        }
    ]
    merged = merge_entity_overlay(
        base,
        overlay,
        subject_defaults={
            "data_catalog": "edw_hr_dev",
            "landing_volume": {"volume_catalog": "edw_hr_dev"},
        },
    )
    ent = merged[0]
    assert ent["load_pattern"] == "lakeflow_connect"
    assert ent["load_config"]["api"]["params"]["a"] == "1"
    assert ent["load_config"]["api"]["params"]["b"] == "2"
    assert ent["data_catalog"] == "edw_hr_dev"
    assert ent["load_config"]["landing_volume"]["volume_catalog"] == "edw_hr_dev"


def test_deep_merge_simple():
    assert deep_merge({"a": 1, "b": {"c": 2}}, {"b": {"d": 3}}) == {
        "a": 1,
        "b": {"c": 2, "d": 3},
    }
