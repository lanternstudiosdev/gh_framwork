"""Validate HR dynamic registration plan without Databricks / DLT."""

from pathlib import Path
import sys
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from pipelines.hr.registration import (
    plan_bronze_registrations,
    plan_silver_registrations,
    validate_registration_plan,
    registration_plan_summary,
    filter_entities_by_scope,
)


def _load_hr_entities():
    data = yaml.safe_load((ROOT / "config/entities/hr.yaml").read_text(encoding="utf-8"))
    entities = []
    for ent in data["entities"]:
        e = dict(ent)
        load = dict(e.get("load_config") or {})
        connect = dict(load.get("lakeflow_connect_config") or {})
        for k in ("connect_output_table", "raw_table"):
            if k in connect and isinstance(connect[k], str):
                connect[k] = connect[k].replace("{data_catalog}", "edw_hr_dev")
        load["lakeflow_connect_config"] = connect
        e["load_config"] = load
        e["data_catalog"] = "edw_hr_dev"
        e["subject_area_key"] = "hr"
        entities.append(e)
    return entities


def test_filter_restricted_scope():
    ents = _load_hr_entities()
    std = filter_entities_by_scope(ents, restricted_scope=False)
    restr = filter_entities_by_scope(ents, restricted_scope=True)
    assert all(not e.get("restricted") for e in std)
    assert all(e.get("restricted") for e in restr)
    assert len(std) + len(restr) == len(ents)
    assert len(restr) >= 1


def test_bronze_plan_connect_src_ownership():
    ents = _load_hr_entities()
    bronze = plan_bronze_registrations(ents, environment="dev", restricted_scope=False)
    assert bronze
    for b in bronze:
        assert b.load_kind == "lakeflow_connect"
        assert b.table_name.startswith("workday_")
        assert b.connect_source_table.endswith("__src")
        assert "bronze_connect" not in b.connect_source_table
        assert b.target_schema == "bronze"
        assert b.bronze_writer == "framework_dlt"


def test_bronze_plan_restricted_schema():
    ents = _load_hr_entities()
    bronze = plan_bronze_registrations(ents, environment="dev", restricted_scope=True)
    assert bronze
    for b in bronze:
        assert b.restricted
        assert b.target_schema == "bronze_restricted"
        assert "bronze_restricted" in (b.connect_source_table or "")


def test_silver_plan_and_validate():
    ents = _load_hr_entities()
    for scope in (False, True):
        bronze = plan_bronze_registrations(
            ents, environment="dev", restricted_scope=scope
        )
        silver = plan_silver_registrations(
            ents, environment="dev", restricted_scope=scope
        )
        errors = validate_registration_plan(bronze, silver)
        assert errors == [], errors
        summary = registration_plan_summary(bronze, silver)
        assert summary["bronze_count"] == len(bronze)
        assert summary["silver_count"] == len(silver)


def test_no_duplicate_table_names_across_full_hr():
    ents = _load_hr_entities()
    all_bronze = []
    for scope in (False, True):
        all_bronze.extend(
            plan_bronze_registrations(ents, environment="dev", restricted_scope=scope)
        )
    names = [b.table_name for b in all_bronze]
    assert len(names) == len(set(names))
