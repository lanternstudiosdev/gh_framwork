#!/usr/bin/env python3
"""
CI lint for medallion framework config + naming conventions.

Checks:
  - Valid YAML under config/ and samples/config/
  - No bronze_connect schema references
  - No abfss:// in pipeline source (src/pipelines, samples/pipelines)
  - Base entity YAML (not env overlays) has no hardcoded edw_*_dev catalogs in connect FQNs
  - Table names use source_key prefix when target_* set
  - load_pattern is known
  - HR registration plan validates (Connect __src ownership)

Exit code 1 on any failure.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "config"
SAMPLES_CONFIG = ROOT / "samples" / "config"
SRC = ROOT / "src"
SAMPLES_PIPELINES = ROOT / "samples" / "pipelines"

KNOWN_LOAD_PATTERNS = {
    "lakeflow_connect",
    "cdc",
    "connect",
    "api_extract",
    "custom_extract",
    "api_paged",
    "file_incremental",
    "snapshot_watermark",
}

ENV_OVERLAY_RE = re.compile(r"\.(dev|qat|prod)\.ya?ml$", re.I)
HARD_CATALOG_RE = re.compile(r"edw_[a-z0-9]+_(dev|qat|prod)\.", re.I)
BRONZE_CONNECT_RE = re.compile(r"bronze_connect", re.I)
ABFSS_RE = re.compile(r"abfss://", re.I)


def _load_yaml(path: Path):
    with path.open(encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def _iter_yaml(base: Path):
    if not base.is_dir():
        return
    for p in base.rglob("*.yaml"):
        yield p
    for p in base.rglob("*.yml"):
        yield p


def lint_yaml_parse(errors: list) -> None:
    for base in (CONFIG, SAMPLES_CONFIG):
        for path in _iter_yaml(base):
            try:
                _load_yaml(path)
            except Exception as e:
                errors.append(f"YAML parse error {path.relative_to(ROOT)}: {e}")


def lint_no_bronze_connect(errors: list) -> None:
    for base in (CONFIG, SAMPLES_CONFIG, SRC, SAMPLES_PIPELINES):
        if not base.exists():
            continue
        for path in base.rglob("*"):
            if path.suffix not in {".yaml", ".yml", ".py", ".md", ".sql"}:
                continue
            # Allow mentions in docs that forbid the pattern
            if "docs" in path.parts or "_archive" in path.parts:
                continue
            if path.name in {"lint_framework_config.py", "test_volumes.py", "registration.py"}:
                text = path.read_text(encoding="utf-8", errors="ignore")
                # registration validates against bronze_connect — skip
                if "registration.py" in path.name or "lint_framework" in path.name:
                    continue
            text = path.read_text(encoding="utf-8", errors="ignore")
            # Schema/path usage only (not function names like _bronze_from_connect)
            if re.search(r"(^|[^A-Za-z_])bronze_connect([^A-Za-z_]|$)", text):
                if re.search(
                    r"(never|forbidden|do not|don't|not use).{0,40}bronze_connect",
                    text,
                    re.I | re.S,
                ):
                    continue
                errors.append(f"bronze_connect reference: {path.relative_to(ROOT)}")


def lint_no_abfss_in_pipelines(errors: list) -> None:
    for base in (SRC / "pipelines", SAMPLES_PIPELINES):
        if not base.exists():
            continue
        for path in base.rglob("*.py"):
            text = path.read_text(encoding="utf-8", errors="ignore")
            if ABFSS_RE.search(text):
                errors.append(f"abfss:// in pipeline code: {path.relative_to(ROOT)}")


def lint_entity_files(errors: list) -> None:
    for path in _iter_yaml(CONFIG / "entities"):
        if ENV_OVERLAY_RE.search(path.name):
            continue  # overlays may set concrete catalogs
        data = _load_yaml(path) or {}
        entities = data.get("entities") or []
        for ent in entities:
            if not isinstance(ent, dict):
                continue
            ek = ent.get("entity_key", "?")
            sk = ent.get("source_key", "")
            pattern = (ent.get("load_pattern") or "").lower()
            if pattern and pattern not in KNOWN_LOAD_PATTERNS:
                errors.append(f"{path.name}:{ek}: unknown load_pattern {pattern!r}")

            for tkey in ("target_bronze_table", "target_silver_table"):
                tname = ent.get(tkey)
                if tname and sk and not str(tname).startswith(f"{sk}_"):
                    errors.append(
                        f"{path.name}:{ek}: {tkey}={tname!r} should start with {sk}_"
                    )

            load = ent.get("load_config") or {}
            connect = load.get("lakeflow_connect_config") or {}
            for fqn_key in ("raw_table", "connect_output_table", "framework_bronze_table"):
                fqn = connect.get(fqn_key) or ""
                if HARD_CATALOG_RE.search(str(fqn)):
                    errors.append(
                        f"{path.name}:{ek}: hardcoded env catalog in {fqn_key}={fqn!r} "
                        f"(use {{data_catalog}} placeholder)"
                    )
                if BRONZE_CONNECT_RE.search(str(fqn)):
                    errors.append(f"{path.name}:{ek}: bronze_connect in {fqn_key}")


def lint_pipeline_assets(errors: list) -> None:
    path = CONFIG / "pipeline_assets" / "hr.yaml"
    if not path.exists():
        errors.append("missing config/pipeline_assets/hr.yaml")
        return
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    assets = data.get("assets") or []
    names = {a.get("asset_name") for a in assets if isinstance(a, dict)}
    for required in (
        "reprocess_hr_workday",
        "reprocess_orchestrator",
        "hr_workday_orchestration",
    ):
        if required not in names:
            errors.append(f"pipeline_assets missing asset_name={required}")
    reprocess_wf = [
        a
        for a in assets
        if isinstance(a, dict)
        and a.get("asset_type") == "workflow"
        and a.get("supports_reprocess")
    ]
    if not reprocess_wf:
        errors.append("pipeline_assets: no workflow with supports_reprocess=true")


def lint_registration_plan(errors: list) -> None:
    sys.path.insert(0, str(ROOT / "src"))
    from pipelines.hr.registration import (  # type: ignore
        plan_bronze_registrations,
        plan_silver_registrations,
        validate_registration_plan,
    )

    hr_path = CONFIG / "entities" / "hr.yaml"
    if not hr_path.exists():
        errors.append("missing config/entities/hr.yaml")
        return
    data = _load_yaml(hr_path) or {}
    entities = data.get("entities") or []
    # Simulate apply resolving {data_catalog}
    resolved = []
    for ent in entities:
        e = dict(ent)
        load = dict(e.get("load_config") or {})
        connect = dict(load.get("lakeflow_connect_config") or {})
        for k in ("connect_output_table", "raw_table"):
            if k in connect and isinstance(connect[k], str):
                connect[k] = connect[k].replace("{data_catalog}", "edw_hr_dev")
        load["lakeflow_connect_config"] = connect
        e["load_config"] = load
        e["data_catalog"] = "edw_hr_dev"
        e["subject_area_key"] = data.get("subject_area_key", "hr")
        resolved.append(e)

    for restricted_scope in (False, True):
        bronze = plan_bronze_registrations(
            resolved, environment="dev", restricted_scope=restricted_scope
        )
        silver = plan_silver_registrations(
            resolved, environment="dev", restricted_scope=restricted_scope
        )
        errs = validate_registration_plan(bronze, silver)
        for err in errs:
            errors.append(f"registration(restricted={restricted_scope}): {err}")

        # Expect both scopes non-empty for HR
        if restricted_scope and not bronze:
            errors.append("no restricted HR bronze entities planned")
        if not restricted_scope and not bronze:
            errors.append("no standard HR bronze entities planned")


def main() -> int:
    errors: list = []
    lint_yaml_parse(errors)
    lint_no_bronze_connect(errors)
    lint_no_abfss_in_pipelines(errors)
    lint_entity_files(errors)
    lint_pipeline_assets(errors)
    lint_registration_plan(errors)

    if errors:
        print("FRAMEWORK LINT FAILED:")
        for e in errors:
            print(f"  - {e}")
        print(f"\n{len(errors)} issue(s)")
        return 1
    print("FRAMEWORK LINT OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
