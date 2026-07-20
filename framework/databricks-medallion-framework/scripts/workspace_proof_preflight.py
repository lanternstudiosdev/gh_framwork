#!/usr/bin/env python3
"""
Local preflight before / during workspace proof (option 8).

Does not require a Databricks cluster. Checks repo + config readiness.
Optional: --check-cli runs `databricks` if on PATH.

Usage:
  python scripts/workspace_proof_preflight.py --env dev
  python scripts/workspace_proof_preflight.py --env dev --check-cli
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def ok(msg: str) -> None:
    print(f"  [OK]  {msg}")


def warn(msg: str) -> None:
    print(f"  [WARN] {msg}")


def fail(msg: str, errors: list) -> None:
    print(f"  [FAIL] {msg}")
    errors.append(msg)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default="dev", choices=["dev", "qat", "prod"])
    parser.add_argument("--check-cli", action="store_true")
    args = parser.parse_args()
    errors: list = []

    print(f"Workspace proof preflight (env={args.env})")
    print("=" * 60)

    # Required paths
    required = [
        "sql/control/00_create_catalog_and_schema.sql",
        "sql/control/01_control_tables.sql",
        "sql/control/02_hr_data_catalog_skeleton.sql",
        "sql/control/03_workspace_proof_checks.sql",
        "config/entities/hr.yaml",
        "config/sources/workday.yaml",
        "config/pipeline_assets/hr.yaml",
        "config/reprocess_requests/hr/smoke-reprocess-location.yaml",
        "bundles/databricks.yml",
        "src/jobs/apply_control_config.py",
        "src/jobs/reprocess_dispatcher.py",
        "docs/08-workspace-proof-runbook.md",
        "docs/09-reprocess-and-pipeline-assets.md",
    ]
    for rel in required:
        p = ROOT / rel
        if p.exists():
            ok(rel)
        else:
            fail(f"missing {rel}", errors)

    # HR config sanity
    hr = yaml.safe_load((ROOT / "config/entities/hr.yaml").read_text(encoding="utf-8"))
    ents = hr.get("entities") or []
    if len(ents) < 1:
        fail("no HR entities", errors)
    else:
        ok(f"HR entities: {len(ents)}")
    connect_n = sum(
        1 for e in ents if (e.get("load_pattern") or "") == "lakeflow_connect"
    )
    ok(f"lakeflow_connect entities: {connect_n}")
    restricted_n = sum(1 for e in ents if e.get("restricted"))
    ok(f"restricted entities: {restricted_n}")

    for e in ents:
        c = (e.get("load_config") or {}).get("lakeflow_connect_config") or {}
        cot = c.get("connect_output_table") or ""
        if e.get("load_pattern") == "lakeflow_connect":
            if "{data_catalog}" not in cot and "edw_" in cot:
                warn(f"{e['entity_key']}: connect_output_table may be env-hardcoded: {cot}")
            if not str(cot).endswith("__src") and "{data_catalog}" in str(cot):
                # template ends with __src
                if not str(cot).endswith("__src"):
                    fail(f"{e['entity_key']}: connect_output_table should end with __src", errors)

    # pipeline assets
    pa = yaml.safe_load(
        (ROOT / "config/pipeline_assets/hr.yaml").read_text(encoding="utf-8")
    )
    assets = pa.get("assets") or []
    names = {a.get("asset_name") for a in assets}
    for required_name in (
        "hr_workday_orchestration",
        "reprocess_hr_workday",
        "reprocess_orchestrator",
        "hr_workday_bronze",
        "hr_workday_silver",
    ):
        if required_name in names:
            ok(f"pipeline_assets has {required_name}")
        else:
            fail(f"pipeline_assets missing {required_name}", errors)

    reprocess_assets = [
        a for a in assets if a.get("supports_reprocess") and a.get("asset_type") == "workflow"
    ]
    if reprocess_assets:
        ok(f"reprocess workflows: {[a['asset_name'] for a in reprocess_assets]}")
    else:
        fail("no workflow with supports_reprocess=true", errors)

    # smoke reprocess request
    req = yaml.safe_load(
        (ROOT / "config/reprocess_requests/hr/smoke-reprocess-location.yaml").read_text(
            encoding="utf-8"
        )
    )
    if req.get("request_id") and req.get("requested_entities"):
        ok(f"smoke reprocess request_id={req['request_id']}")
    else:
        fail("invalid smoke reprocess YAML", errors)

    # lint
    print("-" * 60)
    print("Running framework lint...")
    lint = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "lint_framework_config.py")],
        cwd=str(ROOT),
    )
    if lint.returncode == 0:
        ok("lint_framework_config.py")
    else:
        fail("lint_framework_config.py failed", errors)

    if args.check_cli:
        print("-" * 60)
        if shutil.which("databricks"):
            ok("databricks CLI on PATH")
            r = subprocess.run(
                ["databricks", "bundle", "validate", "--target", args.env],
                cwd=str(ROOT),
                capture_output=True,
                text=True,
            )
            if r.returncode == 0:
                ok(f"databricks bundle validate --target {args.env}")
            else:
                warn(
                    f"bundle validate failed (auth/workspace?): {r.stderr[:500] or r.stdout[:500]}"
                )
        else:
            warn("databricks CLI not on PATH — skip live validate")

    print("=" * 60)
    if errors:
        print(f"PREFLIGHT FAILED ({len(errors)} issue(s))")
        for e in errors:
            print(f"  - {e}")
        print("\nNext: fix issues, then follow docs/08-workspace-proof-runbook.md")
        return 1

    print("PREFLIGHT OK — proceed with docs/08-workspace-proof-runbook.md in the workspace")
    print(
        f"Catalogs for {args.env}: edw_platform_control_{args.env}, edw_hr_{args.env}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
