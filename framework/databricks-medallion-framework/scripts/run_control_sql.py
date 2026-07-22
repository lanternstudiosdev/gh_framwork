"""
Run the control-plane DDL SQL files from VS Code **without a SQL Warehouse**.

This executes the bootstrap SQL under ``sql/control/`` on Databricks serverless
compute via Databricks Connect, so an engineer can stand up (or update) the
control catalog + tables without opening the Databricks UI.

By default it runs, in order:
    sql/control/00_create_catalogs.sql
    sql/control/01_create_schemas.sql
    sql/control/02_control_tables.sql

Catalog creation (00) is separated from schema/volume creation (01) because a
metastore admin may own catalogs (and UC connections) outside the DAB
deployment. Pass ``--files`` to run only a subset if an admin has already
created the catalogs.

The SQL files are TEMPLATES containing two tokens, which this runner renders
before executing:
    {env}              -> the ``--env`` value (dev | qat | prod)
    {storage_account}  -> config/environments.yaml (environments.<env>.storage_account)

So you never hardcode the environment or the ADLS storage account per file —
update the storage account for an environment in ONE place
(config/environments.yaml), or pass ``--storage-account`` to override it.

Examples
--------
    # Create/refresh the DEV control plane (default files, default env)
    python scripts/run_control_sql.py --env dev

    # Same for QAT (catalog names auto-rewritten to edw_*_qat)
    python scripts/run_control_sql.py --env qat

    # Run just one file
    python scripts/run_control_sql.py --files sql/control/02_control_tables.sql

    # Preview the rendered statements without executing them
    python scripts/run_control_sql.py --env dev --dry-run

Setup + docs: see scripts/README.md and scripts/_db_session.py.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml

# Repo-relative import of the shared session helper (works when run from the
# framework root: ``python scripts/run_control_sql.py``).
sys.path.insert(0, str(Path(__file__).resolve().parent))
from _db_session import get_spark  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
ENV_CONFIG = ROOT / "config" / "environments.yaml"

# Default DDL files, in dependency order.
DEFAULT_FILES = [
    "sql/control/00_create_catalogs.sql",
    "sql/control/01_create_schemas.sql",
    "sql/control/02_control_tables.sql",
]


def resolve_storage_account(env: str, override: str | None = None) -> str:
    """Return the ADLS storage account for ``env``.

    Uses ``--storage-account`` when given, else
    ``config/environments.yaml`` (``environments.<env>.storage_account``).
    """
    if override:
        return override
    if not ENV_CONFIG.exists():
        raise SystemExit(f"Missing storage config: {ENV_CONFIG.relative_to(ROOT)}")
    with ENV_CONFIG.open(encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh) or {}
    entry = (cfg.get("environments") or {}).get(env) or {}
    account = entry.get("storage_account")
    if not account:
        raise SystemExit(
            f"No storage_account for env '{env}' in "
            f"{ENV_CONFIG.relative_to(ROOT)} (environments.{env}.storage_account)"
        )
    return account


def render(sql_text: str, env: str, storage_account: str) -> str:
    """Substitute the ``{env}`` and ``{storage_account}`` tokens in a SQL file."""
    return sql_text.replace("{storage_account}", storage_account).replace("{env}", env)


def split_statements(sql_text: str) -> list[str]:
    """Split a SQL script into individual statements.

    Quote-aware: ``--`` line comments and ``;`` statement terminators are ignored
    when they appear inside a single-quoted string literal (e.g. a ``COMMENT``
    that itself contains ``;`` or ``--``). ``''`` is treated as an escaped quote.
    Commented-out example statements (e.g. the GRANT examples) are dropped.
    """
    statements: list[str] = []
    buf: list[str] = []
    i, n = 0, len(sql_text)
    in_str = False
    while i < n:
        ch = sql_text[i]
        if in_str:
            buf.append(ch)
            if ch == "'":
                if i + 1 < n and sql_text[i + 1] == "'":  # escaped ''
                    buf.append("'")
                    i += 2
                    continue
                in_str = False
            i += 1
            continue
        if ch == "'":
            in_str = True
            buf.append(ch)
            i += 1
        elif ch == "-" and i + 1 < n and sql_text[i + 1] == "-":  # line comment
            nl = sql_text.find("\n", i)
            i = n if nl == -1 else nl
        elif ch == ";":
            stmt = "".join(buf).strip()
            if stmt:
                statements.append(stmt)
            buf = []
            i += 1
        else:
            buf.append(ch)
            i += 1
    tail = "".join(buf).strip()
    if tail:
        statements.append(tail)
    return statements


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--env",
        default="dev",
        choices=["dev", "qat", "prod"],
        help="Target environment; fills the {env} token (default: dev).",
    )
    parser.add_argument(
        "--storage-account",
        default=None,
        help="Override the ADLS storage account (default: config/environments.yaml).",
    )
    parser.add_argument(
        "--files",
        nargs="*",
        default=DEFAULT_FILES,
        help="SQL files to run (repo-relative), in order. Defaults to 00/01/02.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the statements that would run, but do not execute them.",
    )
    args = parser.parse_args()

    storage_account = resolve_storage_account(args.env, args.storage_account)

    print(f"Control SQL runner  (env={args.env}, dry_run={args.dry_run})")
    print(f"Storage account   : {storage_account}")
    print("-" * 60)

    spark = None if args.dry_run else get_spark(serverless=True)

    for rel in args.files:
        path = ROOT / rel
        if not path.exists():
            print(f"ERROR: file not found: {rel}")
            return 1
        sql_text = render(path.read_text(encoding="utf-8"), args.env, storage_account)
        statements = split_statements(sql_text)
        print(f"\n=== {rel}  ({len(statements)} statement(s)) ===")
        for i, stmt in enumerate(statements, 1):
            preview = " ".join(stmt.split())[:100]
            print(f"  [{i:>2}] {preview}{'...' if len(preview) == 100 else ''}")
            if not args.dry_run:
                spark.sql(stmt)

    print("-" * 60)
    print("DRY RUN complete (nothing executed)." if args.dry_run else "Control SQL applied.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
