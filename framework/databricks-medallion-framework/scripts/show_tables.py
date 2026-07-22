"""
Inspect Unity Catalog from VS Code **without a SQL Warehouse**.

This is the "SHOW TABLES" helper: instead of logging into the Databricks UI to
run ``SHOW SCHEMAS`` / ``SHOW TABLES`` / ``SHOW VOLUMES``, run it here and get the
results printed in your terminal. It uses Databricks Connect (serverless).

Examples
--------
    # Control plane + HR data catalog summary for DEV
    python scripts/show_tables.py --env dev

    # A single catalog (all schemas + their tables + volumes)
    python scripts/show_tables.py --catalog edw_hr_dev

    # A single schema
    python scripts/show_tables.py --catalog edw_hr_dev --schema bronze

Setup + docs: see scripts/README.md and scripts/_db_session.py.
SHOW TABLES reference:
https://learn.microsoft.com/azure/databricks/sql/language-manual/sql-ref-syntax-aux-show-tables
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _db_session import get_spark  # noqa: E402


def _rows(spark, sql: str) -> list:
    try:
        return spark.sql(sql).collect()
    except Exception as e:  # catalog/schema may not exist yet
        print(f"    (skipped: {sql} -> {e})")
        return []


def _schema_names(spark, catalog: str) -> list[str]:
    rows = _rows(spark, f"SHOW SCHEMAS IN {catalog}")
    # SHOW SCHEMAS returns a single column (databaseName / namespace).
    return [r[0] for r in rows]


def dump_catalog(spark, catalog: str, only_schema: str | None = None) -> None:
    print(f"\n### Catalog: {catalog}")
    schemas = [only_schema] if only_schema else _schema_names(spark, catalog)
    if not schemas:
        return
    for schema in schemas:
        print(f"  schema: {schema}")
        for r in _rows(spark, f"SHOW TABLES IN {catalog}.{schema}"):
            # SHOW TABLES columns: database, tableName, isTemporary
            name = r["tableName"] if "tableName" in r.__fields__ else r[1]
            print(f"    - table  {name}")
        for r in _rows(spark, f"SHOW VOLUMES IN {catalog}.{schema}"):
            name = r[1] if len(r) > 1 else r[0]
            print(f"    - volume {name}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--env",
        default="dev",
        choices=["dev", "qat", "prod"],
        help="Environment shortcut: shows edw_platform_control_<env> + edw_hr_<env>.",
    )
    parser.add_argument("--catalog", help="Inspect a specific catalog (overrides --env).")
    parser.add_argument("--schema", help="Limit output to a single schema.")
    args = parser.parse_args()

    spark = get_spark(serverless=True)

    if args.catalog:
        dump_catalog(spark, args.catalog, args.schema)
    else:
        dump_catalog(spark, f"edw_platform_control_{args.env}", args.schema)
        dump_catalog(spark, f"edw_hr_{args.env}", args.schema)
    return 0


if __name__ == "__main__":
    sys.exit(main())
