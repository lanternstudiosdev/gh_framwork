"""
Higher-level safe Spark SQL operations used by jobs.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, lit

from lib.sql_safe import (
    SqlSafetyError,
    merge_insert_clause,
    merge_set_clause,
    qualified_table,
    sql_ident,
    sql_int,
    sql_str,
    sql_str_list,
)


def upsert_via_merge(
    spark: SparkSession,
    catalog: str,
    schema: str,
    table: str,
    rows: List[Dict[str, Any]],
    key_cols: Sequence[str],
    provenance: Optional[Dict[str, str]] = None,
    array_columns: Optional[Sequence[str]] = None,
) -> int:
    """
    Idempotent upsert using DataFrame + explicit MERGE column list (no SET *).
    Complex values should already be JSON strings, except ``array_columns`` which
    are kept as native lists and cast to ARRAY<STRING> to match the target schema.
    """
    if not rows:
        return 0

    full = qualified_table(catalog, schema, table)
    df = spark.createDataFrame(rows)
    for c in array_columns or []:
        if c in df.columns:
            df = df.withColumn(c, col(c).cast("array<string>"))
    if provenance:
        for k, v in provenance.items():
            if k == "last_applied_ts":
                df = df.withColumn("last_applied_ts", current_timestamp())
            else:
                df = df.withColumn(k, lit(v))

    # Align to existing table columns when possible
    try:
        target_cols = [f.name for f in spark.table(full).schema.fields]
        present = [c for c in target_cols if c in df.columns]
        if present:
            df = df.select(*present)
    except Exception:
        pass

    columns = list(df.columns)
    for k in key_cols:
        if k not in columns:
            raise SqlSafetyError(f"Key column {k!r} missing from upsert DataFrame for {full}")

    temp_view = f"__safe_upsert_{sql_ident(table)}"
    df.createOrReplaceTempView(temp_view)

    on_parts = [f"target.{sql_ident(c)} = source.{sql_ident(c)}" for c in key_cols]
    on_sql = " AND ".join(on_parts)
    update_cols = [c for c in columns if c not in key_cols]
    # Always update all non-key columns; if none, touch a provenance col if present
    if not update_cols:
        update_cols = [c for c in columns if c != key_cols[0]]

    set_sql = merge_set_clause(update_cols, "source")
    insert_sql = merge_insert_clause(columns, "source")

    merge_sql = f"""
    MERGE INTO {full} AS target
    USING {temp_view} AS source
    ON {on_sql}
    WHEN MATCHED THEN UPDATE SET {set_sql}
    WHEN NOT MATCHED THEN INSERT {insert_sql}
    """
    spark.sql(merge_sql)
    return df.count()


def insert_deployment_start(
    spark: SparkSession,
    catalog: str,
    schema: str,
    deployment_id: str,
    git_commit_sha: str,
    git_branch: str,
    triggered_by: str,
    target_control_catalog: str,
    dab_target: str = "",
) -> None:
    """Insert a 'running' row into control.config_deployments to open an audit trail
    for a Config Apply run (git provenance + who triggered it + which DAB target).
    Pair with :func:`update_deployment_end` to close it out."""
    full = qualified_table(catalog, schema, "config_deployments")
    sql = f"""
    INSERT INTO {full}
    (deployment_id, git_commit_sha, git_branch, triggered_by, target_control_catalog,
     dab_target, status, started_ts, tables_applied)
    VALUES (
        {sql_str(deployment_id)},
        {sql_str(git_commit_sha)},
        {sql_str(git_branch)},
        {sql_str(triggered_by)},
        {sql_str(target_control_catalog)},
        {sql_str(dab_target)},
        'running',
        current_timestamp(),
        array()
    )
    """
    spark.sql(sql)


def update_deployment_end(
    spark: SparkSession,
    catalog: str,
    schema: str,
    deployment_id: str,
    status: str,
    tables_applied: List[str],
    error: Optional[str] = None,
) -> None:
    """Close out a config_deployments audit row as 'success' or 'failed', recording the
    completion timestamp, the tables applied, and (on failure) truncated error details."""
    full = qualified_table(catalog, schema, "config_deployments")
    tables_lit = sql_str_list(tables_applied)
    if status == "success":
        sql = f"""
        UPDATE {full}
        SET status = 'success',
            completed_ts = current_timestamp(),
            tables_applied = {tables_lit}
        WHERE deployment_id = {sql_str(deployment_id)}
        """
    else:
        sql = f"""
        UPDATE {full}
        SET status = 'failed',
            completed_ts = current_timestamp(),
            error_details = {sql_str(error or "Unknown error", max_len=4000)},
            tables_applied = {tables_lit}
        WHERE deployment_id = {sql_str(deployment_id)}
        """
    spark.sql(sql)
