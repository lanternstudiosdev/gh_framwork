"""
SQL safety helpers for Spark SQL string assembly.

Prefer DataFrame / DeltaTable APIs when possible. When SQL strings are required:
  - Identifiers (catalog, schema, table, column) → sql_ident()
  - String literals → sql_str()
Never interpolate untrusted input without these helpers.
"""

from __future__ import annotations

import re
from typing import Any, Iterable, List, Optional, Sequence

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
# Allow dotted FQN parts validated individually; also allow env-style catalog names
_CATALOG_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class SqlSafetyError(ValueError):
    """Raised when a value is not safe for SQL identifier/literal use."""


def sql_ident(name: str, *, allow_dot: bool = False) -> str:
    """
    Validate and return a SQL identifier.
    If allow_dot=True, validates each segment of a.b.c independently.
    """
    if name is None or not str(name).strip():
        raise SqlSafetyError("SQL identifier is empty")
    text = str(name).strip()
    if allow_dot:
        parts = text.split(".")
        for p in parts:
            if not _IDENT_RE.match(p):
                raise SqlSafetyError(f"Invalid SQL identifier segment: {p!r}")
        return ".".join(parts)
    if not _IDENT_RE.match(text):
        raise SqlSafetyError(f"Invalid SQL identifier: {text!r}")
    return text


def sql_str(value: Any, *, max_len: int = 8000) -> str:
    """Escape a value as a single-quoted SQL string literal (including quotes)."""
    if value is None:
        return "NULL"
    text = str(value)
    if len(text) > max_len:
        text = text[:max_len]
    # Escape single quotes by doubling (ANSI SQL / Spark)
    escaped = text.replace("'", "''")
    return f"'{escaped}'"


def sql_str_list(values: Iterable[Any]) -> str:
    """Build SQL array( 'a', 'b' ) literal from values."""
    items = [sql_str(v) for v in values]
    return "array(" + ",".join(items) + ")" if items else "array()"


def sql_int(value: Any) -> str:
    """Validate and return an integer literal."""
    try:
        return str(int(value))
    except (TypeError, ValueError) as exc:
        raise SqlSafetyError(f"Invalid SQL integer: {value!r}") from exc


def qualified_table(catalog: str, schema: str, table: str) -> str:
    """Return catalog.schema.table with validated identifiers."""
    return f"{sql_ident(catalog)}.{sql_ident(schema)}.{sql_ident(table)}"


def merge_set_clause(columns: Sequence[str], source_alias: str = "source") -> str:
    """Build explicit MERGE UPDATE SET col = source.col, ... (no SET *)."""
    cols = [sql_ident(c) for c in columns]
    return ", ".join(f"target.{c} = {source_alias}.{c}" for c in cols)


def merge_insert_clause(columns: Sequence[str], source_alias: str = "source") -> str:
    """Build explicit MERGE INSERT (cols) VALUES (source.cols)."""
    cols = [sql_ident(c) for c in columns]
    col_list = ", ".join(cols)
    val_list = ", ".join(f"{source_alias}.{c}" for c in cols)
    return f"({col_list}) VALUES ({val_list})"


def safe_where_eq(column: str, value: str) -> str:
    """column = 'escaped_value' with validated column name."""
    return f"{sql_ident(column)} = {sql_str(value)}"
