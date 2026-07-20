"""Unit tests for SQL safety helpers."""

import pytest
from src.lib.sql_safe import (
    SqlSafetyError,
    sql_ident,
    sql_str,
    sql_str_list,
    sql_int,
    qualified_table,
    merge_set_clause,
    safe_where_eq,
)


def test_sql_ident_ok():
    assert sql_ident("edw_hr_dev") == "edw_hr_dev"
    assert sql_ident("control") == "control"


def test_sql_ident_rejects_injection():
    with pytest.raises(SqlSafetyError):
        sql_ident("foo; drop table")
    with pytest.raises(SqlSafetyError):
        sql_ident("a-b")


def test_sql_ident_dotted():
    assert sql_ident("edw_hr_dev.bronze.workday_x", allow_dot=True).count(".") == 2
    with pytest.raises(SqlSafetyError):
        sql_ident("edw_hr_dev.bronze.workday-x", allow_dot=True)


def test_sql_str_escapes_quotes():
    assert sql_str("O'Brien") == "'O''Brien'"
    assert sql_str(None) == "NULL"


def test_sql_str_list_and_int():
    assert "array(" in sql_str_list(["a", "b"])
    assert sql_int(10) == "10"
    with pytest.raises(SqlSafetyError):
        sql_int("x")


def test_qualified_and_merge():
    assert qualified_table("edw_platform_control_dev", "control", "sources").endswith(
        ".control.sources"
    )
    assert "target.a = source.a" in merge_set_clause(["a", "b"])
    assert safe_where_eq("entity_key", "hr_x") == "entity_key = 'hr_x'"
