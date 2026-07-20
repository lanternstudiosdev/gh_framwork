"""
best_practices.py

Reusable, cross-domain quality rules.

These are referenced via library_reference in quality_rules YAML (e.g. "best_practices.not_null").
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def not_null(df: DataFrame, column: str) -> DataFrame:
    """Simple not-null check (used for business keys etc.)."""
    return df.filter(F.col(column).isNotNull())


def freshness_24h(df: DataFrame, timestamp_col: str = "_source_extract_ts") -> DataFrame:
    """Data must have arrived within the last 24 hours."""
    return df.filter(F.col(timestamp_col) >= F.date_sub(F.current_date(), 1))


def no_future_dates(df: DataFrame, date_col: str = "date") -> DataFrame:
    """Reject rows with dates in the future."""
    return df.filter(F.col(date_col) <= F.current_date())


def unique_business_key(df: DataFrame, key_columns: list) -> DataFrame:
    """Ensure no duplicate business keys (after any required grouping)."""
    if not key_columns:
        return df
    return df.dropDuplicates(key_columns)


def volume_anomaly(df: DataFrame, count_col: str = "cnt", threshold_pct: float = 0.3) -> DataFrame:
    """
    Very simple volume anomaly detector (for Gold or monitoring tables).
    In production this would compare against historical baselines stored in a control table.
    """
    # Placeholder: real implementation would join to a stats table
    return df  # For now a no-op that can be extended
