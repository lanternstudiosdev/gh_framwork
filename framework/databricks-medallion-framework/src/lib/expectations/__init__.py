"""
src/lib/expectations/

Real expectations package for the medallion framework.

Exports the main apply function + submodules for best practices and domain-specific rules.

Usage in pipelines:
    from lib.expectations import apply_hybrid_expectations
    from lib.expectations.best_practices import not_null, freshness_24h
"""

from .best_practices import (
    not_null,
    freshness_24h,
    no_future_dates,
    unique_business_key,
    volume_anomaly,
)

from .base import apply_hybrid_expectations   # The main dispatcher (moved/refactored from old single file)

__all__ = [
    "apply_hybrid_expectations",
    "not_null",
    "freshness_24h",
    "no_future_dates",
    "unique_business_key",
    "volume_anomaly",
]
