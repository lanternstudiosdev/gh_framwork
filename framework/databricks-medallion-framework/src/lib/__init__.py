"""
Shared libraries for the Databricks Medallion Ingestion Framework.

Imports are intentionally light so pure helpers (volumes, config_merge) work
without Databricks / DLT / full Spark in unit tests.
"""

from .volumes import (
    landing_paths_from_entity_cfg,
    layer_schema_for_entity,
    resolve_raw_path,
    resolve_archive_path,
    resolve_data_catalog,
    resolve_medallion_table_fqn,
    resolve_connect_output_fqn,
    table_name_with_source_prefix,
    expand_catalog_placeholders,
)
from .config_merge import deep_merge, merge_entity_overlay

__all__ = [
    "landing_paths_from_entity_cfg",
    "layer_schema_for_entity",
    "resolve_raw_path",
    "resolve_archive_path",
    "resolve_data_catalog",
    "resolve_medallion_table_fqn",
    "resolve_connect_output_fqn",
    "table_name_with_source_prefix",
    "expand_catalog_placeholders",
    "deep_merge",
    "merge_entity_overlay",
]


def __getattr__(name: str):
    """Lazy-load Spark/DLT-dependent modules on first access."""
    if name in {
        "get_control_catalog",
        "get_entity_config",
        "get_source_config",
        "get_subject_area_config",
        "get_entities_for_subject",
        "get_quality_rules",
        "get_column_policies",
        "get_watermark_state",
        "is_reprocess_requested",
        "mark_reprocess_completed",
        "update_watermark",
    }:
        from . import metadata as _metadata

        return getattr(_metadata, name)
    if name == "apply_column_policies":
        from .security import apply_column_policies

        return apply_column_policies
    if name in {"apply_hybrid_expectations", "expectations"}:
        from . import expectations as _expectations
        from .expectations import apply_hybrid_expectations

        if name == "expectations":
            return _expectations
        return apply_hybrid_expectations
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
