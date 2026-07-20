"""
Pure registration planning for Bronze/Silver (no DLT import), subject-agnostic.

Used by:
  - pipelines/bronze_entry.py / pipelines/silver_entry.py at runtime
  - unit tests and CI to validate the dynamic graph plan without Databricks

Why this exists:
  Loop-based @dlt.table registration is fragile across DBR versions.
  This module makes the *plan* (which tables, load patterns, ownership) testable
  and documents the expected graph before DLT decorators run.

This module is generic across subject areas (hr, sales, refdata, ...). Nothing
here is Workday/HR specific; the subject/source come from each entity's config.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Literal, Optional

from lib.volumes import (
    layer_schema_for_entity,
    resolve_connect_output_fqn,
    resolve_data_catalog,
    table_name_with_source_prefix,
)

LoadKind = Literal["lakeflow_connect", "api_extract", "file", "skip"]


def _entity_source_key(ent: Dict[str, Any]) -> str:
    """Source key for an entity; entities always carry one, ``unknown`` guards typos."""
    return ent.get("source_key") or "unknown"


@dataclass(frozen=True)
class BronzeRegistration:
    """Immutable plan for a single Bronze table registration.

    Describes *what* the Bronze DLT graph will create for one entity (name, schema,
    ingestion kind, ownership) without importing DLT — so it can be asserted in tests.
    """

    entity_key: str
    table_name: str
    load_kind: LoadKind
    restricted: bool
    target_schema: str  # bronze | bronze_restricted
    connect_source_table: Optional[str] = None  # __src FQN
    volume_raw_path: Optional[str] = None
    bronze_writer: str = "framework_dlt"


@dataclass(frozen=True)
class SilverRegistration:
    """Immutable plan for a single Silver table registration.

    Captures the Bronze source FQN, target silver schema, and primary keys that the
    Silver DLT graph will use for one entity — testable without DLT.
    """

    entity_key: str
    table_name: str
    bronze_fqn: str
    restricted: bool
    target_schema: str  # silver | silver_restricted
    primary_key_columns: List[str]


CONNECT_PATTERNS = frozenset({"lakeflow_connect", "cdc", "connect"})
FILE_PATTERNS = frozenset(
    {
        "api_extract",
        "custom_extract",
        "api_paged",
        "file_incremental",
        "snapshot_watermark",
    }
)


def filter_entities_by_scope(
    entities: List[Dict[str, Any]],
    *,
    entity_keys: str = "*",
    restricted_scope: bool = False,
) -> List[Dict[str, Any]]:
    """Apply entity_keys filter and restricted_scope split."""
    result = list(entities)
    if entity_keys and entity_keys != "*":
        wanted = {k.strip() for k in entity_keys.split(",") if k.strip()}
        result = [e for e in result if e.get("entity_key") in wanted]
    return [
        e for e in result if bool(e.get("restricted", False)) == restricted_scope
    ]


def plan_bronze_registrations(
    entities: List[Dict[str, Any]],
    *,
    environment: str = "dev",
    entity_keys: str = "*",
    restricted_scope: bool = False,
) -> List[BronzeRegistration]:
    """Build deterministic bronze registration plan (testable, no DLT)."""
    planned: List[BronzeRegistration] = []
    for ent in filter_entities_by_scope(
        entities, entity_keys=entity_keys, restricted_scope=restricted_scope
    ):
        source_key = _entity_source_key(ent)
        entity_name = ent.get("entity_name") or ent["entity_key"]
        table = table_name_with_source_prefix(
            source_key, entity_name, explicit=ent.get("target_bronze_table")
        )
        pattern = (ent.get("load_pattern") or "lakeflow_connect").lower()
        restricted = bool(ent.get("restricted", False))
        target_schema = layer_schema_for_entity(ent, "bronze")

        if pattern in CONNECT_PATTERNS:
            connect = ent.get("lakeflow_connect_config") or (
                ent.get("load_config") or {}
            ).get("lakeflow_connect_config") or {}
            if isinstance(connect, str):
                import json

                try:
                    connect = json.loads(connect)
                except Exception:
                    connect = {}
            connect_src = (
                connect.get("connect_output_table")
                or connect.get("raw_table")
                or resolve_connect_output_fqn(ent, environment=environment)
            )
            if connect_src.endswith(f".{table}") and not connect_src.endswith(
                f"{table}__src"
            ):
                connect_src = f"{connect_src}__src"
            planned.append(
                BronzeRegistration(
                    entity_key=ent["entity_key"],
                    table_name=table,
                    load_kind="lakeflow_connect",
                    restricted=restricted,
                    target_schema=target_schema,
                    connect_source_table=connect_src,
                    bronze_writer="framework_dlt",
                )
            )
        elif pattern in FILE_PATTERNS:
            from lib.volumes import landing_paths_from_entity_cfg

            paths = landing_paths_from_entity_cfg(ent)
            planned.append(
                BronzeRegistration(
                    entity_key=ent["entity_key"],
                    table_name=table,
                    load_kind="api_extract" if "api" in pattern else "file",
                    restricted=restricted,
                    target_schema=target_schema,
                    volume_raw_path=paths.get("raw_path"),
                    bronze_writer="framework_dlt",
                )
            )
        else:
            planned.append(
                BronzeRegistration(
                    entity_key=ent["entity_key"],
                    table_name=table,
                    load_kind="skip",
                    restricted=restricted,
                    target_schema=target_schema,
                )
            )
    return planned


def plan_silver_registrations(
    entities: List[Dict[str, Any]],
    *,
    environment: str = "dev",
    entity_keys: str = "*",
    restricted_scope: bool = False,
    subject_area_key: Optional[str] = None,
) -> List[SilverRegistration]:
    """Build deterministic silver registration plan (testable, no DLT).

    ``subject_area_key`` is used only to resolve the data catalog when an entity has
    no concrete ``data_catalog``; entities also carry their own ``subject_area_key``
    as a fallback.
    """
    planned: List[SilverRegistration] = []
    for ent in filter_entities_by_scope(
        entities, entity_keys=entity_keys, restricted_scope=restricted_scope
    ):
        source_key = _entity_source_key(ent)
        entity_name = ent.get("entity_name") or ent["entity_key"]
        table = table_name_with_source_prefix(
            source_key, entity_name, explicit=ent.get("target_silver_table")
        )
        load = ent.get("load_config") or {}
        bronze_fqn = load.get("bronze_table_fqn")
        if not bronze_fqn:
            catalog = resolve_data_catalog(
                ent, environment=environment, subject_area_key=subject_area_key
            )
            bschema = layer_schema_for_entity(ent, "bronze")
            btable = table_name_with_source_prefix(
                source_key, entity_name, explicit=ent.get("target_bronze_table")
            )
            bronze_fqn = f"{catalog}.{bschema}.{btable}"

        pks = ent.get("primary_key_columns") or []
        if isinstance(pks, str):
            import json

            try:
                pks = json.loads(pks)
            except Exception:
                pks = [pks]

        planned.append(
            SilverRegistration(
                entity_key=ent["entity_key"],
                table_name=table,
                bronze_fqn=bronze_fqn,
                restricted=bool(ent.get("restricted", False)),
                target_schema=layer_schema_for_entity(ent, "silver"),
                primary_key_columns=list(pks),
            )
        )
    return planned


def registration_plan_summary(
    bronze: List[BronzeRegistration], silver: List[SilverRegistration]
) -> Dict[str, Any]:
    """JSON-serializable summary for CI logs."""
    return {
        "bronze_count": len(bronze),
        "silver_count": len(silver),
        "bronze_tables": [asdict(b) for b in bronze if b.load_kind != "skip"],
        "silver_tables": [asdict(s) for s in silver],
        "skipped": [b.entity_key for b in bronze if b.load_kind == "skip"],
        "connect_tables": [
            b.table_name for b in bronze if b.load_kind == "lakeflow_connect"
        ],
        "file_tables": [
            b.table_name for b in bronze if b.load_kind in ("api_extract", "file")
        ],
    }


def validate_registration_plan(
    bronze: List[BronzeRegistration], silver: List[SilverRegistration]
) -> List[str]:
    """Return list of validation errors (empty = ok)."""
    errors: List[str] = []
    bronze_names = [b.table_name for b in bronze if b.load_kind != "skip"]
    if len(bronze_names) != len(set(bronze_names)):
        errors.append(f"Duplicate bronze table names: {bronze_names}")

    silver_names = [s.table_name for s in silver]
    if len(silver_names) != len(set(silver_names)):
        errors.append(f"Duplicate silver table names: {silver_names}")

    for b in bronze:
        if b.load_kind == "skip":
            errors.append(f"{b.entity_key}: unknown/skipped load_pattern")
        if b.restricted and b.target_schema != "bronze_restricted":
            errors.append(
                f"{b.entity_key}: restricted entity must target bronze_restricted, got {b.target_schema}"
            )
        if not b.restricted and b.target_schema != "bronze":
            errors.append(
                f"{b.entity_key}: non-restricted must target bronze, got {b.target_schema}"
            )
        if b.load_kind == "lakeflow_connect":
            if not b.connect_source_table or not b.connect_source_table.endswith("__src"):
                errors.append(
                    f"{b.entity_key}: Connect path requires connect_source_table ending in __src"
                )
            if "bronze_connect" in (b.connect_source_table or ""):
                errors.append(f"{b.entity_key}: bronze_connect schema is forbidden")

    for s in silver:
        if s.restricted and s.target_schema != "silver_restricted":
            errors.append(
                f"{s.entity_key}: restricted silver must target silver_restricted"
            )
        if "bronze_connect" in s.bronze_fqn:
            errors.append(f"{s.entity_key}: bronze_connect in bronze_fqn forbidden")

    return errors
