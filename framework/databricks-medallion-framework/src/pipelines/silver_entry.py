"""
Generic Silver entry — config-driven cleaning over Bronze tables (subject-agnostic).

One file, reused by every subject area (hr, sales, refdata, ...). The DAB pipeline
resource selects the subject via Spark conf ``subject_area_key``.

DABs runs two pipeline instances per subject:
  - schema silver,            restricted_scope=false  → non-restricted entities
  - schema silver_restricted, restricted_scope=true   → restricted entities

Bronze is read via **batch** ``spark.read.table(fqn)`` from the matching bronze schema:
  {catalog}.bronze.{src_key_entity}  or  {catalog}.bronze_restricted.{src_key_entity}

Batch read is intentional: bronze and silver are **separate pipeline resources**.
Silver materializes on each job/pipeline refresh from the current bronze snapshot
(not a continuous streaming join across pipelines). Ensure orchestration runs
bronze tasks before silver tasks.

Spark conf parameters (set by the DAB pipeline ``configuration`` block):
  subject_area_key  which subject's entities to register (required)
  source_key        optional default source key (entities normally carry their own)
  entity_keys       "*" or comma list to subset entities
  environment       dev | qat | prod
  restricted_scope  "true" targets silver_restricted; "false" targets silver
"""

from __future__ import annotations

import json
import dlt
from pyspark.sql import functions as F

# Ensure src/ is importable under DLT before the wheel is on sys.path.
import sys
from pathlib import Path

_SRC = Path(__file__).resolve().parents[1]
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from lib.bootstrap import ensure_src_on_path

ensure_src_on_path()

from lib.metadata import (
    get_control_catalog,
    get_entities_for_subject,
    get_quality_rules,
    get_column_policies,
    get_watermark_state,
    is_reprocess_requested,
)
from lib.security import apply_column_policies
from lib.expectations import apply_hybrid_expectations
from lib.volumes import (
    table_name_with_source_prefix,
    resolve_data_catalog,
    layer_schema_for_entity,
)
from pipelines.registration import plan_silver_registrations


def _conf(key: str, default: str = "") -> str:
    """Read a Spark conf value with a fallback (DLT sets these from the DAB)."""
    try:
        return spark.conf.get(key, default)
    except Exception:
        return default


CONTROL_CATALOG = get_control_catalog()
SUBJECT = _conf("subject_area_key", "")
DEFAULT_SOURCE_KEY = _conf("source_key", "")
ENTITY_KEYS_PARAM = _conf("entity_keys", "*")
ENVIRONMENT = _conf("environment", "dev")
RESTRICTED_SCOPE = _conf("restricted_scope", "false").lower() in ("1", "true", "yes")

print(
    f"Silver starting subject={SUBJECT!r} control={CONTROL_CATALOG} env={ENVIRONMENT} "
    f"entities={ENTITY_KEYS_PARAM} restricted_scope={RESTRICTED_SCOPE}"
)
if not SUBJECT:
    print(
        "WARNING: no subject_area_key configured for this pipeline; "
        "set configuration.subject_area_key in the DAB. Registering no tables."
    )


def _source_key(entity_cfg: dict) -> str:
    return entity_cfg.get("source_key") or DEFAULT_SOURCE_KEY or "unknown"


def _entities():
    if not SUBJECT:
        return []
    try:
        all_ents = get_entities_for_subject(SUBJECT)
    except Exception as e:
        print(f"WARNING: could not load entities ({e})")
        return []
    if ENTITY_KEYS_PARAM and ENTITY_KEYS_PARAM != "*":
        wanted = {k.strip() for k in ENTITY_KEYS_PARAM.split(",")}
        all_ents = [e for e in all_ents if e.get("entity_key") in wanted]
    return [
        e for e in all_ents if bool(e.get("restricted", False)) == RESTRICTED_SCOPE
    ]


def _pk_cols(entity_cfg: dict):
    pks = entity_cfg.get("primary_key_columns")
    if isinstance(pks, str):
        try:
            pks = json.loads(pks)
        except Exception:
            pks = [pks]
    return pks or []


def _bronze_fqn(entity_cfg: dict) -> str:
    """Fully-qualified bronze table in bronze or bronze_restricted."""
    load = entity_cfg.get("load_config") or {}
    if load.get("bronze_table_fqn"):
        return load["bronze_table_fqn"]
    catalog = resolve_data_catalog(entity_cfg, environment=ENVIRONMENT, subject_area_key=SUBJECT)
    schema = layer_schema_for_entity(entity_cfg, "bronze")
    source_key = _source_key(entity_cfg)
    entity_name = entity_cfg.get("entity_name") or entity_cfg["entity_key"]
    table = table_name_with_source_prefix(
        source_key, entity_name, explicit=entity_cfg.get("target_bronze_table")
    )
    return f"{catalog}.{schema}.{table}"


def _register_silver(entity_cfg: dict) -> None:
    entity_key = entity_cfg["entity_key"]
    source_key = _source_key(entity_cfg)
    entity_name = entity_cfg.get("entity_name") or entity_cfg["entity_key"]
    default_table = f"{source_key}_{entity_name}"
    silver_name = entity_cfg.get("target_silver_table") or default_table
    bronze_fqn = _bronze_fqn(entity_cfg)
    pks = _pk_cols(entity_cfg)

    quality_rules = []
    column_policies = []
    try:
        quality_rules = get_quality_rules(entity_key, "silver", include_defaults=True)
        column_policies = get_column_policies(entity_key)
    except Exception as e:
        print(f"Metadata optional load failed for {entity_key}: {e}")

    print(
        f"Registering Silver: {silver_name} from {bronze_fqn} "
        f"(entity={entity_key}, restricted={entity_cfg.get('restricted')})"
    )

    @dlt.table(
        name=silver_name,
        comment=f"Silver business-ready table for {entity_key}.",
        table_properties={
            "pipelines.quality": "silver",
            "delta.autoOptimize.optimizeWrite": "true",
            "entity_key": entity_key,
            "restricted": str(bool(entity_cfg.get("restricted", False))).lower(),
            "bronze_source": bronze_fqn,
        },
    )
    def _silver(bfqn=bronze_fqn, ek=entity_key, pk_list=pks, pols=column_policies, rules=quality_rules):
        try:
            bronze = spark.read.table(bfqn)
        except Exception:
            # Same-pipeline graph fallback (if bronze tables ever co-located)
            try:
                bronze = dlt.read(bfqn.split(".")[-1])
            except Exception as e:
                print(f"Cannot read bronze {bfqn}: {e}")
                bronze = spark.createDataFrame([], "dummy STRING").drop("dummy")

        reprocess = {"is_reprocess": False}
        watermark = {}
        try:
            reprocess = is_reprocess_requested(ek)
            watermark = get_watermark_state(ek)
        except Exception as e:
            print(f"WARNING: reprocess/watermark state unavailable for {ek}: {e}")

        if reprocess.get("is_reprocess"):
            source = bronze
        else:
            last_wm = watermark.get("current_watermark")
            if last_wm and "_source_extract_ts" in bronze.columns:
                source = bronze.filter(F.col("_source_extract_ts") > F.lit(last_wm))
            else:
                source = bronze

        if pk_list:
            existing = [c for c in pk_list if c in source.columns]
            if existing:
                source = source.dropDuplicates(existing)

        if pols:
            # SECURITY-CRITICAL: column policies apply encryption / masking /
            # hashing to sensitive (often restricted/PII) columns. If this fails
            # — e.g. the encryption-key secret is missing — we MUST fail the table
            # build rather than silently emit cleartext. Do NOT wrap this in a
            # broad except that swallows the error (see lib/security.py._get_secret,
            # which now raises instead of returning a fake key).
            source = apply_column_policies(source, pols)

        source = source.withColumn("_silver_processed_ts", F.current_timestamp())

        if rules:
            try:
                source = apply_hybrid_expectations(
                    source,
                    rules,
                    layer="silver",
                    entity_key=ek,
                    control_catalog=CONTROL_CATALOG,
                )
            except Exception as e:
                print(
                    f"WARNING: quality expectations failed and were skipped "
                    f"for {ek}: {e}"
                )

        for pk in pk_list:
            if pk in source.columns:
                dlt.expect_or_drop(f"{ek}_{pk}_not_null", f"{pk} IS NOT NULL")

        return source

    _silver.__name__ = f"silver_{silver_name}"
    globals()[f"silver_{silver_name}"] = _silver


_entity_list = _entities()
_plan = plan_silver_registrations(
    _entity_list,
    environment=ENVIRONMENT,
    entity_keys=ENTITY_KEYS_PARAM,
    restricted_scope=RESTRICTED_SCOPE,
    subject_area_key=SUBJECT or None,
)
print(f"Silver plan: {len(_plan)} table(s) subject={SUBJECT!r} restricted_scope={RESTRICTED_SCOPE}")
for _item in _plan:
    print(f"  plan {_item.entity_key} -> {_item.table_name} from {_item.bronze_fqn}")

_by_key = {e["entity_key"]: e for e in _entity_list}
for _item in _plan:
    _ent = _by_key.get(_item.entity_key)
    if _ent:
        _register_silver(_ent)

print("Silver registration complete.")
