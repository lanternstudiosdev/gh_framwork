"""
Generic Silver — metadata-driven, aligned with framework naming.

Parameters:
  source: source_key (e.g. workday)
  tables: comma list of entity_keys or *

Table names from config target_silver_table (e.g. workday_employees), not silver_{key}.
"""

import dlt
from pyspark.sql import functions as F

from lib.metadata import (
    get_control_catalog,
    get_entities_for_source,
    get_entity_config,
    get_column_policies,
    get_quality_rules,
    get_data_contract_columns,
)
from lib.volumes import table_name_with_source_prefix

SOURCE = spark.conf.get("source", "workday")
TABLES_CSV = spark.conf.get("tables", "*")
CONTROL_CATALOG = get_control_catalog()


def _get_requested_entities():
    if TABLES_CSV.strip() == "*":
        return get_entities_for_source(SOURCE)
    requested = [t.strip() for t in TABLES_CSV.split(",") if t.strip()]
    all_for_source = get_entities_for_source(SOURCE)
    return [e for e in requested if e in all_for_source]


def _get_contract_columns(entity_key: str):
    try:
        return get_data_contract_columns(entity_key)
    except Exception:
        if "employee" in entity_key:
            return [
                "employee_id",
                "first_name",
                "last_name",
                "department_id",
                "email",
                "hire_date",
                "salary",
                "status",
            ]
        if "department" in entity_key:
            return ["department_id", "department_name", "manager_id", "location"]
        return ["id", "name"]


entities = _get_requested_entities()
print(f"Generic Silver source={SOURCE} control={CONTROL_CATALOG} entities={entities}")

for entity_key in entities:
    cfg = get_entity_config(entity_key)
    source_key = cfg.get("source_key", SOURCE)
    entity_name = cfg.get("entity_name") or entity_key
    bronze_table = table_name_with_source_prefix(
        source_key, entity_name, explicit=cfg.get("target_bronze_table")
    )
    silver_table = table_name_with_source_prefix(
        source_key, entity_name, explicit=cfg.get("target_silver_table") or bronze_table
    )

    load_cfg = cfg.get("load_config") or {}
    evolution_enabled = load_cfg.get("silver_schema_evolution_enabled", True)
    allow_new = load_cfg.get("silver_allow_new_columns", True)
    preserve_dropped = load_cfg.get("silver_preserve_dropped_columns", True)
    enforce_contract = load_cfg.get("silver_enforce_downstream_contract", True)

    try:
        policies = get_column_policies(entity_key)
        rules = get_quality_rules(entity_key, layer="silver")
    except Exception:
        policies, rules = [], []

    contract_cols = _get_contract_columns(entity_key)

    @dlt.table(
        name=silver_table,
        comment=f"Silver {silver_table} for {entity_key} (evolution={evolution_enabled}).",
        table_properties={"quality": "silver", "entity_key": entity_key},
    )
    def _silver_table(
        bt=bronze_table,
        st=silver_table,
        ek=entity_key,
        ccols=contract_cols,
        evo=evolution_enabled,
        anew=allow_new,
        preserve=preserve_dropped,
        enforce=enforce_contract,
    ):
        bronze = dlt.read(bt)
        cols = list(bronze.columns)

        # Controlled evolution: ensure contract columns exist
        if enforce and ccols:
            for c in ccols:
                if c not in cols:
                    bronze = bronze.withColumn(c, F.lit(None))
                    cols.append(c)

        if not evo or not anew:
            keep = set(ccols or cols) | {
                c for c in cols if c.startswith("_")
            }
            select_cols = [c for c in bronze.columns if c in keep]
            if select_cols:
                bronze = bronze.select(*select_cols)

        return bronze.withColumn("_silver_processed_ts", F.current_timestamp())

    _silver_table.__name__ = f"silver_{silver_table}"
