"""
Deep-merge helpers for env overlays and free-form source/entity params.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional


def deep_merge(base: Dict[str, Any], overlay: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively merge overlay onto base (overlay wins). Lists are replaced, not concatenated."""
    result = deepcopy(base) if base else {}
    for key, value in (overlay or {}).items():
        if (
            key in result
            and isinstance(result[key], dict)
            and isinstance(value, dict)
        ):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = deepcopy(value)
    return result


def index_entities_by_key(entities: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Return a dict of entities keyed by ``entity_key`` (entities without a key are skipped)."""
    return {e["entity_key"]: e for e in entities if e.get("entity_key")}


def merge_entity_overlay(
    base_entities: List[Dict[str, Any]],
    overlay_entities: List[Dict[str, Any]],
    subject_defaults: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    Merge per-entity overlays by entity_key and apply subject_defaults
    (data_catalog, control_catalog, landing_volume). Subject defaults only fill
    gaps: any value defined on the entity itself takes precedence.
    """
    by_key = index_entities_by_key(base_entities)
    for ov in overlay_entities or []:
        key = ov.get("entity_key")
        if not key:
            continue
        if key in by_key:
            by_key[key] = deep_merge(by_key[key], ov)
        else:
            by_key[key] = deepcopy(ov)

    merged = list(by_key.values())
    if not subject_defaults:
        return merged

    for ent in merged:
        load = ent.setdefault("load_config", {})
        # Subject defaults fill gaps only; a value defined on the entity wins.
        if subject_defaults.get("data_catalog") and not ent.get("data_catalog"):
            ent["data_catalog"] = subject_defaults["data_catalog"]
        if subject_defaults.get("control_catalog") and not ent.get("control_catalog"):
            ent["control_catalog"] = subject_defaults["control_catalog"]
        if "landing_volume" in subject_defaults:
            # Start from the subject default and let the entity's own keys override.
            load["landing_volume"] = deep_merge(
                subject_defaults["landing_volume"],
                load.get("landing_volume") or {},
            )
    return merged
