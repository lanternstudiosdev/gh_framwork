"""
Framework-managed archive: after successful Bronze load for an entity,
move files from landing raw/ to archive/ (same source_key/entity_name layout,
date-partitioned under archive).

Parameters:
  control_catalog, environment, entity_key (required) or entity_keys,
  source_path (optional override), dry_run
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import List, Optional

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

try:
    from pyspark.dbutils import DBUtils

    dbutils = DBUtils(spark)
except Exception:
    dbutils = None


try:
    from jobs._cli_params import parse_job_args, get_param as _resolve_param
except ImportError:
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from jobs._cli_params import parse_job_args, get_param as _resolve_param

_CLI = parse_job_args()


def _get_param(name: str, default: Optional[str] = None) -> str:
    return _resolve_param(
        name, default, cli=_CLI, spark=spark, dbutils=dbutils
    )


CONTROL_CATALOG = _get_param("control_catalog", "edw_platform_control_dev")
ENVIRONMENT = _get_param("environment", "dev")
ENTITY_KEY = _get_param("entity_key", "")
ENTITY_KEYS = _get_param("entity_keys", "")
DRY_RUN = _get_param("dry_run", "false").lower() in ("1", "true", "yes")

spark.conf.set("control_catalog", CONTROL_CATALOG)
spark.conf.set("environment", ENVIRONMENT)

from lib.metadata import get_entity_config
from lib.volumes import landing_paths_from_entity_cfg, resolve_archive_path


def _list_files(path: str) -> List[str]:
    if dbutils:
        try:
            return [f.path for f in dbutils.fs.ls(path) if not f.isDir()]
        except Exception as e:
            print(f"No files or path missing at {path}: {e}")
            return []
    if not os.path.isdir(path):
        return []
    out = []
    for root, _, files in os.walk(path):
        for name in files:
            out.append(os.path.join(root, name))
    return out


def archive_entity(entity_key: str) -> dict:
    """Move an entity's landed files from its UC Volume ``raw/`` path to a date-partitioned
    ``archive/`` path (run after Bronze succeeds). Honors DRY_RUN and works with either
    dbutils.fs or local filesystem. Returns a summary of what was (or would be) moved."""
    entity = get_entity_config(entity_key, environment=ENVIRONMENT)
    paths = landing_paths_from_entity_cfg(entity)
    raw = paths["raw_path"]
    vol = paths

    # Fresh archive path with today's partition
    archive = resolve_archive_path(
        volume_catalog=vol["volume_catalog"],
        source_key=entity.get("source_key", "unknown"),
        entity_name=entity.get("entity_name") or entity_key,
        volume_schema=vol["volume_schema"],
        volume_name=vol["volume_name"],
        archive_subpath=entity.get("archive_subpath")
        or (entity.get("load_config") or {}).get("archive_subpath"),
        archive_partitioning="yyyy/MM/dd",
        as_of=datetime.now(timezone.utc),
    )

    files = _list_files(raw)
    print(f"Archive {entity_key}: {len(files)} file(s) from {raw} -> {archive}")

    moved = []
    if DRY_RUN:
        return {"entity_key": entity_key, "raw": raw, "archive": archive, "files": files, "dry_run": True}

    if dbutils:
        try:
            dbutils.fs.mkdirs(archive)
        except Exception:
            pass
        for src in files:
            name = src.rstrip("/").split("/")[-1]
            dest = f"{archive.rstrip('/')}/{name}"
            dbutils.fs.mv(src, dest)
            moved.append(dest)
    else:
        os.makedirs(archive, exist_ok=True)
        import shutil

        for src in files:
            name = os.path.basename(src)
            dest = os.path.join(archive, name)
            shutil.move(src, dest)
            moved.append(dest)

    return {
        "entity_key": entity_key,
        "raw": raw,
        "archive": archive,
        "moved_count": len(moved),
        "moved": moved,
    }


def main():
    """Entry point: resolve the target entity key(s) and archive each one's landed files."""
    keys: List[str] = []
    if ENTITY_KEY:
        keys = [ENTITY_KEY]
    elif ENTITY_KEYS:
        keys = [k.strip() for k in ENTITY_KEYS.split(",") if k.strip()]
    else:
        raise ValueError("entity_key or entity_keys is required")

    results = [archive_entity(k) for k in keys]
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
