"""
Config-driven API extract job (Workday RaaS and other REST sources).

For each requested entity (or all active entities for a source/subject):
  1. Load source connection + entity api config from platform control.
  2. Resolve secrets by *name* from Databricks secret scope (values in Key Vault).
  3. Call the endpoint with a variable param map.
  4. Write response files to UC Volume:
       /Volumes/{cat}/files/landing/raw/{source_key}/{entity_name}/

Does not use abfss:// paths. Lakeflow Connect entities are skipped
(load_pattern != api_extract / custom_extract / api_paged / file-related).

Parameters (widgets / job params / spark conf):
  control_catalog, environment, source_key, subject_area_key,
  entity_keys (comma-separated or *), dry_run (true/false)
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

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
SOURCE_KEY = _get_param("source_key", "workday")
SUBJECT_AREA_KEY = _get_param("subject_area_key", "hr")
ENTITY_KEYS = _get_param("entity_keys", "*")
DRY_RUN = _get_param("dry_run", "false").lower() in ("1", "true", "yes")

# Make control catalog visible to metadata helpers
spark.conf.set("control_catalog", CONTROL_CATALOG)
spark.conf.set("environment", ENVIRONMENT)

from lib.metadata import get_entity_config, get_source_config
from lib.volumes import landing_paths_from_entity_cfg
from lib.config_merge import deep_merge


API_LOAD_PATTERNS = {
    "api_extract",
    "custom_extract",
    "api_paged",
    "file_incremental",  # allow re-land files if extract still writes them
}


def _secret(scope: str, key: str) -> Optional[str]:
    if not scope or not key or not dbutils:
        return None
    try:
        return dbutils.secrets.get(scope=scope, key=key)
    except Exception as e:
        print(f"WARNING: could not read secret {scope}/{key}: {e}")
        return None


def _list_entity_keys() -> List[str]:
    if ENTITY_KEYS and ENTITY_KEYS != "*":
        return [k.strip() for k in ENTITY_KEYS.split(",") if k.strip()]

    from lib.sql_safe import qualified_table, sql_str

    full = qualified_table(CONTROL_CATALOG, "control", "source_entities")
    sql = f"""
        SELECT entity_key
        FROM {full}
        WHERE is_active = true
          AND source_key = {sql_str(SOURCE_KEY)}
          AND (subject_area_key = {sql_str(SUBJECT_AREA_KEY)} OR subject_area_key IS NULL)
        ORDER BY entity_key
    """
    return [r.entity_key for r in spark.sql(sql).collect()]


def _build_url(base_url: str, api_path_prefix: str, endpoint_path: str, tenant: str = "") -> str:
    path = (endpoint_path or "").replace("{tenant}", tenant or "")
    if path.startswith("http://") or path.startswith("https://"):
        return path
    root = (base_url or "").rstrip("/") + "/"
    prefix = (api_path_prefix or "").strip("/")
    rel = path.lstrip("/")
    if prefix:
        return urljoin(root, f"{prefix}/{rel}")
    return urljoin(root, rel)


def _auth_headers(connection: Dict[str, Any]) -> Dict[str, str]:
    scope = connection.get("secret_scope")
    secrets = connection.get("secrets") or {}
    auth_type = (connection.get("auth_type") or "oauth2_client_credentials").lower()
    headers = {"Accept": "application/json"}

    if auth_type.startswith("oauth2"):
        # Production: exchange client credentials / refresh token at token_url.
        # Here we support a pre-staged bearer token secret name if present.
        bearer = _secret(scope, secrets.get("access_token", "workday-access-token") or "workday-access-token")
        if bearer:
            headers["Authorization"] = f"Bearer {bearer}"
        else:
            client_id = _secret(scope, secrets.get("client_id", "workday-client-id"))
            client_secret = _secret(scope, secrets.get("client_secret", "workday-client-secret"))
            print(
                f"OAuth configured (scope={scope}); client_id present={bool(client_id)}, "
                f"client_secret present={bool(client_secret)}. "
                "Token exchange should be completed via token_url in production hardening."
            )
    elif auth_type == "basic":
        import base64

        user = _secret(scope, secrets.get("username", "workday-username")) or ""
        pwd = _secret(scope, secrets.get("password", "workday-password")) or ""
        token = base64.b64encode(f"{user}:{pwd}".encode()).decode()
        headers["Authorization"] = f"Basic {token}"

    return headers


def _http_get(url: str, headers: Dict[str, str], params: Dict[str, Any], timeout: int) -> bytes:
    try:
        import requests
    except ImportError:
        # Fallback without requests (Databricks clusters usually have it)
        from urllib.request import Request, urlopen
        from urllib.parse import urlencode

        q = urlencode({k: str(v) for k, v in (params or {}).items()})
        full = f"{url}?{q}" if q else url
        req = Request(full, headers=headers)
        with urlopen(req, timeout=timeout) as resp:
            return resp.read()

    resp = requests.get(url, headers=headers, params=params or {}, timeout=timeout)
    resp.raise_for_status()
    return resp.content


def _write_to_volume(raw_path: str, content: bytes, filename: str) -> str:
    """Write bytes to a UC Volume path using dbutils or Spark."""
    dest_dir = raw_path.rstrip("/")
    dest = f"{dest_dir}/{filename}"

    if DRY_RUN:
        print(f"DRY_RUN: would write {len(content)} bytes to {dest}")
        return dest

    if dbutils:
        # Ensure directory exists; dbutils.fs.put writes text — use binary via local temp + cp when needed
        try:
            dbutils.fs.mkdirs(dest_dir)
        except Exception:
            pass
        # put is text; for binary JSON/csv text is fine for Workday reports
        text = content.decode("utf-8", errors="replace")
        dbutils.fs.put(dest, text, overwrite=True)
    else:
        os.makedirs(dest_dir, exist_ok=True)
        with open(dest, "wb") as fh:
            fh.write(content)
    print(f"Wrote extract to {dest} ({len(content)} bytes)")
    return dest


def extract_entity(entity_key: str, source_cfg: Dict[str, Any]) -> Optional[str]:
    """Extract one entity via its REST/API load pattern and land the response file(s)
    into the entity's UC Volume ``raw/`` path. Connect-owned entities are skipped
    (they ingest via Lakeflow Connect). Returns the landed path, or None if skipped."""
    entity = get_entity_config(entity_key, environment=ENVIRONMENT)
    pattern = (entity.get("load_pattern") or "").lower()
    if pattern not in API_LOAD_PATTERNS and pattern != "api_extract":
        if pattern == "lakeflow_connect":
            print(f"SKIP {entity_key}: load_pattern=lakeflow_connect (not API extract)")
            return None
        print(f"SKIP {entity_key}: unsupported load_pattern={pattern}")
        return None

    connection = deep_merge(
        source_cfg.get("connection") or source_cfg.get("connection_json") or {},
        {},
    )
    if isinstance(connection, str):
        connection = json.loads(connection)

    extract_defaults = source_cfg.get("extract_defaults") or source_cfg.get("extract_defaults_json") or {}
    if isinstance(extract_defaults, str):
        extract_defaults = json.loads(extract_defaults)

    api = entity.get("api") or entity.get("api_config") or (entity.get("load_config") or {}).get("api") or {}
    if isinstance(api, str):
        api = json.loads(api)

    params = deep_merge(extract_defaults.get("common_params") or {}, api.get("params") or {})
    timeout = int(api.get("timeout_seconds") or extract_defaults.get("timeout_seconds") or 120)
    method = (api.get("http_method") or extract_defaults.get("http_method") or "GET").upper()

    tenant = params.pop("tenant", None) or connection.get("tenant") or ""
    url = _build_url(
        connection.get("base_url", ""),
        connection.get("api_path_prefix", ""),
        api.get("endpoint_path") or api.get("url") or "",
        tenant=str(tenant),
    )
    headers = _auth_headers(connection)
    paths = landing_paths_from_entity_cfg(entity)
    raw_path = paths["raw_path"]

    print(f"=== Extract {entity_key} ===")
    print(f"  URL      : {url}")
    print(f"  Method   : {method}")
    print(f"  Params   : {list(params.keys())}")
    print(f"  Landing  : {raw_path}")

    if method != "GET":
        raise NotImplementedError(f"HTTP method {method} not implemented in v1 extract job")

    if DRY_RUN:
        sample = json.dumps({"entity_key": entity_key, "dry_run": True, "params": params}).encode()
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        return _write_to_volume(raw_path, sample, f"{entity_key}_{ts}_dryrun.json")

    content = _http_get(url, headers, params, timeout)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run = uuid.uuid4().hex[:8]
    ext = "json"
    fmt = (api.get("response_format") or extract_defaults.get("response_format") or "json").lower()
    if fmt == "csv":
        ext = "csv"
    filename = f"{entity.get('entity_name', entity_key)}_{ts}_{run}.{ext}"
    return _write_to_volume(raw_path, content, filename)


def main():
    """Entry point: resolve the source config and target entities, then extract each one
    to its UC Volume landing path (or simulate when DRY_RUN)."""
    print(
        f"API Extract starting source={SOURCE_KEY} subject={SUBJECT_AREA_KEY} "
        f"env={ENVIRONMENT} control={CONTROL_CATALOG} dry_run={DRY_RUN}"
    )
    source_cfg = get_source_config(SOURCE_KEY, environment=ENVIRONMENT)
    keys = _list_entity_keys()
    print(f"Entities to process: {keys}")

    written = []
    errors = []
    for key in keys:
        try:
            path = extract_entity(key, source_cfg)
            if path:
                written.append({"entity_key": key, "path": path})
        except Exception as e:
            print(f"ERROR extracting {key}: {e}")
            errors.append({"entity_key": key, "error": str(e)})

    print(json.dumps({"written": written, "errors": errors}, indent=2))
    if errors and not written:
        raise RuntimeError(f"All extracts failed: {errors}")


if __name__ == "__main__":
    main()
