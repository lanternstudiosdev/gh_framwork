"""
Shared CLI + Spark conf + env parameter resolution for job entrypoints.

spark_python_task passes: --key=value arguments from the DAB job definition.
Also supports spark.conf and environment variables (uppercase).
"""

from __future__ import annotations

import os
from typing import Any, Dict, Optional


def parse_job_args(argv: Optional[list] = None) -> Dict[str, str]:
    """Parse --key=value and --key value into a flat dict (keys without --)."""
    import sys

    args = list(sys.argv[1:] if argv is None else argv)
    out: Dict[str, str] = {}
    i = 0
    while i < len(args):
        tok = args[i]
        if tok.startswith("--"):
            body = tok[2:]
            if "=" in body:
                k, v = body.split("=", 1)
                out[k.replace("-", "_")] = v
            elif i + 1 < len(args) and not args[i + 1].startswith("--"):
                out[body.replace("-", "_")] = args[i + 1]
                i += 1
            else:
                out[body.replace("-", "_")] = "true"
        i += 1
    return out


def get_param(
    name: str,
    default: Optional[str] = None,
    *,
    cli: Optional[Dict[str, str]] = None,
    spark: Any = None,
    dbutils: Any = None,
) -> str:
    """
    Resolution order: CLI args → spark.conf → dbutils.widgets → env → default.
    """
    key = name.replace("-", "_")
    if cli and key in cli and cli[key] not in (None, ""):
        return str(cli[key])

    if spark is not None:
        try:
            val = spark.conf.get(name)
            if val:
                return str(val)
        except Exception:
            pass
        try:
            val = spark.conf.get(key)
            if val:
                return str(val)
        except Exception:
            pass

    if dbutils is not None:
        try:
            val = dbutils.widgets.get(name)
            if val:
                return str(val)
        except Exception:
            pass

    env_val = os.getenv(name.upper()) or os.getenv(key.upper())
    if env_val:
        return env_val

    return default or ""
