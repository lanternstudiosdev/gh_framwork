"""Ensure src packages are importable when entry modules load outside the wheel."""

from __future__ import annotations

import sys
from pathlib import Path


def ensure_src_on_path() -> Path:
    here = Path(__file__).resolve()
    # lib/bootstrap.py → parent=lib → parent.parent=src
    candidates = [here.parent.parent, here.parent.parent / "src"]
    for src_root in candidates:
        if (src_root / "lib").is_dir() and (src_root / "pipelines").is_dir():
            path = str(src_root)
            if path not in sys.path:
                sys.path.insert(0, path)
            return src_root
    return here.parent.parent
