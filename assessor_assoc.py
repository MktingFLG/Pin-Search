# assessor_assoc.py
# Tries local data/_index/*.json first.
# If missing, falls back to GitHub raw (private repo) using GITHUB_TOKEN.

import os
import json
from functools import lru_cache
from pathlib import Path

import requests

# ---------- configuration ----------
# Option A: local folder (works if you copied assessor-passes-data/data into this repo)
DATA_ROOT = os.getenv("ASSESSOR_DATA_DIR", "")          # e.g. C:\Coding\assessor-passes-data\data
DATA_DIR  = Path(DATA_ROOT) if DATA_ROOT else Path("data")
INDEX_DIR = DATA_DIR / "_index"

PIN_TO_KEY_JSON   = INDEX_DIR / "pin_to_key.json"
KEY_TO_CHILD_JSON = INDEX_DIR / "key_to_children.json"

# Option B: GitHub fallback (private repo) — use GitHub “raw/refs/heads” URLs
GITHUB_TOKEN  = os.getenv("GITHUB_TOKEN", "").strip()
GITHUB_REPO   = os.getenv("ASSESSOR_DATA_REPO", "MktingFLG/assessor-passes-data")   # owner/name
GITHUB_BRANCH = os.getenv("ASSESSOR_DATA_BRANCH", "main")

# Allow full override (lets you paste an exact base if needed)
RAW_BASE = os.getenv(
    "ASSESSOR_INDEX_RAW_BASE",
    f"https://github.com/{GITHUB_REPO}/raw/refs/heads/{GITHUB_BRANCH}/data/_index"
)
AUTH_HEADERS = {"Authorization": f"Bearer {GITHUB_TOKEN}"} if GITHUB_TOKEN else {}

# -----------------------------------

@lru_cache(maxsize=1)
def _load_index() -> tuple[dict, dict]:
    """
    Returns (pin_to_key, key_to_children) dicts.
    Prefers local files. If not found, fetches from GitHub raw with token.
    """
    # 1) local files
    if PIN_TO_KEY_JSON.exists() and KEY_TO_CHILD_JSON.exists():
        try:
            p2k = json.loads(PIN_TO_KEY_JSON.read_text(encoding="utf-8"))
            k2c = json.loads(KEY_TO_CHILD_JSON.read_text(encoding="utf-8"))
            return p2k, k2c
        except Exception:
            pass  # fall through to GitHub

    # 2) GitHub raw (private ok if GITHUB_TOKEN set)
    try:
        p2k_url = f"{RAW_BASE}/pin_to_key.json"
        k2c_url = f"{RAW_BASE}/key_to_children.json"
        r1 = requests.get(p2k_url, headers=AUTH_HEADERS, timeout=30)
        r1.raise_for_status()
        r2 = requests.get(k2c_url, headers=AUTH_HEADERS, timeout=30)
        r2.raise_for_status()
        p2k = r1.json()
        k2c = r2.json()
        return p2k, k2c
    except Exception:
        # final fallback: empty
        return {}, {}

def _digits14(s: str) -> str:
    d = "".join(ch for ch in (s or "") if ch.isdigit())
    # keep exactly 14 when present; we do NOT left-pad here to avoid accidental mismatches
    return d if len(d) == 14 else d.zfill(14)[:14] if d else ""

def refresh_local_cache() -> None:
    """Clear cache (if you replace files on disk)."""
    _load_index.cache_clear()  # type: ignore[attr-defined]

def get_associated_pins(pin_undashed: str) -> list[str]:
    """
    Returns all PINs (undashed, 14 digits) for the key group of the given PIN.
    - If we know the key → returns [key, child1, child2, ...] (unique order not guaranteed)
    - If unknown but appears as a standalone child of itself → [pin]
    - Else [].
    """
    pin_to_key, key_to_children = _load_index()

    pin = _digits14(pin_undashed)
    if not pin:
        return []

    key = pin_to_key.get(pin)
    if not key:
        # standalone guard (if detail mapped it to itself)
        if pin in key_to_children.get(pin, []):
            return [pin]
        return []

    children = key_to_children.get(key, [])
    # ensure pin is included; dedupe while preserving order
    out = []
    seen = set()
    for x in [key] + children:
        x14 = _digits14(x)
        if x14 and x14 not in seen:
            seen.add(x14)
            out.append(x14)
    return out
