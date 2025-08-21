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

# --- GitHub fallback (private repo raw via API) ---
API_BASE = f"https://api.github.com/repos/{GITHUB_REPO}/contents/data/_index"
API_HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN}"
} if GITHUB_TOKEN else {}
API_RAW_HEADERS = {**API_HEADERS, "Accept": "application/vnd.github.raw"}

def _fetch_json_from_github(filename: str) -> dict:
    # example: .../contents/data/_index/pin_to_key.json?ref=main
    url = f"{API_BASE}/{filename}?ref={GITHUB_BRANCH}"
    r = requests.get(url, headers=API_RAW_HEADERS, timeout=30)
    r.raise_for_status()
    # with Accept: raw we receive the file content directly (not base64)
    return r.json()

@lru_cache(maxsize=1)
def _load_index() -> tuple[dict, dict]:
    # 1) local files first
    if PIN_TO_KEY_JSON.exists() and KEY_TO_CHILD_JSON.exists():
        try:
            p2k = json.loads(PIN_TO_KEY_JSON.read_text(encoding="utf-8"))
            k2c = json.loads(KEY_TO_CHILD_JSON.read_text(encoding="utf-8"))
            return p2k, k2c
        except Exception:
            pass  # fall through

    # 2) GitHub API (private ok with token)
    try:
        if not GITHUB_TOKEN:
            raise RuntimeError("GITHUB_TOKEN missing for private repo")
        p2k = _fetch_json_from_github("pin_to_key.json")
        k2c = _fetch_json_from_github("key_to_children.json")
        return p2k, k2c
    except Exception:
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
