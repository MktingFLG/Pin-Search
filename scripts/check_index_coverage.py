# scripts/check_index_coverage.py
# this read the header and teh deatil files that was downloaded from the subscription and saved in teh git repo
# built to check if all the pins from all the files were read to build the associated pins map
# Reads index & manifest JSONs and detail XLSX files directly from GitHub (raw).
# Prints coverage and consistency checks by township + overall.

import json
import re
import sys
from io import BytesIO
from urllib.parse import quote

import pandas as pd
import requests

import os

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "").strip()
AUTH_HEADERS = {"Authorization": f"Bearer {GITHUB_TOKEN}"} if GITHUB_TOKEN else {}


# ---- CONFIG: change if your repo/branch is different ----
REPO  = "MktingFLG/assessor-passes-data"   # owner/name
BRANCH = "main"
RAW_BASE = f"https://raw.githubusercontent.com/{REPO}/{BRANCH}"

INDEX_PIN_TO_KEY_URL = f"{RAW_BASE}/data/_index/pin_to_key.json"
INDEX_KEY_TO_CHILDREN_URL = f"{RAW_BASE}/data/_index/key_to_children.json"
MANIFEST_URL = f"{RAW_BASE}/data/manifest.json"

# ---------------------------------------------------------

def _fetch_json(url: str):
    r = requests.get(url, headers=AUTH_HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()

def _fetch_bytes(url: str) -> bytes:
    r = requests.get(url, headers=AUTH_HEADERS, timeout=60)
    r.raise_for_status()
    return r.content


def _norm_pin(s) -> str:
    """Digits only, left-pad to 14 if plausible."""
    if s is None:
        return ""
    d = re.sub(r"\D", "", str(s))
    if len(d) <= 14:
        d = d.zfill(14)
    return d

def pick_detail_path(files_obj: dict) -> str | None:
    """Choose the best detail file path from manifest files{}."""
    if not files_obj:
        return None
    # 1) Prefer explicit 'detail'
    detail = files_obj.get("detail")
    if detail:
        return detail
    # 2) Look through 'all' for NLTWND*.xlsx or anything with 'detail'
    for f in (files_obj.get("all") or []):
        base = f.rsplit("/", 1)[-1].lower()
        if base.endswith((".xlsx", ".xls", ".csv")) and ("nltwnd" in base or "detail" in base):
            return f
    return None

def read_detail_excel_from_github(rel_path: str) -> pd.DataFrame:
    """
    rel_path is like "Worth/2025-08-12/Detail.xlsx" relative to data/.
    Returns a DataFrame with DT_PIN and DT_KEY_PIN as strings (may be empty).
    """
    url = f"{RAW_BASE}/data/{quote(rel_path)}"
    blob = _fetch_bytes(url)
    df = pd.read_excel(BytesIO(blob), dtype=str, engine="openpyxl")
    # normalize columns (case-insensitive fallback)
    cols = {c.strip().upper(): c for c in df.columns}
    def colname(wanted):
        return cols.get(wanted) or next((c for U,c in cols.items() if U.startswith(wanted)), None)
    c_pin = colname("DT_PIN")
    c_key = colname("DT_KEY_PIN")
    if not c_pin:
        raise ValueError(f"DT_PIN column not found in {rel_path}")
    if not c_key:
        # Some files may name it slightly differently; try a loose match
        c_key = next((c for U,c in cols.items() if "DT_KEY" in U and "PIN" in U), None)
    if not c_key:
        # If truly missing, create empty col for a graceful check
        df["__DT_KEY_PIN__"] = ""
        c_key = "__DT_KEY_PIN__"
    # Keep only needed columns and normalize
    out = pd.DataFrame({
        "DT_PIN": df[c_pin].astype(str).map(_norm_pin),
        "DT_KEY_PIN": df[c_key].fillna("").astype(str).map(_norm_pin),
    })
    # Treat all-zeros (from blanks) as blank
    out.loc[out["DT_KEY_PIN"] == "00000000000000", "DT_KEY_PIN"] = ""
    return out

def main():
    print("Loading index JSONs from GitHub…")
    try:
        pin_to_key = _fetch_json(INDEX_PIN_TO_KEY_URL)
        key_to_children = _fetch_json(INDEX_KEY_TO_CHILDREN_URL)
        print(f"  pin_to_key: {len(pin_to_key):,} entries")
        print(f"  key_to_children: {len(key_to_children):,} keys")
    except Exception as e:
        print("ERROR: could not load index JSONs:", e)
        sys.exit(1)

    print("Loading manifest.json…")
    try:
        manifest = _fetch_json(MANIFEST_URL)
        towns = manifest.get("towns", {})
        print(f"  manifest towns: {len(towns)}")
    except Exception as e:
        print("ERROR: could not load manifest.json:", e)
        sys.exit(1)

    overall_rows = 0
    overall_pins = 0
    overall_missing = 0
    overall_mismatch = 0
    checked_townships = 0

    print("\n=== Per-township coverage check ===")
    for code, rec in sorted(towns.items(), key=lambda kv: int(kv[0])):
        township = rec.get("township", f"code_{code}")
        files = (rec.get("files") or {})
        detail_rel = pick_detail_path(files)
        if not detail_rel:
            print(f" - {township:20} (#{code}): no detail file in manifest -> SKIP")
            continue

        # Fetch & parse the detail workbook directly from GitHub
        try:
            df = read_detail_excel_from_github(detail_rel)
        except Exception as e:
            print(f" - {township:20} (#{code}): ERROR reading detail '{detail_rel}': {e}")
            continue

        checked_townships += 1
        n_rows = len(df)
        pins = df["DT_PIN"].dropna().map(_norm_pin).tolist()
        pins_set = {p for p in pins if p}
        n_pins = len(pins_set)

        # Coverage: pins missing from pin_to_key
        missing = [p for p in pins_set if p not in pin_to_key]
        # Consistency: where a key pin exists, pin_to_key[pin] must equal that key
        mask_has_key = df["DT_KEY_PIN"].astype(str).str.len() > 0
        pairs = df.loc[mask_has_key, ["DT_PIN", "DT_KEY_PIN"]].drop_duplicates()
        mismatches = []
        for _, row in pairs.iterrows():
            p = row["DT_PIN"]
            k = row["DT_KEY_PIN"]
            mapped = pin_to_key.get(p, "")
            if mapped != k:
                mismatches.append((p, k, mapped))

        overall_rows += n_rows
        overall_pins += n_pins
        overall_missing += len(missing)
        overall_mismatch += len(mismatches)

        print(
            f" - {township:20} (#{code}): "
            f"rows={n_rows:,}, unique_pins={n_pins:,}, "
            f"missing_in_index={len(missing)}, key_mismatches={len(mismatches)}"
        )

        if mismatches:
            print(f"   ⚠ First mismatch example in {township}:")
            for i, (p, k, mapped) in enumerate(mismatches[:5]):
                print(f"     PIN {p} → detail has KEY {k}, index says {mapped}")

    print("\n=== Overall ===")
    print(f"  townships checked:      {checked_townships}")
    print(f"  total rows (summed):    {overall_rows:,}")
    print(f"  total unique pins:      {overall_pins:,}")
    print(f"  pins missing in index:  {overall_missing:,}")
    print(f"  key mapping mismatches: {overall_mismatch:,}")

    if overall_missing == 0 and overall_mismatch == 0:
        print("\n✅ Index looks consistent with all available detail files in manifest.")
    else:
        print("\n⚠️  Some gaps detected. See counts above; we can print the first few examples if you want.")

if __name__ == "__main__":
    main()
