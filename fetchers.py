# fetchers.py
"""
Thin adapters for each data source.
Replace stubs with your actual BOR, CV, Sales, Assessor scraping, and Permits functions.
"""


import time
from datetime import datetime
from pathlib import Path
import requests
from bs4 import BeautifulSoup
from utils import normalize_pin, undashed_pin

import os
import urllib.parse
import re

# --- PTAB scraper imports ---
from urllib.parse import urljoin

# typing + requests retry bits
from typing import Any, Dict, List, Optional, Tuple
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


from functools import lru_cache
import pandas as _pd
import re as _re

# ---------------- Cook County Socrata helper (datacatalog.cookcountyil.gov) ----------------
import os, time, random, requests

COOK_SOCRATA_BASE = "https://datacatalog.cookcountyil.gov/resource"
COOK_APP_TOKEN = os.getenv("COOK_SODA_APP_TOKEN") or os.getenv("ILLINOIS_APP_TOKEN") or ""

def _socrata_get_cook(
    dataset_id: str,
    params: dict,
    *,
    fetch_all: bool = True,
    chunk_size: int = 50000,   # safe page size for big pulls
    max_retries: int = 5,
    base_backoff: float = 0.8, # seconds
    timeout: int = 30,
) -> list[dict]:
    """
    Robust Socrata GET for Cook County:
      - No token required (adds X-App-Token automatically if env set)
      - Retries 429/5xx/timeouts with exponential backoff + jitter
      - Optional pagination when fetch_all=True
    """
    base_url = f"{COOK_SOCRATA_BASE}/{dataset_id}.json"

    headers = {"Accept": "application/json", "User-Agent": "PIN-Tool/1.0"}
    if COOK_APP_TOKEN:
        headers["X-App-Token"] = COOK_APP_TOKEN

    effective_params = dict(params or {})
    if fetch_all:
        effective_params.setdefault("$limit", chunk_size)
    else:
        effective_params.setdefault("$limit", 5000)

    results: list[dict] = []
    session = requests.Session()

    def _attempt_once(p: dict) -> list[dict]:
        r = session.get(base_url, params=p, headers=headers, timeout=timeout)
        code = r.status_code

        if code == 200:
            try:
                data = r.json()
            except Exception:
                raise requests.HTTPError(f"Invalid JSON from Socrata ({dataset_id})", response=r)
            return data if isinstance(data, list) else []

        if code == 429:
            raise requests.HTTPError("429 Too Many Requests", response=r)

        if 500 <= code < 600:
            raise requests.HTTPError(f"{code} Server Error", response=r)

        # For other client errors (bad where/params), surface details
        if 400 <= code < 500:
            raise requests.HTTPError(f"{code} Client Error: {r.text[:800]}", response=r)

        r.raise_for_status()

    # Paging
    offset = int(effective_params.get("$offset", 0))
    page_limit = int(effective_params["$limit"])

    while True:
        attempt = 0
        while True:
            try:
                page = _attempt_once({**effective_params, "$offset": offset})
                break
            except requests.HTTPError as e:
                sc = getattr(e.response, "status_code", None)
                if sc == 429 or (sc and 500 <= sc < 600):
                    attempt += 1
                    if attempt >= max_retries:
                        # signal throttling so caller can return _status:"throttled"
                        if sc == 429:
                            raise RuntimeError("SOC_DATA_THROTTLED") from e
                        raise
                    sleep_s = (base_backoff * (2 ** (attempt - 1))) + random.uniform(0, 0.5)
                    time.sleep(sleep_s)
                    continue
                raise
            except (requests.Timeout, requests.ConnectionError):
                attempt += 1
                if attempt >= max_retries:
                    raise
                sleep_s = (base_backoff * (2 ** (attempt - 1))) + random.uniform(0, 0.4)
                time.sleep(sleep_s)
                continue

        results.extend(page)

        if not fetch_all:
            return results
        if len(page) < page_limit:
            return results

        offset += page_limit


DEFAULT_UA = "PIN-Tool/1.0 (+https://example.com)"


_DATA_DIR = Path("data")

# --- PTAB config ---
PTAB_BASE = "https://www.ptab.illinois.gov/asi"
PTAB_TIMEOUT = 25
PTAB_WAIT_SECONDS = 0.8  # gentle delay between requests to be polite

def _retrying_session(
    total: int = 5,
    backoff_factor: float = 0.4,
    status_forcelist: Tuple[int, ...] = (429, 500, 502, 503, 504),
    allowed_methods: Tuple[str, ...] = ("HEAD", "GET", "OPTIONS"),
) -> requests.Session:
    """
    Requests session with retry/backoff for idempotent GETs.
    """
    sess = requests.Session()
    retry = Retry(
        total=total,
        read=total,
        connect=total,
        status=total,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=set(allowed_methods),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)
    sess.headers.update({"User-Agent": DEFAULT_UA})
    return sess


def _coerce_int(v) -> Optional[int]:
    try:
        return int(str(v).strip())
    except Exception:
        return None


def _ok(meta: Dict[str, Any], rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Standard success envelope (matches the rest of your fetchers).
    """
    return {
        "_status": "ok",
        "_meta": meta,
        "rows": rows,
        "count": len(rows),
    }


def _err(meta: Dict[str, Any], message: str) -> Dict[str, Any]:
    """
    Standard error envelope.
    """
    return {
        "_status": "error",
        "_meta": {**meta, "error": str(message)},
        "rows": [],
        "count": 0,
    }



from functools import lru_cache


@lru_cache(maxsize=256)
def _rod_assoc_for(pin14: str) -> list[str]:
    try:
        bundle = fetch_recorder_bundle(pin14)  # <-- your function below
        dashed = bundle.get("normalized", {}).get("associated_pins_dashed") or []
        und    = bundle.get("normalized", {}).get("associated_pins_undashed") or []
        # normalize to dashed for display, but keep 14-digit under the hood
        out = []
        seen = set()
        for u in und:
            u14 = undashed_pin(u)
            if u14 and u14 not in seen:
                seen.add(u14); out.append(u14)
        for d in dashed:
            u14 = undashed_pin(d)
            if u14 and u14 not in seen:
                seen.add(u14); out.append(u14)
        return out
    except Exception:
        return []


def _read_any_table(path: Path) -> _pd.DataFrame:
    if path.suffix.lower() == ".csv":
        return _pd.read_csv(path, dtype=str, keep_default_na=False)
    # Excel
    return _pd.read_excel(path, dtype=str, engine="openpyxl")

def _pick_col(df: _pd.DataFrame, options: list[str]) -> str | None:
    cols = {c.strip().upper(): c for c in df.columns}
    for name in options:
        if name.upper() in cols:
            return cols[name.upper()]
    return None

def get_assessor_associated_pins(pin: str) -> list[str]:
    p = undashed_pin(pin)

    # (A) Remote GitHub index
    key_to_children_remote, child_to_key_remote = _remote_assoc_index()

    group: list[str] = []
    if p in key_to_children_remote:
        group = [p] + key_to_children_remote[p]
    elif p in child_to_key_remote:
        k = child_to_key_remote[p]
        group = [k] + key_to_children_remote.get(k, [])
    else:
        group = [p]  # not in index

    # (B) Fallback to Recorder of Deeds only if we still have just the input
    if len(set(group)) <= 1:
        rod14s = _rod_assoc_for(p)
        if rod14s:
            merged, seen = [], set()
            for x in rod14s:
                x14 = undashed_pin(x)
                if x14 and x14 not in seen:
                    seen.add(x14); merged.append(x14)
            if p not in seen:
                merged.insert(0, p)
            group = merged

    # (C) Dedup, ensure 14‑digit
    seen, out = set(), []
    for x in group:
        x14 = undashed_pin(x)
        if x14 and x14 not in seen:
            seen.add(x14); out.append(x14)
    return out


# ==============================================================================

def _ptab_session() -> requests.Session:
    s = _retrying_session()
    s.headers.update({"User-Agent": "Mozilla/5.0"})
    return s

def _clean_money_to_float(val: str) -> float:
    if val is None:
        return 0.0
    s = str(val)
    s = re.sub(r"[^\d.\-]", "", s)  # keep digits, dot, minus
    if s == "" or s == "." or s == "-":
        return 0.0
    try:
        return float(s)
    except Exception:
        return 0.0

def _parse_row_pairs(tr) -> list[tuple[str, str]]:
    cells = tr.find_all(["th", "td"])
    out = []
    for i in range(0, len(cells) - 1, 2):
        L = cells[i].get_text(strip=True).replace(u' ', ' ').rstrip(":")
        V = cells[i + 1].get_text(strip=True)
        out.append((L, V))
    return out


_S3_ENABLED = os.getenv("S3_ENABLED", "").lower() in {"1","true","yes","on"}
_S3_BUCKET = os.getenv("S3_BUCKET", "")
_S3_CLIENT = None

def _maybe_init_s3():
    global _S3_CLIENT
    if _S3_CLIENT is None:
        try:
            import boto3  # type: ignore[import-not-found]
            _S3_CLIENT = boto3.client("s3")
        except Exception:
            _S3_CLIENT = False  # mark as not available


def save_raw_text(relpath: str, text: str) -> dict:
    """
    Save a raw HTML/text blob either to S3 (if enabled) or to local disk.
    relpath: like "assessor_profile/<pin>_<jur>_<taxyr>.html"
    Returns an object describing where it went.
    """
    data = text.encode("utf-8")

    if _S3_ENABLED and _S3_BUCKET:
        _maybe_init_s3()
        if _S3_CLIENT:
            key = f"raw_cache/{relpath}"
            _S3_CLIENT.put_object(Bucket=_S3_BUCKET, Key=key, Body=data, ContentType="text/html; charset=utf-8")
            # If your bucket is public, the file would be viewable via https://<bucket>.s3.<region>.amazonaws.com/<key>
            return {"storage": "s3", "bucket": _S3_BUCKET, "key": key}
        # fall through to disk if boto3 not available

    # local disk (works in dev; ephemeral in Render)
    p = Path("raw_cache") / relpath
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(data)
    return {"storage": "disk", "path": str(p)}
# -----------------------------------------------------------------------------




PROFILE_BASE = "https://assessorpropertydetails.cookcountyil.gov/datalets/datalet.aspx"
_RAW_DIR = Path("raw_cache/assessor_profile")
_RAW_DIR.mkdir(parents=True, exist_ok=True)
VALUES_BASE = "https://assessorpropertydetails.cookcountyil.gov/datalets/datalet.aspx"

def _now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


def _save_raw_html(pin14_undashed: str, jur: str, taxyr: str, html: str) -> str:
    p = _RAW_DIR / f"{pin14_undashed}_{jur}_{taxyr}.html"
    p.write_text(html, encoding="utf-8")
    return str(p)

def _harvest_tables(block):
    """
    Returns a list of tables found as dicts:
      [{"title": "<id or title>", "type": "grid|kv", "rows": [...]}]
    """
    results = []
    if not block: return results
    tables = block.find_all("table")
    for tbl in tables:
        trs = tbl.find_all("tr")
        if not trs: continue
        # try header/grid first
        headers = [h.get_text(" ", strip=True) for h in trs[0].find_all(["th","td"])]
        has_header = len(headers) >= 2 and any(headers)
        if has_header and len(trs) >= 2:
            rows = []
            for tr in trs[1:]:
                tds = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
                if any(tds):
                    rows.append({headers[i] if i < len(headers) else f"col_{i+1}": (tds[i] if i < len(tds) else "") for i in range(len(headers))})
            if rows:
                results.append({"title": tbl.get("id") or "Table", "type": "grid", "rows": rows})
                continue
        # kv fallback
        kv = {}
        for tr in trs:
            tds = tr.find_all("td")
            if len(tds) >= 2:
                k = tds[0].get_text(" ", strip=True).rstrip(":")
                v = tds[1].get_text(" ", strip=True).replace("\xa0"," ").strip()
                if k: kv[k] = v
        if kv:
            results.append({"title": tbl.get("id") or "Table", "type": "kv", "rows": [kv]})
    return results


def fetch_assessor_profile(pin: str, jur: str = "016", taxyr: str = "2025", force: bool = False) -> dict:
    """
    Fetch EVERYTHING from Assessor 'Profile' page for a PIN, store full HTML, and return normalized fields.
    Returns:
      {
        "normalized": { ...flat dict for UI... },
        "raw": {"html_path": "...", "html_size_bytes": N},
        "_status": "ok" | "error",
        "_meta": {...}
      }
    """
    t0 = time.time()
    pin14_und = undashed_pin(pin)

    url = (
        f"{PROFILE_BASE}?mode=profileall_cc&UseSearch=no"
        f"&pin={pin14_und}&jur={jur}&taxyr={taxyr}&LMparent=896"
    )
    meta = {
        "source": "ASSR_PROFILE",
        "url": url,
        "pin": pin14_und,
        "jur": jur,
        "taxyr": taxyr,
        "fetched_at": _now_iso(),
    }

    try:
        r = requests.get(url, headers={"User-Agent": DEFAULT_UA}, timeout=25)
        r.raise_for_status()
        html = r.text
        raw_rec = save_raw_text(f"assessor_profile/{pin14_und}_{jur}_{taxyr}.html", html)

        soup = BeautifulSoup(html, "html.parser")

        # Header block (Parcel #, Neighborhood, Address line, Roll, Tax Year line)
        header_cell = soup.find(id="datalet_header_row")
        header = {}
        if header_cell:
            inner = BeautifulSoup(str(header_cell), "html.parser")
            # Heuristic to grab address cell
            addr_td = inner.find("td", class_="DataletHeaderBottom")
            if addr_td:
                txt = addr_td.get_text(" ", strip=True)
                if txt and "Parcel #:" not in txt and "ROLL:" not in txt:
                    header["address_line"] = txt
            for tr in inner.find_all("tr"):
                tds = [td.get_text(strip=True) for td in tr.find_all("td")]
                for td in tds:
                    if "Parcel #:" in td:
                        header["parcel_number"] = td.split("Parcel #:")[-1].strip()
                    elif "Neighborhood:" in td:
                        header["neighborhood"] = td.split("Neighborhood:")[-1].strip()
                    elif "ROLL:" in td:
                        header["roll"] = td.split("ROLL:")[-1].strip()
                    elif "Tax Year:" in td:
                        header["tax_year_line"] = td.strip()
                


        # "PIN Info" table → key/value dict
        pin_info = {}
        pin_table = soup.find("table", id="PIN Info")
        if pin_table:
            for tr in pin_table.find_all("tr"):
                tds = tr.find_all("td")
                if len(tds) >= 2:
                    key = tds[0].get_text(strip=True)
                    val = tds[1].get_text(" ", strip=True)
                    if key:
                        pin_info[key] = val

        # Flatten for your UI (so docs page renders clean key/values)
        normalized = {
            # header
            "Parcel #": header.get("parcel_number"),
            "Address (header)": header.get("address_line"),
            "Neighborhood (header)": header.get("neighborhood"),
            "Roll": header.get("roll"),
            "Tax Year (line)": header.get("tax_year_line"),
            # pin info (keep all rows, but safe keys)
            **{f"PIN Info • {k}": v for k, v in pin_info.items()},
        }

        return {
            "normalized": normalized,
            "raw": {**raw_rec, "html_size_bytes": len(html)},
            "_status": "ok",
            "_meta": {**meta, "duration_ms": int((time.time() - t0) * 1000)},
        }

    except Exception as e:
        return {
            "normalized": {},
            "raw": {},
            "_status": "error",
            "_meta": {**meta, "error": str(e)},
        }

# --- Assessor Taxpayer Data (MAILDETAIL) ---
def fetch_assessor_maildetail(pin: str, jur: str = "016", taxyr: str = "2025", force: bool = False) -> dict:
    """
    Pull EVERYTHING from 'Taxpayer Data' (mode=maildetail).
    Saves full HTML and returns a flat normalized dict for UI.
    """
    t0 = time.time()
    pin14_und = undashed_pin(pin)

    url = (
        f"{PROFILE_BASE}?mode=maildetail&UseSearch=no"
        f"&pin={pin14_und}&jur={jur}&taxyr={taxyr}&LMparent=896"
    )
    meta = {
        "source": "ASSR_MAILDETAIL",
        "url": url,
        "pin": pin14_und,
        "jur": jur,
        "taxyr": taxyr,
        "fetched_at": _now_iso(),
    }

    try:
        r = requests.get(url, headers={"User-Agent": DEFAULT_UA}, timeout=25)
        r.raise_for_status()
        html = r.text
        html_path = _save_raw_html(pin14_und, jur, taxyr, html)

        soup = BeautifulSoup(html, "html.parser")

        # Header cell not strictly needed here, but we could parse if you want.
        # The meat is the single MAILDETAIL table just under datalet_div_0.
        normalized = {}

        # Find the datalet block for MAILDETAIL (defensive: search by name or by the first table under the div)
        mail_div = soup.find("div", id="datalet_div_0")
        if mail_div:
            # The second table in this div (after the small title table) holds the key/value rows
            tables = mail_div.find_all("table")
            data_table = tables[1] if len(tables) > 1 else (tables[0] if tables else None)
        else:
            data_table = None

        if data_table:
            for tr in data_table.find_all("tr"):
                tds = tr.find_all("td")
                if len(tds) >= 2:
                    key = tds[0].get_text(strip=True).rstrip(":")
                    val = tds[1].get_text(" ", strip=True)
                    if key:
                        normalized[f"{key}"] = val

        # Optional: also expose a few convenience aliases the UI will likely want
        aliases = {
            "Taxpayer Name": normalized.get("Taxpayer Name"),
            "Taxpayer Name 2": normalized.get("Taxpayer Name 2"),
            "Mailing Address (line1)": normalized.get("Address"),
            "Mailing City": normalized.get("City"),
            "Mailing State": normalized.get("State"),
            "Mailing Zip": normalized.get("Zip"),
        }
        # Merge but keep all raw keys too (don’t overwrite existing)
        for k, v in aliases.items():
            if k not in normalized:
                normalized[k] = v

        return {
            "normalized": normalized,
            "raw": {"html_path": html_path, "html_size_bytes": len(html)},
            "_status": "ok",
            "_meta": {**meta, "duration_ms": int((time.time() - t0) * 1000)},
        }

    except Exception as e:
        return {
            "normalized": {},
            "raw": {},
            "_status": "error",
            "_meta": {**meta, "error": str(e)},
        }

# --- Assessor Exemption Status (EXADMN2) ---
def fetch_assessor_exemptions(pin: str, jur: str = "016", taxyr: str = "2025", force: bool = False) -> dict:
    """
    Pull Exemption Status (mode=exadmn2). Handles both 'No Data' and real tables.
    Saves full HTML and returns:
      {
        "normalized": {
            "has_data": bool,
            "rows": [ { ...row fields... }, ... ],
        },
        "raw": {...},
        "_status": "ok" | "error",
        "_meta": {...}
      }
    """
    t0 = time.time()
    pin14_und = undashed_pin(pin)

    url = (
        f"{PROFILE_BASE}?mode=exadmn2&UseSearch=no"
        f"&pin={pin14_und}&jur={jur}&taxyr={taxyr}&LMparent=896"
    )
    meta = {
        "source": "ASSR_EXEMPT",
        "url": url,
        "pin": pin14_und,
        "jur": jur,
        "taxyr": taxyr,
        "fetched_at": _now_iso(),
    }

    try:
        r = requests.get(url, headers={"User-Agent": DEFAULT_UA}, timeout=25)
        r.raise_for_status()
        html = r.text
        html_path = _save_raw_html(pin14_und, jur, taxyr, html)

        soup = BeautifulSoup(html, "html.parser")

        # Quick 'No Data' check (exact text appears inside a small center table)
        page_text = soup.get_text(" ", strip=True).upper()
        if "-- NO DATA --" in page_text:
            normalized = {"has_data": False, "rows": []}
            return {
                "normalized": normalized,
                "raw": {"html_path": html_path, "html_size_bytes": len(html)},
                "_status": "ok",
                "_meta": {**meta, "duration_ms": int((time.time() - t0) * 1000)},
            }

        # Otherwise, try to harvest any data tables under the datalet area.
        # Different parcels can render this differently, so be flexible:
        # Strategy: find the main datalet container and collect any <table> that looks like data.
        rows_out = []
        main = soup.find("div", id=lambda v: v and v.startswith("datalet_div_")) or soup
        # collect candidate tables
        tables = main.find_all("table")
        for tbl in tables:
            # Heuristic: parse any table that has rows with 2+ tds (label/value) OR header-like ths
            trs = tbl.find_all("tr")
            # Skip the tiny header/title tables (usually 0 or 1 td)
            if not trs or all(len(tr.find_all(["td", "th"])) <= 1 for tr in trs):
                continue

            # Case A: Key/Value style table (2 tds per row)
            kv_pairs = {}
            kv_count = 0
            for tr in trs:
                tds = tr.find_all("td")
                if len(tds) >= 2:
                    k = tds[0].get_text(strip=True).rstrip(":")
                    v = tds[1].get_text(" ", strip=True)
                    if k:
                        kv_pairs[k] = v
                        kv_count += 1

            if kv_count >= 2:
                rows_out.append(kv_pairs)
                continue

            # Case B: Grid table with header row (ths) and data rows (tds)
            ths = [th.get_text(strip=True) for th in trs[0].find_all("th")] if trs else []
            if ths:
                # build maps for subsequent rows
                for tr in trs[1:]:
                    tds = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
                    if not tds:
                        continue
                    row = {}
                    for i, h in enumerate(ths):
                        row[h or f"col_{i+1}"] = tds[i] if i < len(tds) else ""
                    rows_out.append(row)

        normalized = {"has_data": bool(rows_out), "rows": rows_out}

        return {
            "normalized": normalized,
            "raw": {"html_path": html_path, "html_size_bytes": len(html)},
            "_status": "ok",
            "_meta": {**meta, "duration_ms": int((time.time() - t0) * 1000)},
        }

    except Exception as e:
        return {
            "normalized": {"has_data": False, "rows": []},
            "raw": {},
            "_status": "error",
            "_meta": {**meta, "error": str(e)},
        }



def _scrape_value_summary_raw(html: str):
    soup = BeautifulSoup(html, "html.parser")
    block = soup.find("div", attrs={"name": "VALUE_SUMMARY_CC"}) or soup.find(id="datalet_div_0") or soup

    # Prefer the table with id "Values Summary" (yes, space in id…), else fall back to
    # the 2nd table in the block (first is the small title bar).
    table = block.find("table", id="Values Summary")
    if not table:
        tables = block.find_all("table")
        table = tables[1] if len(tables) > 1 else (tables[0] if tables else None)
    if not table:
        return []

    rows = []
    trs = table.find_all("tr")
    if not trs:
        return rows

    # Skip header
    for tr in trs[1:]:
        tds = [td.get_text(strip=True) for td in tr.find_all("td")]
        if len(tds) >= 8:
            rows.append({
                "Year": tds[0],
                "Process Name": tds[1],
                "Total MV": tds[2],
                "Land AV": tds[3],
                "Bldg AV": tds[4],
                "Total AV": tds[5],
                "HIE AV": tds[6],
                "Reason for Change": tds[7],
            })
    return rows

def fetch_assessor_values(pin: str, jur: str = "016", taxyr: str = "2025", force: bool = False) -> dict:
    pin14 = undashed_pin(pin)
    out = {"_status": "ok", "_meta": {}, "normalized": {"summary": [], "detail": {}, "rows": [], "value_summary_raw": []}}

    # --- Current-year detail (Values)
    
    url_values = (
        f"{VALUES_BASE}?mode=curyear_asmt_values&UseSearch=no"
        f"&pin={pin14}&jur={jur}&taxyr={taxyr}&LMparent=896"
    )
    out["_meta"]["url_values"] = url_values
    try:
        r = requests.get(url_values, headers={"User-Agent": DEFAULT_UA}, timeout=25)
        r.raise_for_status()
        html = r.text
        raw_rec_val = save_raw_text(f"values/{pin14}_{jur}_{taxyr}_cur.html", html)
        out["raw_values"] = {**raw_rec_val, "html_size_bytes": len(html)}
        soup = BeautifulSoup(html, "html.parser")


        # Summary (header+one data row)
        div_sum = soup.find("div", attrs={"name": "CURYEAR_ASMT_VALUES"}) or soup.find(id="datalet_div_0")
        if div_sum:
            tables = div_sum.find_all("table")
            if len(tables) >= 2:
                data_tbl = tables[1]
                trs = data_tbl.find_all("tr")
                if len(trs) >= 2:
                    headers = [td.get_text(" ", strip=True) for td in trs[0].find_all("td")]
                    vals    = [td.get_text(" ", strip=True) for td in trs[1].find_all("td")]
                    if headers and len(vals) >= len(headers):
                        out["normalized"]["summary"] = [{headers[i]: vals[i] for i in range(len(headers))}]

        # Detail (label/value rows)
        div_det = soup.find("div", attrs={"name": "ASMT_VALUES_CCD"}) or soup.find(id="datalet_div_1")
        detail = {}
        if div_det:
            tables = div_det.find_all("table")
            if len(tables) >= 2:
                data_tbl = tables[1]
                for tr in data_tbl.find_all("tr"):
                    tds = tr.find_all("td")
                    if len(tds) >= 2:
                        k = tds[0].get_text(" ", strip=True)
                        v = tds[1].get_text(" ", strip=True)
                        if k:
                            detail[k] = v
        out["normalized"]["detail"] = detail

        # UI rows
        def pick(k): return detail.get(k) or ""
        rows = [
            {"Item": "Total MV", "Value": pick("Total MV")},
            {"Item": "Land MV", "Value": pick("Land MV")},
            {"Item": "Building MV", "Value": pick("Building MV")},
            {"Item": "Total AV", "Value": pick("Total AV")},
            {"Item": "Land AV", "Value": pick("Land AV")},
            {"Item": "Building AV", "Value": pick("Building AV")},
            {"Item": "Tax Year", "Value": pick("Tax Year")},
            {"Item": "Class", "Value": pick("Property Class")},
            {"Item": "Roll Type", "Value": pick("Roll Type")},
            {"Item": "Process Name", "Value": pick("Process Name")},
            {"Item": "Process Date", "Value": pick("Process Date")},
            {"Item": "Reason for Change", "Value": pick("Reason for Change")},
        ]
        out["normalized"]["rows"] = [r for r in rows if any(r.values())]
    except Exception as e:
        out["_status"] = "error"
        out["_meta"]["values_error"] = str(e)


    # --- Value Summary (history table)
    url_summary = (
        f"{VALUES_BASE}?mode=value_summary_cc&UseSearch=no"
        f"&pin={pin14}&jur={jur}&taxyr={taxyr}&LMparent=896"
    )
    out["_meta"]["url_value_summary"] = url_summary
    try:
        r = requests.get(url_summary, headers={"User-Agent": DEFAULT_UA}, timeout=25)
        r.raise_for_status()
        sum_rec = save_raw_text(f"values/{pin14}_{jur}_{taxyr}_summary.html", r.text)
        out["raw_value_summary"] = {**sum_rec, "html_size_bytes": len(r.text)}
        out["normalized"]["value_summary_raw"] = _scrape_value_summary_raw(r.text)
    except Exception as e:
        out["_meta"]["summary_error"] = str(e)

    return out

# --- Assessor Property Location (FULL_LEGAL_CD) ---
def fetch_assessor_location(pin: str, jur: str = "016", taxyr: str = "2025", force: bool = False) -> dict:
    """
    Pull 'Property Location' (mode=full_legal_cd).
    Returns:
      {
        "normalized": {
            "detail": {key: value, ...},   # flat key/value map
            "rows": [ {"Item": key, "Value": value}, ... ]  # verbatim for UI
        },
        "raw": {"html_path": "...", "html_size_bytes": N},
        "_status": "ok"|"error",
        "_meta": {...}
      }
    """
    
    t0 = time.time()
    pin14_und = undashed_pin(pin)

    url = (
        f"{PROFILE_BASE}?mode=full_legal_cd&UseSearch=no"
        f"&pin={pin14_und}&jur={jur}&taxyr={taxyr}&LMparent=896"
    )
    meta = {
        "source": "ASSR_LOCATION",
        "url": url,
        "pin": pin14_und,
        "jur": jur,
        "taxyr": taxyr,
        "fetched_at": _now_iso(),
    }

    try:
        r = requests.get(url, headers={"User-Agent": DEFAULT_UA}, timeout=25)
        r.raise_for_status()
        html = r.text

        # save raw (avoid reloader restarts by excluding/moving this dir)
        raw_dir = Path("raw_cache/property_location")
        raw_dir.mkdir(parents=True, exist_ok=True)
        html_path = raw_dir / f"{pin14_und}_{jur}_{taxyr}.html"
        html_path.write_text(html, encoding="utf-8")

        soup = BeautifulSoup(html, "html.parser")

        # main block + specific table
        block = soup.find("div", id="datalet_div_0", attrs={"name": "FULL_LEGAL_CD"}) or soup
        table = block.find("table", id="Property Description")

        detail = {}
        rows = []
        if table:
            for tr in table.find_all("tr"):
                tds = tr.find_all("td")
                if len(tds) >= 2:
                    key = tds[0].get_text(" ", strip=True).rstrip(":")
                    val = tds[1].get_text(" ", strip=True).replace("\xa0", " ").strip()
                    if key:
                        detail[key] = val
                        rows.append({"Item": key, "Value": val})

        return {
            "normalized": {"detail": detail, "rows": rows},
            "raw": {"html_path": str(html_path), "html_size_bytes": len(html)},
            "_status": "ok",
            "_meta": {**meta, "duration_ms": int((time.time() - t0) * 1000)},
        }

    except Exception as e:
        return {
            "normalized": {"detail": {}, "rows": []},
            "raw": {},
            "_status": "error",
            "_meta": {**meta, "error": str(e)},
        }

# --- Assessor Land (LAND_CC) ---
def fetch_assessor_land(pin: str, jur: str = "016", taxyr: str = "2025", force: bool = False) -> dict:
    """
    Pull 'Land' (mode=land_cc).
    Returns:
      {
        "normalized": {
            "summary": [ { "Line": "...", "Class": "...", "Land Type": "...", "Code": "...", "Square Feet": "..." }, ... ],
            "detail": { ... },                     # key/value map from the lower panel
            "rows": [ {"Item": key, "Value": value}, ... ]   # detail as UI rows
        },
        "raw": {"html_path": "...", "html_size_bytes": N},
        "_status": "ok"|"error",
        "_meta": {...}
      }
    """

    t0 = time.time()
    pin14_und = undashed_pin(pin)

    url = (
        f"{PROFILE_BASE}?mode=land_cc&UseSearch=no"
        f"&pin={pin14_und}&jur={jur}&taxyr={taxyr}&LMparent=896"
    )
    meta = {
        "source": "ASSR_LAND",
        "url": url,
        "pin": pin14_und,
        "jur": jur,
        "taxyr": taxyr,
        "fetched_at": _now_iso(),
    }

    try:
        r = requests.get(url, headers={"User-Agent": DEFAULT_UA}, timeout=25)
        r.raise_for_status()
        html = r.text

        # save raw
        raw_dir = Path("raw_cache/land")
        raw_dir.mkdir(parents=True, exist_ok=True)
        html_path = raw_dir / f"{pin14_und}_{jur}_{taxyr}.html"
        html_path.write_text(html, encoding="utf-8")

        soup = BeautifulSoup(html, "html.parser")

        # ---- Land Summary table
        summary_rows = []
        block_sum = soup.find("div", id="datalet_div_0", attrs={"name": "LAND_CC"}) or soup
        table_sum = block_sum.find("table", id="Land Summary")
        if table_sum:
            trs = table_sum.find_all("tr")
            if len(trs) >= 2:
                headers = [td.get_text(" ", strip=True) for td in trs[0].find_all("td")]
                for tr in trs[1:]:
                    tds = [td.get_text(" ", strip=True).replace("\xa0", " ").strip() for td in tr.find_all("td")]
                    # skip separators/blank rows
                    if not tds or len(tds) < len(headers):
                        continue
                    row = {headers[i]: tds[i] for i in range(len(headers))}
                    # guard against the trailing spacer row the page adds
                    if row.get("Line"):
                        summary_rows.append(row)

        # ---- Detail panel (LAND_CCD)
        detail = {}
        rows = []
        block_det = soup.find("div", id="datalet_div_1", attrs={"name": "LAND_CCD"})
        if block_det:
            # second table under the block is the data
            tables = block_det.find_all("table")
            data_tbl = tables[1] if len(tables) > 1 else (tables[0] if tables else None)
            if data_tbl:
                for tr in data_tbl.find_all("tr"):
                    tds = tr.find_all("td")
                    if len(tds) >= 2:
                        key = tds[0].get_text(" ", strip=True).rstrip(":")
                        val = tds[1].get_text(" ", strip=True).replace("\xa0", " ").strip()
                        if key:
                            detail[key] = val
                            rows.append({"Item": key, "Value": val})

        return {
            "normalized": {"summary": summary_rows, "detail": detail, "rows": rows},
            "raw": {"html_path": str(html_path), "html_size_bytes": len(html)},
            "_status": "ok",
            "_meta": {**meta, "duration_ms": int((time.time() - t0) * 1000)},
        }

    except Exception as e:
        return {
            "normalized": {"summary": [], "detail": {}, "rows": []},
            "raw": {},
            "_status": "error",
            "_meta": {**meta, "error": str(e)},
        }

# --- Assessor Residential Building (RESIDENTIAL_CC) ---
def fetch_assessor_residential(pin: str, jur: str = "016", taxyr: str = "2025", force: bool = False) -> dict:
    """
    Pull 'Residential Building' (mode=residential_cc).
    Returns:
      {
        "normalized": {
            "detail": { ... },                  # key/value map exactly as shown
            "rows": [ {"Item": key, "Value": value}, ... ]  # UI-friendly rows
        },
        "raw": {"html_path": "...", "html_size_bytes": N},
        "_status": "ok"|"error",
        "_meta": {...}
      }
    """
    t0 = time.time()
    pin14_und = undashed_pin(pin)

    url = (
        f"{PROFILE_BASE}?mode=residential_cc&UseSearch=no"
        f"&pin={pin14_und}&jur={jur}&taxyr={taxyr}&LMparent=896"
    )
    meta = {
        "source": "ASSR_RESIDENTIAL",
        "url": url,
        "pin": pin14_und,
        "jur": jur,
        "taxyr": taxyr,
        "fetched_at": _now_iso(),
    }

    try:
        r = requests.get(url, headers={"User-Agent": DEFAULT_UA}, timeout=25)
        r.raise_for_status()
        html = r.text

        # save raw
        raw_dir = Path("raw_cache/residential")
        raw_dir.mkdir(parents=True, exist_ok=True)
        html_path = raw_dir / f"{pin14_und}_{jur}_{taxyr}.html"
        html_path.write_text(html, encoding="utf-8")

        soup = BeautifulSoup(html, "html.parser")

        # Block + table
        detail = {}
        rows = []
        block = soup.find("div", id="datalet_div_0", attrs={"name": "RESIDENTIAL_CC"}) or soup
        table = block.find("table", id="Dwelling Characteristics")
        if table:
            for tr in table.find_all("tr"):
                tds = tr.find_all("td")
                if len(tds) >= 2:
                    key = tds[0].get_text(" ", strip=True).rstrip(":")
                    val = tds[1].get_text(" ", strip=True).replace("\xa0", " ").strip()
                    if key:
                        detail[key] = val
                        rows.append({"Item": key, "Value": val})

        return {
            "normalized": {"detail": detail, "rows": rows},
            "raw": {"html_path": str(html_path), "html_size_bytes": len(html)},
            "_status": "ok",
            "_meta": {**meta, "duration_ms": int((time.time() - t0) * 1000)},
        }

    except Exception as e:
        return {
            "normalized": {"detail": {}, "rows": []},
            "raw": {},
            "_status": "error",
            "_meta": {**meta, "error": str(e)},
        }

# --- Assessor Other Structures (OBY) ---
def fetch_assessor_other_structures(pin: str, jur: str = "016", taxyr: str = "2025", force: bool = False) -> dict:
    """
    Pull 'Other Structures' (mode=oby) and return verbatim content.
    Returns:
      {
        "normalized": {
            "verbatim": "<exact text or HTML snippet>",
            "rows": [ {...} ]  # if a data table exists; else []
        },
        "raw": {"html_path": "...", "html_size_bytes": N},
        "_status": "ok"|"empty"|"error",
        "_meta": {...}
      }
    """
    

    t0 = time.time()
    pin14_und = undashed_pin(pin)

    url = (
        f"{PROFILE_BASE}?mode=oby&UseSearch=no"
        f"&pin={pin14_und}&jur={jur}&taxyr={taxyr}&LMparent=896"
    )
    meta = {
        "source": "ASSR_OBY",
        "url": url,
        "pin": pin14_und,
        "jur": jur,
        "taxyr": taxyr,
        "fetched_at": _now_iso(),
    }

    try:
        r = requests.get(url, headers={"User-Agent": DEFAULT_UA}, timeout=25)
        r.raise_for_status()
        html = r.text

        # save raw
        raw_dir = Path("raw_cache/other_structures")
        raw_dir.mkdir(parents=True, exist_ok=True)
        html_path = raw_dir / f"{pin14_und}_{jur}_{taxyr}.html"
        html_path.write_text(html, encoding="utf-8")

        soup = BeautifulSoup(html, "html.parser")

        # Grab the main 'holder' content (verbatim)
        holder = soup.find("div", class_="holder")
        all_tables = _harvest_tables(holder)
        
        verbatim_html = holder.decode() if holder else ""
        verbatim_text = holder.get_text(" ", strip=True) if holder else ""

        # Try to parse a data table if present (many parcels will show a grid)
        rows = []
        # Common table label sometimes used: 'Other Buildings' or similar
        candidate_tables = []
        for t in (holder.find_all("table") if holder else []):
            hdr = (t.get("id") or "").lower()
            if "other" in hdr or "building" in hdr or "structures" in hdr:
                candidate_tables.append(t)

        # Fallback: pick the first grid-like table with a header row
        if not candidate_tables and holder:
            candidate_tables = [
                t for t in holder.find_all("table")
                if t.find("tr") and t.find("td")
            ]

        # Parse the first meaningful data table, if it’s not the “-- No Data --” stub
        if candidate_tables:
            tbl = candidate_tables[0]
            if "-- No Data --" not in tbl.get_text():
                # build header
                trs = tbl.find_all("tr")
                headers = [td.get_text(" ", strip=True) for td in trs[0].find_all("td")]
                for tr in trs[1:]:
                    tds = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
                    if any(x for x in tds):
                        row = {headers[i] if i < len(headers) else f"Col{i+1}": v for i, v in enumerate(tds)}
                        rows.append(row)

        status = "empty" if ("-- No Data --" in verbatim_text and not rows) else "ok"

        normalized = {
            "verbatim": verbatim_html or verbatim_text or "",
            "rows": rows,
            "__all_tables": all_tables,   # <-- include it here
        }

        return {
            "normalized": normalized,
            "raw": {"html_path": str(html_path), "html_size_bytes": len(html)},
            "_status": status,
            "_meta": {**meta, "duration_ms": int((time.time() - t0) * 1000)},
        }

    except Exception as e:
        return {
            "normalized": {"verbatim": "", "rows": []},
            "raw": {},
            "_status": "error",
            "_meta": {**meta, "error": str(e)},
        }

# --- Assessor Commercial Building (mode=commercial_bldg_cc) ---
def fetch_assessor_commercial_building(pin: str, jur: str = "016", taxyr: str = "2025", force: bool = False) -> dict:
    """
    Pull 'Commercial Building' and return verbatim HTML/text.
    Returns:
      {
        "normalized": {
            "verbatim": "<exact HTML or plain text>",
            "rows": [ {...} ]  # parsed grid if present; else []
        },
        "raw": {"html_path": "...", "html_size_bytes": N},
        "_status": "ok"|"empty"|"error",
        "_meta": {...}
      }
    """


    t0 = time.time()
    pin14_und = undashed_pin(pin)

    url = (
        f"{PROFILE_BASE}?mode=commercial_bldg_cc&UseSearch=no"
        f"&pin={pin14_und}&jur={jur}&taxyr={taxyr}&LMparent=896"
    )
    meta = {
        "source": "ASSR_COMM_BLDG",
        "url": url,
        "pin": pin14_und,
        "jur": jur,
        "taxyr": taxyr,
        "fetched_at": _now_iso(),
    }

    try:
        r = requests.get(url, headers={"User-Agent": DEFAULT_UA}, timeout=25)
        r.raise_for_status()
        html = r.text

        outdir = Path("raw_cache/commercial_building")
        outdir.mkdir(parents=True, exist_ok=True)
        html_path = outdir / f"{pin14_und}_{jur}_{taxyr}.html"
        html_path.write_text(html, encoding="utf-8")

        soup = BeautifulSoup(html, "html.parser")
        holder = soup.find("div", class_="holder")

        verbatim_html = holder.decode() if holder else ""
        verbatim_text = holder.get_text(" ", strip=True) if holder else ""

        rows = []
        if holder:
            # Look for the first grid-like table (if any)
            candidate_tables = [t for t in holder.find_all("table") if t.find("tr")]
            if candidate_tables:
                tbl = candidate_tables[0]
                if "-- No Data --" not in tbl.get_text():
                    trs = tbl.find_all("tr")
                    headers = [td.get_text(" ", strip=True) for td in trs[0].find_all(("td","th"))]
                    for tr in trs[1:]:
                        tds = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
                        if any(tds):
                            rows.append({headers[i] if i < len(headers) else f"Col{i+1}": v
                                         for i, v in enumerate(tds)})

        status = "empty" if ("-- No Data --" in (verbatim_text or verbatim_html)) and not rows else "ok"

        return {
            "normalized": {"verbatim": verbatim_html or verbatim_text or "", "rows": rows},
            "raw": {"html_path": str(html_path), "html_size_bytes": len(html)},
            "_status": status,
            "_meta": {**meta, "duration_ms": int((time.time() - t0) * 1000)},
        }

    except Exception as e:
        return {
            "normalized": {"verbatim": "", "rows": []},
            "raw": {},
            "_status": "error",
            "_meta": {**meta, "error": str(e)},
        }

# --- Assessor Divisions & Consolidations (mode=prop_association) ---
def fetch_assessor_prop_association(pin: str, jur: str = "016", taxyr: str = "2025", force: bool = False) -> dict:
    """
    Pull 'Divisions & Consolidations' and return verbatim HTML/text.

    Returns:
      {
        "normalized": {
            "verbatim": "<exact HTML or plain text>",
            "rows": [ {...} ]  # parsed grid if present; else []
        },
        "raw": {"html_path": "...", "html_size_bytes": N},
        "_status": "ok"|"empty"|"error",
        "_meta": {...}
      }
    """


    t0 = time.time()
    pin14 = undashed_pin(pin)

    url = (
        f"{PROFILE_BASE}?mode=prop_association&UseSearch=no"
        f"&pin={pin14}&jur={jur}&taxyr={taxyr}&LMparent=896"
    )
    meta = {
        "source": "ASSR_PROP_ASSOCIATION",
        "url": url,
        "pin": pin14,
        "jur": jur,
        "taxyr": taxyr,
        "fetched_at": _now_iso(),
    }

    try:
        r = requests.get(url, headers={"User-Agent": DEFAULT_UA}, timeout=25)
        r.raise_for_status()
        html = r.text

        outdir = Path("raw_cache/prop_association")
        outdir.mkdir(parents=True, exist_ok=True)
        html_path = outdir / f"{pin14}_{jur}_{taxyr}.html"
        html_path.write_text(html, encoding="utf-8")

        soup = BeautifulSoup(html, "html.parser")
        holder = soup.find("div", class_="holder")

        verbatim_html = holder.decode() if holder else ""
        verbatim_text = holder.get_text(" ", strip=True) if holder else ""

        rows = []
        if holder:
            tables = [t for t in holder.find_all("table") if t.find("tr")]
            if tables:
                tbl_text = tables[0].get_text(" ", strip=True)
                if "-- No Data --" not in tbl_text:
                    trs = tables[0].find_all("tr")
                    headers = [td.get_text(" ", strip=True) for td in trs[0].find_all(("td","th"))]
                    for tr in trs[1:]:
                        tds = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
                        if any(tds):
                            rows.append({headers[i] if i < len(headers) else f"Col{i+1}": v
                                         for i, v in enumerate(tds)})

        status = "empty" if ("-- No Data --" in (verbatim_text or verbatim_html)) and not rows else "ok"

        return {
            "normalized": {"verbatim": verbatim_html or verbatim_text or "", "rows": rows},
            "raw": {"html_path": str(html_path), "html_size_bytes": len(html)},
            "_status": status,
            "_meta": {**meta, "duration_ms": int((time.time() - t0) * 1000)},
        }

    except Exception as e:
        return {
            "normalized": {"verbatim": "", "rows": []},
            "raw": {},
            "_status": "error",
            "_meta": {**meta, "error": str(e)},
        }
    
# --- Assessor Sales (mode=sales) ---
def fetch_assessor_sales_datalet(pin: str, jur: str = "016", taxyr: str = "2025", force: bool = False) -> dict:
    """
    Pull 'Sales' (mode=sales) and return verbatim HTML plus parsed summary/details.
    Returns:
      {
        "normalized": {
            "verbatim": "<exact HTML for the datalet blocks>",
            "summary_rows": [ { "Sale Date": "...", "Sale Price": "...", ... }, ... ],
            "details": { "Instrument Type": "...", "Grantor/Seller": "...", ... }
        },
        "raw": {"html_path": "...", "html_size_bytes": N},
        "_status": "ok"|"empty"|"error",
        "_meta": {...}
      }
    """

    t0 = time.time()
    pin14 = undashed_pin(pin)

    url = (
        f"{PROFILE_BASE}?mode=sales&UseSearch=no"
        f"&pin={pin14}&jur={jur}&taxyr={taxyr}&LMparent=896"
    )
    meta = {
        "source": "ASSR_SALES",
        "url": url,
        "pin": pin14,
        "jur": jur,
        "taxyr": taxyr,
        "fetched_at": _now_iso(),
    }

    try:
        r = requests.get(url, headers={"User-Agent": DEFAULT_UA}, timeout=25)
        r.raise_for_status()
        html = r.text

        outdir = Path("raw_cache/assessor_sales")
        outdir.mkdir(parents=True, exist_ok=True)
        html_path = outdir / f"{pin14}_{jur}_{taxyr}.html"
        html_path.write_text(html, encoding="utf-8")

        soup = BeautifulSoup(html, "html.parser")

        # Prefer the explicit datalet blocks if present
        div_summary = soup.find("div", attrs={"name": "SALES_SUMMARY"}) or soup.find(id="datalet_div_1")
        div_details = soup.find("div", attrs={"name": "SALES_DETAILS"}) or soup.find(id="datalet_div_2")
        holder = soup.find("div", class_="holder")

        # Build verbatim from the two blocks (fall back to holder if needed)
        verbatim_parts = []
        if div_summary: verbatim_parts.append(div_summary.decode())
        if div_details: verbatim_parts.append(div_details.decode())
        if not verbatim_parts and holder:
            verbatim_parts.append(holder.decode())
        verbatim_html = "\n".join(verbatim_parts)

        # Detect 'No Data'
        page_text = (holder.get_text(" ", strip=True) if holder else soup.get_text(" ", strip=True)).upper()
        no_data = "-- NO DATA --" in page_text

        # ---- Parse summary grid (multiple sales possible)
        summary_rows = []
        if div_summary:
            # Usually table id="Sales"
            tbl = div_summary.find("table", id=lambda v: v and v.lower() == "sales") or div_summary.find("table")
            if tbl:
                trs = tbl.find_all("tr")
                if len(trs) >= 2:
                    headers = [td.get_text(" ", strip=True) for td in trs[0].find_all("td")]
                    for tr in trs[1:]:
                        tds = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
                        if any(tds) and len(tds) >= 2:
                            row = {headers[i] if i < len(headers) else f"Col{i+1}": tds[i] for i in range(len(tds))}
                            # skip separator/blank rows
                            if any(v for v in row.values()):
                                summary_rows.append(row)

        # ---- Parse detail key/value table
        details = {}
        if div_details:
            # Usually table id="Sale Details" (note the space)
            dt_tbl = div_details.find("table", id=lambda v: v and v.lower() == "sale details")
            if not dt_tbl:
                tables = div_details.find_all("table")
                dt_tbl = tables[1] if len(tables) > 1 else (tables[0] if tables else None)
            if dt_tbl:
                for tr in dt_tbl.find_all("tr"):
                    tds = tr.find_all("td")
                    if len(tds) >= 2:
                        k = tds[0].get_text(" ", strip=True).rstrip(":")
                        v = tds[1].get_text(" ", strip=True).replace("\xa0", " ").strip()
                        if k:
                            details[k] = v

        status = "empty" if (no_data and not summary_rows and not details) else "ok"

        return {
            "normalized": {
                "verbatim": verbatim_html,
                "summary_rows": summary_rows,
                "details": details,
            },
            "raw": {"html_path": str(html_path), "html_size_bytes": len(html)},
            "_status": status,
            "_meta": {**meta, "duration_ms": int((time.time() - t0) * 1000)},
        }

    except Exception as e:
        return {
            "normalized": {"verbatim": "", "summary_rows": [], "details": {}},
            "raw": {},
            "_status": "error",
            "_meta": {**meta, "error": str(e)},
        }

# --- Assessor Notice Summary (NOTICE_SUMMARY_CC) ---
def fetch_assessor_notice_summary(pin: str, jur: str = "016", taxyr: str = "2025", force: bool = False) -> dict:
    """
    Pull 'Notice Summary' (mode=notice_summary_cc).

    Returns:
      {
        "normalized": {
            "summary": {key: value, ...},            # top key/value block
            "detail_rows": [ {...row...}, ... ]      # grid under "Notice Detail"
        },
        "raw": {"html_path": "...", "html_size_bytes": N},
        "_status": "ok" | "empty" | "error",
        "_meta": {...}
      }
    """
    
    t0 = time.time()
    pin14 = undashed_pin(pin)

    url = (
        f"{PROFILE_BASE}?mode=notice_summary_cc&UseSearch=no"
        f"&pin={pin14}&jur={jur}&taxyr={taxyr}&LMparent=896"
    )
    meta = {
        "source": "ASSR_NOTICE_SUMMARY",
        "url": url,
        "pin": pin14,
        "jur": jur,
        "taxyr": taxyr,
        "fetched_at": _now_iso(),
    }

    try:
        r = requests.get(url, headers={"User-Agent": DEFAULT_UA}, timeout=25)
        r.raise_for_status()
        html = r.text

        # save raw
        raw_dir = Path("raw_cache/notice_summary")
        raw_dir.mkdir(parents=True, exist_ok=True)
        html_path = raw_dir / f"{pin14}_{jur}_{taxyr}.html"
        html_path.write_text(html, encoding="utf-8")

        soup = BeautifulSoup(html, "html.parser")

        normalized = {"summary": {}, "detail_rows": []}

        # --- Summary block ---
        # Prefer the div name/id, then its second table (after the tiny title table)
        blk_sum = soup.find("div", attrs={"name": "NOTICE_SUMMARY_CC"}) or soup.find(id="datalet_div_0")
        tbl_sum = None
        if blk_sum:
            tbl_sum = blk_sum.find("table", id="Notice Summary")
            if not tbl_sum:
                tables = blk_sum.find_all("table")
                tbl_sum = tables[1] if len(tables) > 1 else (tables[0] if tables else None)

        if tbl_sum:
            for tr in tbl_sum.find_all("tr"):
                tds = tr.find_all("td")
                if len(tds) >= 2:
                    k = tds[0].get_text(" ", strip=True).rstrip(":")
                    v = tds[1].get_text(" ", strip=True).replace("\xa0", " ").strip()
                    if k:
                        normalized["summary"][k] = v

        # --- Detail grid ---
        blk_det = soup.find("div", attrs={"name": "NOTICE_SUMMARY_DETAIL_CC"}) or soup.find(id="datalet_div_1")
        tbl_det = None
        if blk_det:
            tbl_det = blk_det.find("table", id="Notice Detail")
            if not tbl_det:
                det_tables = blk_det.find_all("table")
                # find first grid-looking table
                for t in det_tables:
                    trs = t.find_all("tr")
                    if trs and len(trs[0].find_all(["td", "th"])) >= 2:
                        tbl_det = t
                        break

        if tbl_det:
            trs = tbl_det.find_all("tr")
            if trs:
                headers = [td.get_text(" ", strip=True) for td in trs[0].find_all(["td", "th"])]
                for tr in trs[1:]:
                    tds = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
                    if any(tds):
                        row = {headers[i] if i < len(headers) else f"col_{i+1}": tds[i] if i < len(tds) else "" for i in range(len(headers))}
                        normalized["detail_rows"].append(row)

        page_text = soup.get_text(" ", strip=True).upper()
        status = "empty" if (not normalized["summary"] and not normalized["detail_rows"] and "-- NO DATA --" in page_text) else "ok"

        return {
            "normalized": normalized,
            "raw": {"html_path": str(html_path), "html_size_bytes": len(html)},
            "_status": status,
            "_meta": {**meta, "duration_ms": int((time.time() - t0) * 1000)},
        }

    except Exception as e:
        return {
            "normalized": {"summary": {}, "detail_rows": []},
            "raw": {},
            "_status": "error",
            "_meta": {**meta, "error": str(e)},
        }

# --- Assessor Appeals & COEs (mode=appeals_cc) ---
def fetch_assessor_appeals_coes(pin: str, jur: str = "016", taxyr: str = "2025", force: bool = False) -> dict:
    """
    Pull 'Appeals & COEs' (mode=appeals_cc).

    Returns:
      {
        "normalized": {
            "owner_details": { ... },          # key/value from "Owner/Complainant Details"
            "nature_of_appeal": { ... },       # key/value from "Nature of Appeal"
            "reason_for_appeal": { ... },      # key/value Y/blank flags
            "reason_flags_true": [ ... ]       # convenience list of flagged reasons (value == 'Y')
          },
        "raw": {"html_path": "...", "html_size_bytes": N},
        "_status": "ok" | "empty" | "error",
        "_meta": {...}
      }
    """
    
    t0 = time.time()
    pin14 = undashed_pin(pin)

    url = (
        f"{PROFILE_BASE}?mode=appeals_cc&UseSearch=no"
        f"&pin={pin14}&jur={jur}&taxyr={taxyr}&LMparent=896"
    )
    meta = {
        "source": "ASSR_APPEALS",
        "url": url,
        "pin": pin14,
        "jur": jur,
        "taxyr": taxyr,
        "fetched_at": _now_iso(),
    }

    try:
        r = requests.get(url, headers={"User-Agent": DEFAULT_UA}, timeout=25)
        r.raise_for_status()
        html = r.text

        # save raw
        outdir = Path("raw_cache/appeals_coes")
        outdir.mkdir(parents=True, exist_ok=True)
        html_path = outdir / f"{pin14}_{jur}_{taxyr}.html"
        html_path.write_text(html, encoding="utf-8")

        soup = BeautifulSoup(html, "html.parser")

        def _parse_kv_block(div_name: str, table_id_hint: str):
            """
            Find the block by div name, then pick either the table with the ID hint,
            or the 2nd table under the block (the 1st is usually the tiny title bar).
            Return a dict of label:value rows.
            """
            block = soup.find("div", attrs={"name": div_name})
            if not block:
                # sometimes the index shifts; fall back to any datalet_div_* that contains the hint table
                for cand in soup.find_all("div", id=lambda v: v and v.startswith("datalet_div_")):
                    if cand.find("table", id=table_id_hint):
                        block = cand
                        break
            if not block:
                return {}

            table = block.find("table", id=table_id_hint)
            if not table:
                tables = block.find_all("table")
                table = tables[1] if len(tables) > 1 else (tables[0] if tables else None)
            if not table:
                return {}

            out = {}
            for tr in table.find_all("tr"):
                tds = tr.find_all("td")
                if len(tds) >= 2:
                    k = tds[0].get_text(" ", strip=True).rstrip(":")
                    v = tds[1].get_text(" ", strip=True).replace("\xa0", " ").strip()
                    if k:
                        out[k] = v
            return out

        owner = _parse_kv_block("APPEAL_DETAILS_CC", "Owner/Complainant Details")
        nature = _parse_kv_block("APPEALS_DETAIL_2_CC", "Nature of Appeal")

        # Reasons: same structure, but values are Y/blank; we also build a convenience list
        reasons = _parse_kv_block("APPEALS_DETAIL_3_CC", "Reason For Appeal")
        reason_flags_true = [k for k, v in reasons.items() if (v or "").strip().upper().startswith("Y")]

        normalized = {
            "owner_details": owner,
            "nature_of_appeal": nature,
            "reason_for_appeal": reasons,
            "reason_flags_true": reason_flags_true,
        }

        page_text = soup.get_text(" ", strip=True).upper()
        status = "empty" if (not any(normalized.values()) and "-- NO DATA --" in page_text) else "ok"

        return {
            "normalized": normalized,
            "raw": {"html_path": str(html_path), "html_size_bytes": len(html)},
            "_status": status,
            "_meta": {**meta, "duration_ms": int((time.time() - t0) * 1000)},
        }

    except Exception as e:
        return {
            "normalized": {
                "owner_details": {},
                "nature_of_appeal": {},
                "reason_for_appeal": {},
                "reason_flags_true": [],
            },
            "raw": {},
            "_status": "error",
            "_meta": {**meta, "error": str(e)},
        }

# --- Assessor Permits (mode=permit_ck_cc) ---
def fetch_permits(pin: str, jur: str = "016", taxyr: str = "2025", force: bool = False) -> dict:
    """
    Pull 'Permits' (mode=permit_ck_cc). Handles multiple records via idx=1..N.
    Returns:
      {
        "normalized": {
            "rows": [ { ...permit fields... }, ... ],  # one dict per permit record
            "count": N
        },
        "raw": {"saved_files": [paths...]},
        "_status": "ok"|"empty"|"error",
        "_meta": {...}
      }
    """
    
    t0 = time.time()
    pin14 = undashed_pin(pin)

    base = (
        f"{PROFILE_BASE}?mode=permit_ck_cc&UseSearch=no"
        f"&pin={pin14}&jur={jur}&taxyr={taxyr}&LMparent=896"
    )
    meta = {
        "source": "ASSR_PERMITS",
        "pin": pin14,
        "jur": jur,
        "taxyr": taxyr,
        "base_url": base,
        "fetched_at": _now_iso(),
    }

    def _fetch_idx(idx: int):
        url = f"{base}&idx={idx}&sIndex=0"
        r = requests.get(url, headers={"User-Agent": DEFAULT_UA}, timeout=25)
        r.raise_for_status()
        return url, r.text

    def _parse_one(html: str) -> dict:
        soup = BeautifulSoup(html, "html.parser")
        # Quick "no data" check
        if "-- NO DATA --" in soup.get_text(" ", strip=True).upper():
            return {"__empty__": True}

        block = soup.find("div", attrs={"name": "PERMITS_REC_CC"}) or soup
        table = block.find("table", id="Permit Information")
        one = {}
        if table:
            for tr in table.find_all("tr"):
                tds = tr.find_all("td")
                if len(tds) >= 2:
                    k = tds[0].get_text(" ", strip=True).rstrip(":")
                    v = tds[1].get_text(" ", strip=True).replace("\xa0", " ").strip()
                    if k:
                        one[k] = v

        # Convenience aliases commonly used in UIs
        aliases = {
            "Permit #": one.get("Permit No."),
            "Status": one.get("Permit Status"),
            "Issue Date": one.get("Issue Date"),
            "Amount ($)": one.get("Amount"),
            "Description": one.get("Permit Description") or one.get("Primary Job Description"),
        }
        for k, v in aliases.items():
            if k not in one and v:
                one[k] = v

        # Read record count (if present) so caller can loop
        rec = soup.find("input", id="DTLNavigator_hdRecCount")
        rec_count = int(rec.get("value", "1")) if rec else 1
        return {"__empty__": False, "__rec_count__": rec_count, **one}

    try:
        out_rows, saved_files = [], []
        # First record
        url1, html1 = _fetch_idx(1)
        raw_dir = Path("raw_cache/permits"); raw_dir.mkdir(parents=True, exist_ok=True)
        p1 = raw_dir / f"{pin14}_{jur}_{taxyr}_idx1.html"
        p1.write_text(html1, encoding="utf-8"); saved_files.append(str(p1))

        first = _parse_one(html1)
        if first.get("__empty__"):
            return {
                "normalized": {"rows": [], "count": 0},
                "raw": {"saved_files": saved_files},
                "_status": "empty",
                "_meta": {**meta, "duration_ms": int((time.time() - t0) * 1000), "first_url": url1},
            }

        rec_count = max(1, int(first.get("__rec_count__", 1)))
        # Keep the first parsed record (drop helper flags)
        out_rows.append({k: v for k, v in first.items() if not k.startswith("__")})

        # If more, loop idx=2..rec_count
        for idx in range(2, rec_count + 1):
            url_i, html_i = _fetch_idx(idx)
            pi = raw_dir / f"{pin14}_{jur}_{taxyr}_idx{idx}.html"
            pi.write_text(html_i, encoding="utf-8"); saved_files.append(str(pi))
            rec = _parse_one(html_i)
            if not rec.get("__empty__"):
                out_rows.append({k: v for k, v in rec.items() if not k.startswith("__")})

        return {
            "normalized": {"rows": out_rows, "count": len(out_rows)},
            "raw": {"saved_files": saved_files},
            "_status": "ok",
            "_meta": {
                **meta,
                "record_count_seen": len(out_rows),
                "duration_ms": int((time.time() - t0) * 1000),
            },
        }

    except Exception as e:
        return {
            "normalized": {"rows": [], "count": 0},
            "raw": {},
            "_status": "error",
            "_meta": {**meta, "error": str(e)},
        }

def fetch_assessor_hie_additions(pin: str, jur: str = "016", taxyr: str = "2025", force: bool = False) -> dict:
    """
    Pull 'HIE Additions' (mode=res_addn). Captures any grid- or kv-style table.
    """
    t0 = time.time()
    pin14 = undashed_pin(pin)
    url = f"{PROFILE_BASE}?mode=res_addn&UseSearch=no&pin={pin14}&jur={jur}&taxyr={taxyr}&LMparent=896"
    meta = {"source":"ASSR_HIE_ADDN","url":url,"pin":pin14,"jur":jur,"taxyr":taxyr,"fetched_at":_now_iso()}
    try:
        r = requests.get(url, headers={"User-Agent": DEFAULT_UA}, timeout=25)
        r.raise_for_status()
        html = r.text
        outdir = Path("raw_cache/hie_additions"); outdir.mkdir(parents=True, exist_ok=True)
        html_path = outdir / f"{pin14}_{jur}_{taxyr}.html"
        html_path.write_text(html, encoding="utf-8")

        soup = BeautifulSoup(html, "html.parser")
        holder = soup.find("div", class_="holder")
        verbatim = holder.decode() if holder else ""

        # try to find a likely table
        rows = []
        tables = holder.find_all("table") if holder else []
        for tbl in tables:
            # Skip tiny title bars
            trs = tbl.find_all("tr")
            if not trs or sum(len(tr.find_all(["td","th"])) for tr in trs) < 4:
                continue
            # header?
            heads = [h.get_text(" ", strip=True) for h in trs[0].find_all(["td","th"])]
            if heads and any(heads):
                for tr in trs[1:]:
                    tds = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
                    if any(tds):
                        rows.append({heads[i] if i < len(heads) else f"col_{i+1}": (tds[i] if i < len(tds) else "") for i in range(len(heads))})
            else:
                # kv fallback
                kv = {}
                for tr in trs:
                    tds = tr.find_all("td")
                    if len(tds) >= 2:
                        k = tds[0].get_text(" ", strip=True).rstrip(":")
                        v = tds[1].get_text(" ", strip=True).replace("\xa0"," ").strip()
                        if k: kv[k] = v
                if kv: rows.append(kv)

        page_text = soup.get_text(" ", strip=True).upper()
        status = "empty" if ("-- NO DATA --" in page_text and not rows) else "ok"
        return {
            "normalized": {"rows": rows, "verbatim": verbatim},
            "raw": {"html_path": str(html_path), "html_size_bytes": len(html)},
            "_status": status,
            "_meta": {**meta, "duration_ms": int((time.time()-t0)*1000)},
        }
    except Exception as e:
        return {"normalized":{"rows": [], "verbatim": ""}, "raw": {}, "_status":"error", "_meta":{**meta, "error": str(e)}}


# ======================= PTAX (Socrata) =======================

def fetch_ptax_decl_ids_by_addl_pin(pins: list[str], dataset_id: str = "ay2h-5hx3") -> dict:
    """
    Return unique declaration_ids where any of the given pins appear in the Additional PINs table.
    Accepts dashed/undashed; normalizes both ways.
    """
    dashed, undashed = [], []
    for p in pins or []:
        d = _pin_for_socrata(p)  # XX-XX-XXX-XXX-XXXX
        dashed.append(d)
        undashed.append(d.replace("-", ""))

    def _in(vals):
        vals = [v for v in (vals or []) if v]
        if not vals:
            return None
        return "(" + ",".join("'" + v.replace("'", "''") + "'" for v in vals) + ")"

    incl_d = _in(dashed)
    incl_u = _in(undashed)

    where = []
    if incl_d: where.append(f"pin IN {incl_d}")
    if incl_u: where.append(f"replace(pin,'-','') IN {incl_u}")
    if not where:
        return {"_status": "ok", "normalized": {"declaration_ids": []}, "_meta": {"dataset": dataset_id, "count": 0}}

    try:
        rows = _socrata_get(dataset_id, {
            "$where": " OR ".join(where),
            "$select": "declaration_id",
            "$limit": "5000",
        })
        decls = sorted({r.get("declaration_id") for r in (rows or []) if r.get("declaration_id")})
        return {"_status": "ok", "normalized": {"declaration_ids": decls},
                "_meta": {"dataset": dataset_id, "count": len(decls)}}
    except Exception as e:
        return {"_status": "error", "normalized": {"declaration_ids": []},
                "_meta": {"dataset": dataset_id, "error": str(e)}}


def fetch_ptax_main_by_declaration_ids(declaration_ids: list[str], dataset_id: str = "it54-y4c6") -> dict:
    """
    Fetch PTAX main rows by a list of declaration_ids. Complements fetch_ptax_main_multi
    to cover declarations where target pins are ONLY Additional PINs.
    """
    incl = _ids_in_clause(declaration_ids or [])
    if not incl:
        return {"_status": "ok", "normalized": {"rows": []}, "_meta": {"dataset": dataset_id, "count": 0}}
    try:
        rows = _socrata_get(dataset_id, {
            "$where": f"declaration_id IN {incl}",
            "$order": "date_recorded DESC",
            "$limit": "5000",
        })
        return {"_status": "ok", "normalized": {"rows": rows},
                "_meta": {"dataset": dataset_id, "count": len(rows)}}
    except Exception as e:
        return {"_status": "error", "normalized": {"rows": []},
                "_meta": {"dataset": dataset_id, "error": str(e)}}


SOCRATA_BASE = "https://data.illinois.gov/resource"
APP_TOKEN = os.getenv("ILLINOIS_APP_TOKEN", "")  #

# --- PTAX helpers (place ABOVE any fetch_ptax_* uses) ---
def _pin_for_socrata(pin: str) -> str:
    """Return dashed PIN XX-XX-XXX-XXX-XXXX for Socrata tables."""
    d = undashed_pin(pin)  # "12345678901234"
    return f"{d[0:2]}-{d[2:4]}-{d[4:7]}-{d[7:10]}-{d[10:14]}"

def _ids_in_clause(ids):
    # normalize to strings and drop empties
    ids = [str(x) for x in (ids or []) if str(x)]
    if not ids:
        return None
    # escape single quotes for Socrata SQL and join as ('a','b','c')
    quoted = ",".join("'" + i.replace("'", "''") + "'" for i in ids)
    return f"({quoted})"


def _socrata_get(dataset_id: str, params: dict) -> list:
    """
    Socrata fetch with auto-pagination, polite throttling (esp. when no APP_TOKEN),
    and backoff on 429/5xx. Keeps memory small by streaming pages.
    """
    headers = {"User-Agent": "PIN-Tool/1.0"}
    if APP_TOKEN:
        headers["X-App-Token"] = APP_TOKEN

    out, limit, offset = [], int(params.get("$limit", 5000)), 0

    # Anonymous = slower; tokened = faster
    max_rps          = 4 if not APP_TOKEN else 10
    base_sleep       = 1.0 / max(1, max_rps)           # ~0.25s (anon)
    backoff_sleep_s  = 1.0
    backoff_max_s    = 20.0
    retries_per_page = 5

    max_pages = int(os.getenv("SOCRATA_MAX_PAGES", "200"))
    pages = 0
    while True:
        q = {**params, "$limit": str(limit), "$offset": str(offset)}
        attempt = 0
        while True:
            try:
                time.sleep(base_sleep)
                url = f"{SOCRATA_BASE}/{dataset_id}.json"
                r = requests.get(url, headers=headers, params=q, timeout=30)
                if r.status_code in (429, 500, 502, 503, 504):
                    attempt += 1
                    if attempt > retries_per_page:
                        raise requests.HTTPError(f"{r.status_code} after retries from {dataset_id}: {r.text}", response=r)
                    time.sleep(min(backoff_sleep_s, backoff_max_s))
                    backoff_sleep_s = min(backoff_sleep_s * 2, backoff_max_s)
                    continue
                r.raise_for_status()
                batch = r.json()
                break
            except requests.Timeout:
                attempt += 1
                if attempt > retries_per_page:
                    raise
                time.sleep(min(backoff_sleep_s, backoff_max_s))
                backoff_sleep_s = min(backoff_sleep_s * 2, backoff_max_s)

        if not batch:
            break
        out.extend(batch)
        if len(batch) < limit:
            break
        offset += limit
        pages += 1
        if pages >= max_pages:
            break
        time.sleep(base_sleep)

    return out



def fetch_ptax_main(pin: str, dataset_id: str = "it54-y4c6") -> dict:
    dashed = _pin_for_socrata(pin)
    try:
        rows = _socrata_get(dataset_id, {
            "$where": "line_1_primary_pin = :p OR replace(line_1_primary_pin,'-','') = :p_nodash",
            "$order": "date_recorded DESC",
            "$limit": "2000",
            "p": dashed,
            "p_nodash": dashed.replace("-", ""),
        })
        return {"_status": "ok", "normalized": {"rows": rows}, "_meta": {"dataset": dataset_id, "count": len(rows)}}
    except Exception as e:
        return {"_status": "error", "normalized": {"rows": []}, "_meta": {"dataset": dataset_id, "error": str(e)}}

def fetch_ptax_main_multi(pins: list[str], dataset_id: str = "it54-y4c6") -> dict:
    """
    Fetch PTAX main rows for many PINs at once (<= ~50; your usage ~20).
    Matches both dashed and undashed formats.
    """
    dashed, undashed = [], []
    for p in pins or []:
        d = _pin_for_socrata(p)         # XX-XX-XXX-XXX-XXXX
        dashed.append(d)
        undashed.append(d.replace("-", ""))

    def _in_clause(vals):
        vals = [v for v in (vals or []) if v]
        if not vals:
            return None
        return "(" + ",".join("'" + v.replace("'", "''") + "'" for v in vals) + ")"

    incl_d = _in_clause(dashed)
    incl_u = _in_clause(undashed)
    if not (incl_d or incl_u):
        return {"_status": "ok", "normalized": {"rows": []}, "_meta": {"dataset": dataset_id, "count": 0}}

    where = []
    if incl_d: where.append(f"line_1_primary_pin IN {incl_d}")
    if incl_u: where.append(f"replace(line_1_primary_pin,'-','') IN {incl_u}")

    try:
        rows = _socrata_get(dataset_id, {
            "$where": " OR ".join(where),
            "$order": "date_recorded DESC",
            "$limit": "5000",
        })
        return {"_status": "ok", "normalized": {"rows": rows}, "_meta": {"dataset": dataset_id, "count": len(rows)}}
    except Exception as e:
        return {"_status": "error", "normalized": {"rows": []}, "_meta": {"dataset": dataset_id, "error": str(e)}}

def merge_ptax_by_declaration(ptax_main_rows, buyers_rows, sellers_rows, addlpins_rows, personal_rows, normalize_pin_fn=None):
    """
    Returns (bundle_dict, summary_rows_list, set_matches).
    bundle_dict[decl_id] = { main, buyers, sellers, additional_pins, personal_property, matched_pins }
    summary_rows_list = compact rows for a list UI.
    set_matches(pin_match_map) lets caller attach matched pins per decl_id later.
    """
    from collections import defaultdict
    buyers_by = defaultdict(list)
    sellers_by = defaultdict(list)
    pins_by   = defaultdict(list)
    pp_by     = defaultdict(list)

    for b in buyers_rows:   buyers_by[b.get("declaration_id")].append(b)
    for s in sellers_rows:  sellers_by[s.get("declaration_id")].append(s)
    for p in addlpins_rows: pins_by[p.get("declaration_id")].append(p)
    for x in personal_rows: pp_by[x.get("declaration_id")].append(x)

    pin_match_map = {}
    def set_matches(mapping):
        nonlocal pin_match_map
        pin_match_map = mapping or {}

    bundle = {}
    from datetime import datetime as _dt
    def _pdate(s):
        try: return _dt.fromisoformat((s or "").replace("Z",""))
        except Exception: return _dt.min
    main_sorted = sorted(ptax_main_rows or [], key=lambda r: _pdate(r.get("date_recorded")), reverse=True)

    for r in main_sorted:
        did = r.get("declaration_id")
        if not did: continue
        bundle[did] = {
            "main": r,
            "buyers": buyers_by.get(did, []),
            "sellers": sellers_by.get(did, []),
            "additional_pins": pins_by.get(did, []),
            "personal_property": pp_by.get(did, []),
            "matched_pins": sorted(list(pin_match_map.get(did, set()))) if pin_match_map else [],
        }

    def _row_summary(d):
        m = d.get("main", {})
        primary_pin = m.get("line_1_primary_pin") or ""
        if normalize_pin_fn:
            try: primary_pin = normalize_pin_fn(primary_pin)
            except Exception: pass
        return {
            "declaration_id": m.get("declaration_id"),
            "date_recorded": m.get("date_recorded"),
            "primary_pin": primary_pin,
            "purchase_price": m.get("purchase_price"),
            "grantor": m.get("grantor_name"),
            "grantee": m.get("grantee_name"),
            "doc_number": m.get("doc_number"),
            "matched_pins": d.get("matched_pins", []),
            "additional_pins_count": len(d.get("additional_pins", [])),
        }
    summary_rows = [_row_summary(bundle[did]) for did in bundle]
    return bundle, summary_rows, set_matches

def fetch_ptax_additional_buyers(declaration_ids=None, dataset_id: str = "dwt7-rycp") -> dict:
    incl = _ids_in_clause(declaration_ids or [])
    if not incl:
        return {"_status": "ok", "normalized": {"rows": []}, "_meta": {"dataset": dataset_id, "count": 0}}
    try:
        rows = _socrata_get(dataset_id, {"$where": f"declaration_id IN {incl}", "$order": "buyer_name ASC", "$limit": "5000"})
        return {"_status": "ok", "normalized": {"rows": rows}, "_meta": {"dataset": dataset_id, "count": len(rows)}}
    except Exception as e:
        return {"_status": "error", "normalized": {"rows": []}, "_meta": {"dataset": dataset_id, "error": str(e)}}

def fetch_ptax_additional_sellers(declaration_ids=None, dataset_id: str = "rzbz-mw8b") -> dict:
    incl = _ids_in_clause(declaration_ids or [])
    if not incl:
        return {"_status": "ok", "normalized": {"rows": []}, "_meta": {"dataset": dataset_id, "count": 0}}
    try:
        rows = _socrata_get(dataset_id, {"$where": f"declaration_id IN {incl}", "$order": "seller_name ASC", "$limit": "5000"})
        return {"_status": "ok", "normalized": {"rows": rows}, "_meta": {"dataset": dataset_id, "count": len(rows)}}
    except Exception as e:
        return {"_status": "error", "normalized": {"rows": []}, "_meta": {"dataset": dataset_id, "error": str(e)}}

def fetch_ptax_additional_pins(declaration_ids=None, dataset_id: str = "ay2h-5hx3") -> dict:
    incl = _ids_in_clause(declaration_ids or [])
    if not incl:
        return {"_status": "ok", "normalized": {"rows": []}, "_meta": {"dataset": dataset_id, "count": 0}}
    try:
        rows = _socrata_get(dataset_id, {"$where": f"declaration_id IN {incl}", "$order": "pin ASC", "$limit": "5000"})
        return {"_status": "ok", "normalized": {"rows": rows}, "_meta": {"dataset": dataset_id, "count": len(rows)}}
    except Exception as e:
        return {"_status": "error", "normalized": {"rows": []}, "_meta": {"dataset": dataset_id, "error": str(e)}}

def fetch_ptax_personal_property(declaration_ids=None, dataset_id: str = "b46z-jwev") -> dict:
    incl = _ids_in_clause(declaration_ids or [])
    if not incl:
        return {"_status": "ok", "normalized": {"rows": []}, "_meta": {"dataset": dataset_id, "count": 0}}
    try:
        rows = _socrata_get(dataset_id, {"$where": f"declaration_id IN {incl}", "$order": "declaration_id DESC", "$limit": "5000"})
        return {"_status": "ok", "normalized": {"rows": rows}, "_meta": {"dataset": dataset_id, "count": len(rows)}}
    except Exception as e:
        return {"_status": "error", "normalized": {"rows": []}, "_meta": {"dataset": dataset_id, "error": str(e)}}

# =================================================================

# ======================= Recorder of Deeds (ROD) =======================
# Site: https://crs.cookcountyclerkil.gov
# We search by undashed PIN via: /Search/Result?id1=<14-digit>
# and parse both (A) Associated Pins table, and (B) deed rows.


ROD_BASE = "https://crs.cookcountyclerkil.gov"

def _rod_headers():
    return {"User-Agent": "PinTool/1.0 (+https://example.com)"}

import urllib.parse
import re

def _extract_dids_from_deed_url(deed_url: str) -> tuple[str, str]:
    """
    Pull dId and hId from the Document/Detail URL.
    """
    try:
        q = urllib.parse.urlparse(deed_url).query
        params = urllib.parse.parse_qs(q)
        dId = (params.get("dId", [""])[0] or "").strip()
        hId = (params.get("hId", [""])[0] or "").strip()
        return dId, hId
    except Exception:
        return "", ""

def _rod_fetch_pin_table_html(dId: str, hId: str) -> str:
    """
    Try the endpoints that serve the Associated PINs table used by the accordion.
    We try sortpinresult first (what your snippet shows), then pinresult.
    """
    candidates = [
        f"{ROD_BASE}/Document/sortpinresult?dId={dId}&hId={hId}&column=PIN&direction=asc&page=",
        f"{ROD_BASE}/Document/pinresult?dId={dId}&hId={hId}",
    ]
    for url in candidates:
        r = requests.get(url, headers=_rod_headers(), timeout=30)
        if r.status_code == 200 and "<table" in r.text.lower():
            return r.text
    return ""


# --- NEW helper in fetchers.py ---
def _parse_assoc_pins_from_detail_soup(soup: BeautifulSoup) -> dict:
    """
    On Document Detail pages, the Associated PINs table appears under an accordion.
    We detect any table whose header includes 'Property Index # (PIN)' and collect
    the first-column anchors.
    Returns: {"rows":[{pin, pin_undashed, address, ...}], "unique_dashed":[...], "unique_undashed":[...]}
    """
    target = None
    for tbl in soup.find_all("table"):
        # check header text (either <thead> or first <tr>)
        header_row = tbl.find("thead") or tbl.find("tr")
        if not header_row:
            continue
        hdr = header_row.get_text(" ", strip=True).upper()
        if "PROPERTY INDEX" in hdr and "PIN" in hdr:
            target = tbl
            break
    if target is None:
        return {"rows": [], "unique_dashed": [], "unique_undashed": []}

    # build column keys (nice-to-have; we mainly care about the first col with the PIN link)
    keys = []
    if target.find("thead"):
        ths = target.find("thead").find_all(["th","td"])
    else:
        first_tr = target.find("tr")
        ths = first_tr.find_all(["th","td"]) if first_tr else []
    for th in ths:
        txt = th.get_text(" ", strip=True).strip().lower()
        # normalize a few common headers
        if "property index" in txt and "pin" in txt:
            keys.append("pin")
        elif "address" in txt:
            keys.append("address")
        else:
            keys.append(re.sub(r"\W+", "_", txt) or "col")

    body = target.find("tbody") or target
    rows_out, u_dashed, u_und = [], [], []
    seen_d, seen_u = set(), set()

    for tr in body.find_all("tr"):
        tds = tr.find_all("td")
        if not tds:
            continue
        # first cell contains the PIN link
        a = tds[0].find("a", href=True)
        txt = (a.get_text(" ", strip=True) if a else tds[0].get_text(" ", strip=True)).strip()
        und = ""
        if a:
            try:
                q = urllib.parse.urlparse(a["href"]).query
                und = (urllib.parse.parse_qs(q).get("id1", [""])[0] or "").strip()
            except Exception:
                und = re.sub(r"\D", "", txt)
        else:
            und = re.sub(r"\D", "", txt)

        row = {"pin": txt, "pin_undashed": und}
        # map a couple more columns if present (address commonly in col 2)
        if len(tds) > 1:
            row["address"] = tds[1].get_text(" ", strip=True)

        rows_out.append(row)
        if "-" in txt and txt not in seen_d:
            u_dashed.append(txt); seen_d.add(txt)
        if und and und not in seen_u:
            u_und.append(und); seen_u.add(und)

    return {"rows": rows_out, "unique_dashed": u_dashed, "unique_undashed": u_und}


def fetch_rod_search_html(pin: str) -> dict:
    """
    GET the ROD 'Search/Result' page for the given PIN.
    Saves the HTML and returns {"url": ..., "html": ..., "raw": {...}}.
    """
    und = undashed_pin(pin)  # e.g., "17344060270000"
    url = f"{ROD_BASE}/Search/Result?id1={und}"
    r = requests.get(url, headers=_rod_headers(), timeout=30)
    r.raise_for_status()
    html = r.text

    #Detect common bot-blocker pages
    upper = html.upper()
    blocked = any(s in upper for s in [
        "VERIFY YOU ARE HUMAN", 
        "ACCESS DENIED", 
        "BLOCKED", 
        "CLOUDFLARE", 
        "PLEASE ENABLE JAVASCRIPT"
    ])

    

    # prefer the shared save_raw_text if it's defined in this module
    rel = f"rod/search_result_{und}.html"
    try:
        raw = save_raw_text(rel, html)  # from earlier S3/disk helper
    except Exception:
        p = Path("raw_cache/rod") / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(html, encoding="utf-8")
        raw = {"storage": "disk", "path": str(p)}
    raw["html_size_bytes"] = len(html)
    return {"url": url, "html": html, "raw": raw, "blocked": blocked}


def _parse_rod_associated_pins(html: str) -> dict:
    """
    Parse the 'Associated Pins' table on the results page.
    Returns both dashed and undashed unique lists, and full rows (address, etc.).
    """
    soup = BeautifulSoup(html, "html.parser")

    # Find a table whose header contains "Property Index # (PIN)"
    target = None
    for tbl in soup.find_all("table"):
        ths = [th.get_text(" ", strip=True) for th in tbl.find_all("th")]
        if any("PROPERTY INDEX" in t.upper() and "PIN" in t.upper() for t in ths):
            target = tbl
            break
    # Fallback: any table with links like /Search/ResultByPin?id1=<14digits>
    if target is None:
        for tbl in soup.find_all("table"):
            if tbl.find("a", href=re.compile(r"/Search/ResultByPin\?id1=\d{14}")):
                target = tbl
                break
    if target is None:
        return {"rows": [], "unique_dashed": [], "unique_undashed": []}

    # Build header -> key map (so we can also capture Address/Subdiv/Lot etc.)
    def _key(h: str) -> str:
        h = (h or "").strip().lower()
        mapping = {
            "property index # (pin)": "pin",
            "address": "address",
            "proptype": "prop_type",
            "unit": "unit",
            "s": "s",
            "t": "t",
            "r": "r",
            "subdiv": "subdivision",
            "lot": "lot",
            "block": "block",
            "part lot": "part_lot",
            "bldg": "bldg",
        }
        return mapping.get(h, re.sub(r"\W+", "_", h) or "col")

    header = target.find("thead")
    keys = []
    if header:
        ths = header.find_all("th")
        keys = [_key(th.get_text(" ", strip=True)) for th in ths]

    body = target.find("tbody") or target
    rows_out = []
    for tr in body.find_all("tr"):
        tds = tr.find_all("td")
        if not tds:
            continue
        row = {}

        # First cell: PIN with <a href="/Search/ResultByPin?id1=########......">
        a = tds[0].find("a", href=True)
        dashed = (a.get_text(" ", strip=True) if a else tds[0].get_text(" ", strip=True)).strip()
        # undashed from href param when possible
        und = ""
        if a:
            try:
                q = urllib.parse.urlparse(a["href"]).query
                params = urllib.parse.parse_qs(q)
                und = (params.get("id1", [""])[0] or "").strip()
            except Exception:
                und = re.sub(r"\D", "", dashed)
        else:
            und = re.sub(r"\D", "", dashed)
        row["pin"] = dashed
        row["pin_undashed"] = und

        # other columns mapped by header labels (when present)
        for i, td in enumerate(tds[1:], start=1):
            key = keys[i] if i < len(keys) else f"col_{i+1}"
            row[key] = td.get_text(" ", strip=True)

        rows_out.append(row)

    # Unique sets
    u_dashed, u_und = [], []
    seen_d, seen_u = set(), set()
    for r in rows_out:
        d = r.get("pin")
        u = r.get("pin_undashed")
        if d and d not in seen_d:
            u_dashed.append(d); seen_d.add(d)
        if u and u not in seen_u:
            u_und.append(u); seen_u.add(u)

    return {"rows": rows_out, "unique_dashed": u_dashed, "unique_undashed": u_und}

def _parse_rod_deed_rows(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    table = soup.select_one("#tblData") or soup.find("table")
    if not table:
        return []

    # Build header map if there is a THEAD
    headers = []
    thead = table.find("thead")
    if thead:
        ths = thead.find_all(["th","td"])
        headers = [th.get_text(" ", strip=True).upper() for th in ths]

    # Helper to find column index by name fragment
    def col_idx(*candidates):
        up = [h.upper() for h in headers]
        for i, h in enumerate(up):
            if any(c in h for c in candidates):
                return i
        return None

    # Try to find likely columns
    idx_exec  = col_idx("EXEC", "EXECUTED", "DOC DATE", "DOCUMENT DATE")
    idx_type  = col_idx("DOC TYPE", "DOCUMENT TYPE", "TYPE")

    out = []
    tbody = table.find("tbody") or table
    for tr in tbody.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 3:
            continue

        # doc link
        a = tr.find("a", href=True)
        url = ROD_BASE + a["href"] if a and a["href"].startswith("/") else (a["href"] if a else "")

        # type and exec date (fallbacks if no header mapping)
        exec_txt = tds[idx_exec].get_text(strip=True) if (idx_exec is not None and idx_exec < len(tds)) else ""
        type_txt = tds[idx_type].get_text(strip=True) if (idx_type is not None and idx_type < len(tds)) else (tds[-1].get_text(strip=True) if len(tds)>=1 else "")

        if "DEED" not in type_txt.upper():
            continue

        try:
            exec_date = datetime.strptime(exec_txt, "%m/%d/%Y") if exec_txt else datetime.min
        except Exception:
            exec_date = datetime.min

        out.append({
            "type": type_txt,
            "executed": exec_date,
            "executed_str": exec_txt,
            "url": url,
        })
    return out

def _select_top_deeds(all_deeds: list[dict], n: int) -> list[dict]:
    """
    Return top-N deeds by executed date (desc). Ensures 'executed_str' is set
    and drops the raw datetime for JSON cleanliness.
    """
    if not all_deeds:
        return []
    # sort newest first
    ranked = sorted(
        all_deeds,
        key=lambda d: (d.get("executed") or datetime.min),
        reverse=True
    )
    picked = []
    for d in ranked[:max(1, n)]:
        dd = dict(d)  # shallow copy
        if not dd.get("executed_str"):
            if dd.get("executed") and dd["executed"] != datetime.min:
                dd["executed_str"] = dd["executed"].strftime("%m/%d/%Y")
            else:
                dd["executed_str"] = ""
        dd.pop("executed", None)
        picked.append(dd)
    return picked


def _select_latest_deed(all_deeds: list[dict]) -> list[dict]:
    """
    Return a single-item list with the newest deed purely by executed date,
    ignoring deed type (WARRANTY/QUITCLAIM/etc).
    """
    if not all_deeds:
        return []
    # pick the deed with the max executed datetime
    latest = max(all_deeds, key=lambda d: d.get("executed") or datetime.min)

    # ensure nice string and drop the datetime object
    if not latest.get("executed_str"):
        latest["executed_str"] = (
            latest["executed"].strftime("%m/%d/%Y")
            if latest["executed"] and latest["executed"] != datetime.min else ""
        )
    latest.pop("executed", None)
    return [latest]



def fetch_rod_deed_detail(deed_url: str) -> dict:
    r = requests.get(deed_url, headers=_rod_headers(), timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    # --- your existing consideration + party parsing here ---

    consid = ""
    th = soup.find("th", string=re.compile(r"Consideration", re.I))
    if th:
        td = th.find_next_sibling("td")
        if td:
            consid = td.get_text(strip=True)
    if not consid:
        txt = soup.find(text=re.compile(r"Consideration\s*[:\-]", re.I))
        if txt:
            m = re.search(r"Consideration\s*[:\-]\s*(\$\s*[\d,]+(?:\.\d{2})?)", txt)
            if m:
                consid = m.group(1).replace(" ", "")

    def _party_table(label: str) -> list[dict]:
        span = soup.find("span", string=re.compile(fr"^{label}$", re.I))
        if not span:
            return []
        tbl = span.find_next("table")
        if not tbl:
            return []
        out = []
        for tr in tbl.select("tbody tr"):
            tds = tr.find_all("td")
            if not tds:
                continue
            name  = tds[0].get_text(strip=True) if len(tds) >= 1 else ""
            trust = tds[1].get_text(strip=True) if len(tds) >= 2 else ""
            out.append({"name": name, "trust": trust})
        return out
    # --- Associated PINs: try inline first ---
    assoc = _parse_assoc_pins_from_detail_soup(soup)

    # If none inline, fetch the AJAX table (sortpinresult/pinresult)
    if not assoc.get("rows"):
        dId, hId = _extract_dids_from_deed_url(deed_url)
        if dId and hId:
            html = _rod_fetch_pin_table_html(dId, hId)
            if html:
                assoc = _parse_assoc_pins_from_detail_soup(BeautifulSoup(html, "html.parser"))

    return {
        "consideration": consid,
        "grantors": _party_table("Grantors"),
        "grantees": _party_table("Grantees"),
        "associated_pins_dashed": assoc.get("unique_dashed", []),
        "associated_pins_undashed": assoc.get("unique_undashed", []),
        "associated_rows": assoc.get("rows", []),
    }

def fetch_recorder_bundle(pin: str, top_n: int = 3) -> dict:
    """
    One-stop shop:
      - download search page for PIN
      - parse 'Associated Pins' (unique)
      - parse all deed rows and return top N with details (consideration + parties)
    """
    t0 = time.time()
    try:
        page = fetch_rod_search_html(pin)
        html = page["html"]
        if page.get("blocked"):
            return {
                "_status": "error",
                "normalized": {},
                "raw": page.get("raw", {}),
                "_meta": {"error": "ROD blocked by bot/human check", "searched_pin": undashed_pin(pin)}
            }

        assoc = _parse_rod_associated_pins(html)
        all_deeds = _parse_rod_deed_rows(html)

        # Build an "all_deeds" flat list for the UI/history (lightweight, no extra HTTP)
        all_deeds_out = []
        for d in all_deeds:
            row = {
                "type": d.get("type", ""),
                "executed": (d["executed"].strftime("%m/%d/%Y")
                             if d.get("executed") and d["executed"] != datetime.min else ""),
                "url": d.get("url", ""),
            }
            all_deeds_out.append(row)

        # Enrich ONLY the top-N most recent deeds (default 3 via function arg)
        top = _select_top_deeds(all_deeds, top_n or 1)


        enriched = []
        for d in top:
            detail = {}
            try:
                detail = fetch_rod_deed_detail(d["url"]) if d.get("url") else {}
            except Exception:
                detail = {}
            enriched.append({**d, **detail})



        # Prefer search-page pins; if empty, use pins from the deed detail we just fetched
        pins_dashed = assoc.get("unique_dashed") or [
            normalize_pin(u) for u in (assoc.get("unique_undashed") or []) if u
        ]
        if not pins_dashed and enriched:
            dd = enriched[0]
            pins_dashed = dd.get("associated_pins_dashed") or [
                normalize_pin(u) for u in (dd.get("associated_pins_undashed") or []) if u
            ]

        assoc_rows = [{"Associated Pins": p} for p in pins_dashed]


        return {
            "_status": "ok",
            "normalized": {
                "Associated Pins": assoc_rows,
                "associated_pins_dashed": pins_dashed,
                "associated_pins_undashed": assoc.get("unique_undashed", []),
                "associated_count": len(pins_dashed),
                "all_deeds": all_deeds_out,        # NEW: lightweight list of every deed row
                "top_deeds": enriched,             # Enriched details for top-N
                "latest_deed": (enriched[0] if enriched else {}),
            },

            "raw": page["raw"],
            "_meta": {
                "searched_pin": undashed_pin(pin),
                "duration_ms": int((time.time()-t0)*1000),
                "html_bytes": page["raw"].get("html_size_bytes"),
                "all_deeds_found": len(all_deeds),
                "top_deeds_selected": len(top),
            },
        }
    except Exception as e:
        return {"_status": "error", "normalized": {}, "raw": {}, "_meta": {"error": str(e)}}
# ======================================================================

#=========PTAB==========================================================

def _ptab_get_docket_list_from_pin(sess: requests.Session, pin14: str) -> list[tuple[str, str]]:
    """
    Returns a list of (short_docket, full_docket) for a 14-digit PIN.
    short like '14-24172', full like '2014-024172'
    """
    def _parse(html: str) -> list[tuple[str, str]]:
        soup = BeautifulSoup(html, "html.parser")
        out: list[tuple[str, str]] = []
        for tr in soup.select("tr"):
            tds = tr.find_all("td")
            if len(tds) >= 2:
                a = tds[0].find("a", href=True)
                if a and "docketno=" in a["href"].lower():
                    m = re.search(r"docketno=(\d+-\d+)", a["href"], re.I)
                    if not m:
                        continue
                    short_ = m.group(1)                  # e.g., 14-24172
                    full   = tds[1].get_text(strip=True) # e.g., 2014-024172
                    out.append((short_, full))
        return out

    # 1) try undashed (current behavior)
    url_und = f"{PTAB_BASE}/PropertyPIN.asp?PropPin={pin14}&button=Submit"
    r = sess.get(url_und, timeout=PTAB_TIMEOUT)
    dockets = _parse(r.text)
    if dockets:
        return dockets

    # 2) retry with dashed format if nothing came back
    dashed = f"{pin14[0:2]}-{pin14[2:4]}-{pin14[4:7]}-{pin14[7:10]}-{pin14[10:14]}" if len(pin14) == 14 else pin14
    url_dsh = f"{PTAB_BASE}/PropertyPIN.asp?PropPin={dashed}&button=Submit"
    r2 = sess.get(url_dsh, timeout=PTAB_TIMEOUT)
    return _parse(r2.text)


def _ptab_get_associated_pins(sess: requests.Session, short_d: str) -> list[str]:
    """
    Returns every PIN listed in the 'Property Details' table for a given short docket (e.g., '14-24172').
    """
    url = f"{PTAB_BASE}/property.asp?docketno={short_d}"
    r = sess.get(url, timeout=PTAB_TIMEOUT)
    soup = BeautifulSoup(r.text, "html.parser")
    h3 = next((h for h in soup.find_all("h3") if "Property Details" in h.get_text()), None)
    if not h3:
        return []
    tbl = h3.find_parent("table")
    pins: list[str] = []
    for tr in tbl.find_all("tr")[1:]:
        a = tr.find("a", href=re.compile(r"PropertyDetails\.asp"))
        if a:
            pins.append(a.get_text(strip=True))
    return pins

def _ptab_extract_details(sess: requests.Session, pin_text: str, short_d: str, full_d: str) -> dict:
    """
    Fetches appeal page + property detail page (+ intervenor page if present),
    and extracts all fields into a flat dict. pin_text can be dashed or 14-digit.
    """
    cols = [
        "PIN No","Docket No",
        "First Name","Last Name","Street 1","Street 2","City, State","ZIP Code",
        "County","Township","Type","Class",
        "Transaction Date","Transaction Description","Received/Letter Date",
        "Attorney First Name","Attorney Last Name","Attorney Firm Name",
        "Attorney Street 1","Attorney Street 2","Attorney City","Attorney State",
        "Attorney ZIP Code","Attorney Phone",
        "Intervenor Name","Intervenor Street 1","Intervenor Street 2",
        "Intervenor City","Intervenor State","Intervenor ZIP Code","Intervenor Phone",
        "Intervenor Owner","Intervenor Confirmed",
        "Intervenor Resolution Required","Intervenor Resolution Received",
        "Int Attorney First Name","Int Attorney Last Name","Int Attorney Firm Name",
        "Int Attorney Street 1","Int Attorney Street 2","Int Attorney City",
        "Int Attorney State","Int Attorney ZIP Code","Int Attorney Phone",
        "Int Hist Txn Date","Int Hist Description","Int Hist Received Date",
        "Board of Review Land","Board of Review Improvements","Board of Review Farm Land",
        "Board of Review Farm Building","Board of Review Total",
        "Appellant Land","Appellant Improvements","Appellant Farm Land",
        "Appellant Farm Building","Appellant Total",
        "PTAB Land","PTAB Improvements","PTAB Farm Land","PTAB Farm Building","PTAB Total",
        "County Status","Decision Type","Close Date","Reason Closed","Hearing Status",
        "Hearing Date","Hearing Time","Hearing Site","Meeting ID","Meeting Password",
    ]
    data = {c: "" for c in cols}
    data["PIN No"] = pin_text
    data["Docket No"] = full_d

    appeal_url  = f"{PTAB_BASE}/property.asp?docketno={short_d}"
    appeal_resp = sess.get(appeal_url, timeout=PTAB_TIMEOUT)
    appeal_soup = BeautifulSoup(appeal_resp.text, "html.parser")

    # Find the property detail link for this PIN (or fallback)
    detail_href = None
    for tbl in appeal_soup.find_all("table"):
        rows = tbl.find_all("tr")
        if len(rows) < 3:
            continue
        hdr = [cell.get_text(strip=True).lower() for cell in rows[1].find_all(["th","td"])]
        if "pin" not in hdr:
            continue
        for data_row in rows[2:]:
            a = data_row.find("a", href=True)
            if not a:
                continue
            pin_text_row = a.get_text(strip=True)
            if not pin_text or pin_text_row == pin_text:
                data["PIN No"] = pin_text_row
                detail_href = a["href"]
                break
        if detail_href:
            break
    if not detail_href:
        detail_href = f"PropertyDetails.asp?proppin={data['PIN No']}&docketno={full_d}"

    detail_url = urljoin(appeal_url, detail_href)
    p_resp     = sess.get(detail_url, timeout=PTAB_TIMEOUT)
    p_soup     = BeautifulSoup(p_resp.text, "html.parser")

    # a) Header table on appeal page (owner/contact/township/type/class)
    hdr_tbls = appeal_soup.find_all("table")
    if hdr_tbls:
        hdr = hdr_tbls[0]
        for tr in hdr.find_all("tr"):
            for L, V in _parse_row_pairs(tr):
                if   L == "First Name":   data["First Name"] = V
                elif L == "Last Name":    data["Last Name"]  = V
                elif L == "Street 1":     data["Street 1"]   = V
                elif L == "Street 2":     data["Street 2"]   = V
                elif L == "City, State":  data["City, State"] = V
                elif "ZIP" in L:          data["ZIP Code"]   = V
                elif L == "County":       data["County"]     = V
                elif L == "Township":     data["Township"]   = V
                elif L == "Type":         data["Type"]       = V
                elif L == "Class":        data["Class"]      = V

    # b) Last case history on appeal page
    if len(hdr_tbls) > 1:
        rows = hdr_tbls[1].find_all("tr")[1:]
        if rows:
            last = rows[-1].find_all("td")
            if len(last) >= 3:
                data["Transaction Date"]        = last[0].get_text(strip=True)
                data["Transaction Description"] = last[1].get_text(strip=True)
                data["Received/Letter Date"]    = last[2].get_text(strip=True)

    # c) Appellant attorney info (own table with <h3>)
    atty_table = None
    for tbl in appeal_soup.find_all("table"):
        h3 = tbl.find("h3")
        if h3 and "Appellant Attorney Information" in h3.get_text(strip=True):
            atty_table = tbl
            break
    if atty_table:
        for tr in atty_table.find_all("tr")[1:]:
            for L, V in _parse_row_pairs(tr):
                if   L == "First Name": data["Attorney First Name"] = V
                elif L == "Last Name":  data["Attorney Last Name"]  = V
                elif L == "Firm Name":  data["Attorney Firm Name"]  = V
                elif L == "Street 1":   data["Attorney Street 1"]   = V
                elif L == "Street 2":   data["Attorney Street 2"]   = V
                elif L == "City":       data["Attorney City"]       = V
                elif L == "State":      data["Attorney State"]      = V
                elif "ZIP" in L:        data["Attorney ZIP Code"]   = V
                elif L == "Phone":      data["Attorney Phone"]      = V

    # d) Intervenor (follows link from appeal page)
    link = appeal_soup.find("a", href=re.compile(r"intervenor\.asp", re.I))
    if link:
        iv_url  = urljoin(appeal_url, link["href"])
        iv_resp = sess.get(iv_url, timeout=PTAB_TIMEOUT)
        iv_soup = BeautifulSoup(iv_resp.text, "html.parser")
        iv_tbls = iv_soup.find_all("table")

        if iv_tbls:
            # Intervenor info
            for tr in iv_tbls[0].find_all("tr")[1:]:
                for L, V in _parse_row_pairs(tr):
                    if   L == "Name":     data["Intervenor Name"] = V
                    elif L == "Street 1": data["Intervenor Street 1"] = V
                    elif L == "Street 2": data["Intervenor Street 2"] = V
                    elif L == "City, State, Zip":
                        # format: "City, ST 60606"
                        city, rest = V.split(",", 1)
                        data["Intervenor City"] = city.strip()
                        st, zp = rest.strip().split(None, 1)
                        data["Intervenor State"]    = st
                        data["Intervenor ZIP Code"] = zp
                    elif L == "Phone":     data["Intervenor Phone"] = V
                    elif L == "Owner":     data["Intervenor Owner"] = V
                    elif L == "Confirmed": data["Intervenor Confirmed"] = V
                    elif L == "Resolution Required": data["Intervenor Resolution Required"] = V
                    elif L == "Resolution Received": data["Intervenor Resolution Received"] = V

        if len(iv_tbls) > 1:
            # Intervenor attorney
            for tr in iv_tbls[1].find_all("tr")[1:]:
                for L, V in _parse_row_pairs(tr):
                    if   L == "First Name": data["Int Attorney First Name"] = V
                    elif L == "Last Name":  data["Int Attorney Last Name"]  = V
                    elif L == "Firm Name":  data["Int Attorney Firm Name"]  = V
                    elif L == "Street 1":   data["Int Attorney Street 1"]   = V
                    elif L == "Street 2":   data["Int Attorney Street 2"]   = V
                    elif L == "City, State, Zip":
                        city, rest = V.split(",", 1)
                        data["Int Attorney City"] = city.strip()
                        st, zp = rest.strip().split(None, 1)
                        data["Int Attorney State"]    = st
                        data["Int Attorney ZIP Code"] = zp
                    elif L == "Phone":        data["Int Attorney Phone"]      = V

        if len(iv_tbls) > 2:
            # Intervenor history (last row)
            hist = iv_tbls[2].find_all("tr")[1:]
            if hist:
                last = hist[-1].find_all("td")
                if len(last) >= 3:
                    data["Int Hist Txn Date"]      = last[0].get_text(strip=True)
                    data["Int Hist Description"]   = last[1].get_text(strip=True)
                    data["Int Hist Received Date"] = last[2].get_text(strip=True)

    # e) Property details sections on detail page
    sections = {
        "Board of Review": {
            "Land":"Board of Review Land","Improvements":"Board of Review Improvements",
            "Farm Land":"Board of Review Farm Land","Farm Building":"Board of Review Farm Building",
            "BOR Total":"Board of Review Total"},
        "Appellant": {
            "Land":"Appellant Land","Improvements":"Appellant Improvements",
            "Farm Land":"Appellant Farm Land","Farm Building":"Appellant Farm Building",
            "Appellant Total":"Appellant Total"},
        "PTAB Assessed Value": {
            "Land":"PTAB Land","Improvements":"PTAB Improvements",
            "Farm Land":"PTAB Farm Land","Farm Building":"PTAB Farm Building",
            "PTAB Total":"PTAB Total"},
        "PTAB Information": {
            "County Status":"County Status","Decision Type":"Decision Type",
            "Close Date":"Close Date","Reason Closed":"Reason Closed",
            "Hearing Status":"Hearing Status"},
    }
    for title, mapping in sections.items():
        h3 = next((h for h in p_soup.find_all("h3") if title.lower() in h.get_text(strip=True).lower()), None)
        if not h3:
            continue
        tbl = h3.find_parent("table")
        for tr in tbl.find_all("tr")[1:]:
            for L, V in _parse_row_pairs(tr):
                if L in mapping:
                    data[mapping[L]] = V

    # f) Hearing Information section (date/time/site/zoom)
    hearing_h3 = next((h for h in p_soup.find_all("h3") if "Hearing Information" in h.get_text(strip=True)), None)
    if hearing_h3:
        hearing_table = hearing_h3.find_parent("table")
        for tr in hearing_table.find_all("tr")[1:]:
            th = tr.find("th"); td = tr.find("td")
            if not th or not td:
                continue
            label = th.get_text(strip=True).rstrip(":")
            txt = td.get_text(separator="\n", strip=True)
            if "Hearing Date/Time" in label:
                m = re.match(r"(.+?)\s+at\s+(.+)", txt)
                if m:
                    data["Hearing Date"] = m.group(1)
                    data["Hearing Time"] = m.group(2)
                else:
                    data["Hearing Date"] = txt
            elif "Hearing Site/Location" in label:
                a = td.find("a", href=True)
                if a:
                    data["Hearing Site"] = a["href"]
                for line in txt.splitlines():
                    if line.startswith("Meeting ID"):
                        data["Meeting ID"] = line.split(":", 1)[1].strip()
                    elif line.startswith("Meeting Password"):
                        data["Meeting Password"] = line.split(":", 1)[1].strip()

    return data


def _ptab_consolidate_totals(rows: list[dict]) -> list[dict]:
    """
    Adds Consolidated BOR/Appellant/PTAB totals per Docket No across all its PIN rows.
    """
    # Ensure numeric
    for r in rows:
        r.setdefault("Board of Review Total", 0.0)
        r.setdefault("Appellant Total", 0.0)
        r.setdefault("PTAB Total", 0.0)
        r["Board of Review Total"] = _clean_money_to_float(r["Board of Review Total"])
        r["Appellant Total"] = _clean_money_to_float(r["Appellant Total"])
        r["PTAB Total"] = _clean_money_to_float(r["PTAB Total"])

    sums: dict[str, dict[str, float]] = {}
    for r in rows:
        d = r.get("Docket No", "")
        if d not in sums:
            sums[d] = {"BOR": 0.0, "APP": 0.0, "PTAB": 0.0}
        sums[d]["BOR"] += r["Board of Review Total"]
        sums[d]["APP"] += r["Appellant Total"]
        sums[d]["PTAB"] += r["PTAB Total"]

    for r in rows:
        d = r.get("Docket No", "")
        agg = sums.get(d, {"BOR": 0.0, "APP": 0.0, "PTAB": 0.0})
        r["Consolidated BOR Total"] = agg["BOR"]
        r["Consolidated Appellant Total"] = agg["APP"]
        r["Consolidated PTAB Total"] = agg["PTAB"]
    return rows

def _dedupe_dict_rows(rows: list[dict], keys: tuple[str, ...]) -> list[dict]:
    seen = set()
    out = []
    for r in rows:
        k = tuple(r.get(x) for x in keys)
        if k in seen:
            continue
        seen.add(k)
        out.append(r)
    return out


def fetch_ptab_by_pin(
    pin: str,
    years: list[int] | None = None,
    expand_associated: bool = True,
    compute_consolidated: bool = True,
) -> Dict[str, Any]:
    """
    Pulls PTAB results starting from a PIN. If expand_associated=True:
      PIN -> all dockets -> each docket's associated PINs -> full detail rows
    Returns one row per (PIN No × Docket No).
    """
    meta = {"source": "PTAB", "pin": pin, "years": years or [], "expand_associated": expand_associated, "fetched_at": _now_iso()}
    sess = _ptab_session()
    try:
        pin14 = undashed_pin(pin)
        all_rows: list[dict] = []

        # 1) find dockets for this PIN
        dockets = _ptab_get_docket_list_from_pin(sess, pin14)
        if years:
            ys = {int(y) for y in years}
            # full is like 2014-024172 => first 4 digits are the year
            dockets = [(short_, full) for (short_, full) in dockets if _coerce_int(str(full)[:4]) in ys]

        # 2) for each docket, either:
        #    a) expand to all associated pins, or
        #    b) just fetch details for the original PIN (if site includes it)
        for short_d, full_d in dockets:
            if expand_associated:
                assoc_pins = _ptab_get_associated_pins(sess, short_d) or [pin14]
                for p2 in assoc_pins:
                    rows_for_pin = _ptab_extract_details(sess, p2, short_d, full_d)
                    all_rows.append(rows_for_pin)
                    time.sleep(PTAB_WAIT_SECONDS)
            else:
                rows_for_pin = _ptab_extract_details(sess, pin14, short_d, full_d)
                all_rows.append(rows_for_pin)
                time.sleep(PTAB_WAIT_SECONDS)

        # de-dup (PIN No + Docket No)
        all_rows = _dedupe_dict_rows(all_rows, ("PIN No", "Docket No"))

        # consolidated sums
        if compute_consolidated and all_rows:
            all_rows = _ptab_consolidate_totals(all_rows)

        return {
            "_status": "ok",
            "normalized": {"rows": all_rows},
            "_meta": meta,
        }
    except Exception as e:
        return _err(meta, f"fetch_ptab_by_pin failed: {e}")

def fetch_ptab_by_docket(
    docket_no: str,
    expand_associated: bool = True,
    compute_consolidated: bool = True,
) -> Dict[str, Any]:
    """
    Pulls PTAB results starting from a full docket number (e.g., '2019-012345').
    We normalize to short form and proceed similar to by-PIN flow.
    """
    meta = {"source": "PTAB", "docket_no": docket_no, "expand_associated": expand_associated, "fetched_at": _now_iso()}
    sess = _ptab_session()
    try:
        # normalize '2019-012345' -> '19-12345'
        year, num = docket_no.split("-", 1)
        short_d = f"{year[-2:]}-{num.lstrip('0') or '0'}"
        full_d  = docket_no

        all_rows: list[dict] = []
        if expand_associated:
            assoc_pins = _ptab_get_associated_pins(sess, short_d)
            if not assoc_pins:
                assoc_pins = []
        else:
            assoc_pins = []

        if assoc_pins:
            for p2 in assoc_pins:
                all_rows.append(_ptab_extract_details(sess, p2, short_d, full_d))
                time.sleep(PTAB_WAIT_SECONDS)
        else:
            # still try to extract using the docket; PIN may be discovered on the page
            all_rows.append(_ptab_extract_details(sess, "", short_d, full_d))

        all_rows = _dedupe_dict_rows(all_rows, ("PIN No", "Docket No"))

        if compute_consolidated and all_rows:
            all_rows = _ptab_consolidate_totals(all_rows)

        return {
            "_status": "ok",
            "normalized": {"rows": all_rows},
            "_meta": meta,
        }
    except Exception as e:
        return _err(meta, f"fetch_ptab_by_docket failed: {e}")
    

# ======================= CCAO Permits (Socrata: 6yjf-dfxs) =======================
# Docs: https://datacatalog.cookcountyil.gov/resource/6yjf-dfxs
# Uses 14-digit UNDASHED PIN in the "pin" column.

def fetch_ccao_permits(pin: str, year_min: int | None = None, year_max: int | None = None,
                       dataset_id: str = "6yjf-dfxs") -> dict:
    p = undashed_pin(pin)  # 14-digit string, digits only
    # Inline literals (no bind params)
    where_parts = [f"pin = '{p}'"]
    params = {"$order": "year DESC, date_issued DESC"}  # no :p here

    if year_min is not None:
        where_parts.append(f"year >= {int(year_min)}")
    if year_max is not None:
        where_parts.append(f"year <= {int(year_max)}")

    try:
        rows = _socrata_get_cook(dataset_id, {"$where": " AND ".join(where_parts), **params}, fetch_all=True)
        return {"_status": "ok",
                "normalized": {"rows": rows},
                "_meta": {"dataset": dataset_id, "count": len(rows), "pin": p}}
    except RuntimeError as re:
        if str(re) == "SOC_DATA_THROTTLED":
            return {"_status": "throttled", "normalized": {"rows": []},
                    "_meta": {"dataset": dataset_id, "pin": p, "reason": "429 Too Many Requests"}}
        return {"_status": "error", "normalized": {"rows": []},
                "_meta": {"dataset": dataset_id, "pin": p, "error": str(re)}}
    except requests.HTTPError as e:
        sc   = getattr(e.response, "status_code", None)
        body = getattr(e.response, "text", "")[:800] if getattr(e, "response", None) else ""
        return {"_status": "error", "normalized": {"rows": []},
                "_meta": {"dataset": dataset_id, "pin": p, "status": sc, "error": body or str(e)}}
    except Exception as e:
        return {"_status": "error", "normalized": {"rows": []},
                "_meta": {"dataset": dataset_id, "pin": p, "error": str(e)}}



def fetch_ccao_permits_multi(
    pins: list[str],
    year_min: int | None = None,
    year_max: int | None = None,
    dataset_id: str = "6yjf-dfxs",
) -> dict:
    """
    Fetch ALL columns for MANY pins (<= ~50 recommended). Pins may be dashed/undashed.
    Uses inlined year constraints (no bind params).
    """
    und = []
    for raw in (pins or []):
        up = undashed_pin(raw)
        if up:
            und.append(up)

    incl = _ids_in_clause(und)  # -> ('123...','456...')
    if not incl:
        return {"_status": "ok", "normalized": {"rows": []}, "_meta": {"dataset": dataset_id, "count": 0}}

    # Build WHERE with literals only (no :ymin/:ymax)
    where_parts = [f"pin IN {incl}"]
    if year_min is not None:
        where_parts.append(f"year >= {int(year_min)}")
    if year_max is not None:
        where_parts.append(f"year <= {int(year_max)}")

    params = {"$order": "pin ASC, year DESC, date_issued DESC"}

    try:
        rows = _socrata_get_cook(dataset_id, {"$where": " AND ".join(where_parts), **params}, fetch_all=True)
        return {
            "_status": "ok",
            "normalized": {"rows": rows},
            "_meta": {"dataset": dataset_id, "count": len(rows), "pins": und[:]},
        }
    except RuntimeError as re:
        if str(re) == "SOC_DATA_THROTTLED":
            return {
                "_status": "throttled",
                "normalized": {"rows": []},
                "_meta": {"dataset": dataset_id, "pins": und[:], "reason": "429 Too Many Requests"},
            }
        return {
            "_status": "error",
            "normalized": {"rows": []},
            "_meta": {"dataset": dataset_id, "pins": und[:], "error": str(re)},
        }
    except requests.HTTPError as e:
        sc   = getattr(e.response, "status_code", None)
        body = getattr(e.response, "text", "")[:800] if getattr(e, "response", None) else ""
        return {
            "_status": "error",
            "normalized": {"rows": []},
            "_meta": {"dataset": dataset_id, "pins": und[:], "status": sc, "error": body or str(e)},
        }
    except Exception as e:
        return {
            "_status": "error",
            "normalized": {"rows": []},
            "_meta": {"dataset": dataset_id, "pins": und[:], "error": str(e)},
        }


# ================= Delinquent Taxes (GitHub Contents API) ======================
# fetchers_delinquent.py (drop pandas; pure csv)
import io, os, re, csv, gzip, requests
from functools import lru_cache

GITHUB_TOKEN  = os.getenv("GITHUB_TOKEN", "").strip()
PASSES_REPO   = os.getenv("ASSESSOR_PASSES_REPO", "MktingFLG/assessor-passes-data")
PASSES_BRANCH = os.getenv("ASSESSOR_PASSES_BRANCH", "main")
PASSES_PATH   = os.getenv("ASSESSOR_PASSES_PATH", "delinquencies_master.csv.gz")

API_URL = f"https://api.github.com/repos/{PASSES_REPO}/contents/{PASSES_PATH}?ref={PASSES_BRANCH}"
API_HEADERS = {"Authorization": f"Bearer {GITHUB_TOKEN}", "Accept": "application/vnd.github.raw", "User-Agent": "pin-tool/1.0"}
PIN_COL_CANDIDATES = ("pin","PIN","Pin")

def _digits(s: str) -> str:
    return re.sub(r"\D","", s or "")

@lru_cache(maxsize=1)
def _delinq_index() -> dict[str, dict]:
    if not GITHUB_TOKEN:
        raise RuntimeError("GITHUB_TOKEN not set; cannot access private repo.")
    r = requests.get(API_URL, headers=API_HEADERS, timeout=20)
    if r.status_code == 401: raise RuntimeError("401 Unauthorized: bad/expired token.")
    if r.status_code == 403: raise RuntimeError("403 Forbidden: token lacks contents:read.")
    if r.status_code == 404: raise RuntimeError(f"404 Not Found: {PASSES_BRANCH}/{PASSES_PATH}")
    r.raise_for_status()

    index: dict[str, dict] = {}
    with gzip.GzipFile(fileobj=io.BytesIO(r.content)) as gz:
        reader = csv.DictReader(io.TextIOWrapper(gz, encoding="utf-8", newline=""))
        # Detect pin column
        header = [h.strip() for h in reader.fieldnames or []]
        pin_col = next((c for c in PIN_COL_CANDIDATES if c in header), None)
        if not pin_col:
            raise RuntimeError(f"'pin' column not found in {PASSES_PATH}")

        # Keep *all* columns so we don't drop fields due to header name drift
        for row in reader:
            pin14 = _digits(row.get(pin_col, ""))
            if len(pin14) != 14:
                continue
            # normalize empty strings to None
            cleaned = {k: (v if (v is not None and str(v).strip() != "") else None) for k, v in row.items()}
            index[pin14] = cleaned  # last one wins

    return index

def fetch_delinquent(pin: str):
    try:
        pin14 = _digits(pin)
        if len(pin14) != 14:
            return {"status":"ok","data":[]}
        row = _delinq_index().get(pin14)
        return {"status":"ok","data":[(row | {"pin14": pin14})] if row else []}
    except Exception as e:
        return {"status":"error","error": f"delinquency fetch failed: {e}"}
    
    #===================================================================================

# === Remote Assessor Associated-PIN Index (GitHub JSON) ======================
# File path in the repo: data/_index/key_to_children.json
ASSR_INDEX_REPO   = os.getenv("ASSESSOR_PASSES_REPO", "MktingFLG/assessor-passes-data")
ASSR_INDEX_BRANCH = os.getenv("ASSESSOR_PASSES_BRANCH", "main")
ASSR_INDEX_PATH   = os.getenv("ASSESSOR_INDEX_PATH", "data/_index/key_to_children.json")

ASSR_INDEX_URL = f"https://api.github.com/repos/{ASSR_INDEX_REPO}/contents/{ASSR_INDEX_PATH}?ref={ASSR_INDEX_BRANCH}"
ASSR_INDEX_HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.raw",
    "User-Agent": "pin-tool/1.0",
}

@lru_cache(maxsize=1)
def _remote_assoc_index() -> tuple[dict[str, list[str]], dict[str, str]]:
    """
    Load key_to_children.json from GitHub (if token present), and build:
      key_to_children: { <KEY_PIN14>: [child14, ...] }
      child_to_key:    { <child14>: <KEY_PIN14> }
    Returns ({}, {}) on any failure (caller can fall back to ROD).
    """
    try:
        if not GITHUB_TOKEN:
            return {}, {}
        r = requests.get(ASSR_INDEX_URL, headers=ASSR_INDEX_HEADERS, timeout=25)
        if r.status_code == 401:
            return {}, {}
        r.raise_for_status()
        import json as _json
        data = _json.loads(r.text)  # dict: key_pin -> [child pins]
        key_to_children: dict[str, list[str]] = {}
        child_to_key: dict[str, str] = {}
        for k, arr in (data or {}).items():
            k14 = undashed_pin(k)
            if not k14:
                continue
            kids = []
            for c in arr or []:
                c14 = undashed_pin(c)
                if c14:
                    kids.append(c14)
                    child_to_key[c14] = k14
            key_to_children[k14] = sorted(set(kids))
        return key_to_children, child_to_key
    except Exception:
        return {}, {}

#====================PRC link===================================================

# --- Property Record Card (PRC) link ---
from utils import undashed_pin

def fetch_prc_link(pin: str) -> dict:
    """
    Return the PRC ViewCard URL for an undashed 14-digit PIN.
    No network call; just constructs the link.
    """
    try:
        pin14 = undashed_pin(pin)
        url = f"https://data.cookcountyassessoril.gov/viewcard/viewcard.aspx?pin={pin14}"
        return {
            "_status": "ok",
            "normalized": {"url": url, "pin": pin14},
            "_meta": {"pin": pin14, "host": "data.cookcountyassessoril.gov"}
        }
    except Exception as e:
        return {"_status": "error", "normalized": {}, "_meta": {"error": str(e)}}


def fetch_bor(pin: str, force: bool = False) -> dict:
    # TODO: Replace with your real BOR dataset fetch
    return {"class": None, "building_av": None, "land_av": None, "address": None, "_status": "stub"}

def fetch_cv(pin: str, force: bool = False) -> dict:
    # TODO: Replace with your real Commercial Valuation dataset fetch
    return {"property_use": None, "building_sqft": None, "land_sqft": None,
            "investment_rating": None, "age": None, "address": None, "_status": "stub"}

def fetch_sales(pin: str, force: bool = False) -> dict:
    # TODO: Replace with your real Parcel Sales dataset fetch
    return {"latest": {}, "history": [], "_status": "stub"}

