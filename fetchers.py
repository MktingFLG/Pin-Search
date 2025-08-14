# fetchers.py
"""
Thin adapters for each data source.
Replace stubs with your actual BOR, CV, Sales, Assessor scraping, and Permits functions.
"""

# --- Assessor Profile (pull everything) ---
import time
from datetime import datetime
from pathlib import Path
import requests
from bs4 import BeautifulSoup
from utils import undashed_pin  # we need the 14-digit no-dash for the URL

# --- S3 toggleable saving of raw HTML ----------------------------------------
import os
from pathlib import Path

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
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=25)
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
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=25)
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
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=25)
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
        r = requests.get(url_values, headers={"User-Agent": "Mozilla/5.0"}, timeout=25)
        raw_rec_val = save_raw_text(f"values/{pin14}_{jur}_{taxyr}_cur.html", r.text)
        out["raw_values"] = {**raw_rec_val, "html_size_bytes": len(r.text)}


        # For the summary page:
        # After r = requests.get(url_summary, ...)
        # For the summary page (mirror the new save helper)
        sum_rec = save_raw_text(f"values/{pin14}_{jur}_{taxyr}_summary.html", r.text)
        out["raw_value_summary"] = {**sum_rec, "html_size_bytes": len(r.text)}

        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

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
        r = requests.get(url_summary, headers={"User-Agent": "Mozilla/5.0"}, timeout=25)
        r.raise_for_status()
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
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=25)
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
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=25)
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
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=25)
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
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=25)
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
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=25)
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
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=25)
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
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=25)
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
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=25)
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
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=25)
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
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=25)
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
    import time
    t0 = time.time()
    pin14 = undashed_pin(pin)
    url = f"{PROFILE_BASE}?mode=res_addn&UseSearch=no&pin={pin14}&jur={jur}&taxyr={taxyr}&LMparent=896"
    meta = {"source":"ASSR_HIE_ADDN","url":url,"pin":pin14,"jur":jur,"taxyr":taxyr,"fetched_at":_now_iso()}
    try:
        r = requests.get(url, headers={"User-Agent":"Mozilla/5.0"}, timeout=25)
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
SOCRATA_BASE = "https://data.illinois.gov/resource"
APP_TOKEN = os.getenv("ILLINOIS_APP_TOKEN", "")  # Set this in Render → Environment

def _socrata_get(dataset_id: str, params: dict) -> list:
    """
    Generic Socrata GET with paging. Returns list[dict].
    """
    headers = {"User-Agent": "PIN-Tool/1.0"}
    if APP_TOKEN:
        headers["X-App-Token"] = APP_TOKEN

    out = []
    limit = int(params.get("$limit", 5000))
    offset = 0
    while True:
        q = {**params, "$limit": str(limit), "$offset": str(offset)}
        url = f"{SOCRATA_BASE}/{dataset_id}.json"
        r = requests.get(url, headers=headers, params=q, timeout=30)
        r.raise_for_status()
        batch = r.json()
        if not batch:
            break
        out.extend(batch)
        if len(batch) < limit:
            break
        offset += limit
        time.sleep(0.15)  # polite throttle
    return out

def _pin_for_socrata(pin: str) -> str:
    """
    Most PTAX tables use dashed 'pin'. We search by both dashed & undashed.
    """
    d = undashed_pin(pin)  # "12345678901234"
    return f"{d[0:2]}-{d[2:4]}-{d[4:7]}-{d[7:10]}-{d[10:14]}"

def fetch_ptax_main(pin: str, dataset_id: str = "it54-y4c6") -> dict:
    dashed = _pin_for_socrata(pin)
    rows = _socrata_get(dataset_id, {
        # match both versions of the PIN
        "$where": "pin = @p OR replace(pin,'-','') = @p_nodash",
        "$order": "date_recorded DESC",
        "$limit": "5000",
        "@p": dashed,
        "@p_nodash": dashed.replace("-", ""),
        # You can also add a $select with your exact columns if you want to trim
    })
    return {"_status": "ok", "normalized": {"rows": rows}, "_meta": {"dataset": dataset_id, "count": len(rows)}}


def fetch_ptax_additional_buyers(pin: str, dataset_id: str = "dwt7-rycp") -> dict:
    dashed = _pin_for_socrata(pin)
    # This table only has declaration_id + buyer_name per your query,
    # but it usually relates by declaration_id to main table. We still filter by pin when present.
    rows = _socrata_get(dataset_id, {
        "$where": "pin = @p OR replace(pin,'-','') = @p_nodash OR pin IS NULL",
        "$order": "buyer_name ASC",
        "$limit": "5000",
        "@p": dashed,
        "@p_nodash": dashed.replace("-", ""),
    })
    return {"_status": "ok", "normalized": {"rows": rows}, "_meta": {"dataset": dataset_id, "count": len(rows)}}

def fetch_ptax_additional_sellers(pin: str, dataset_id: str = "rzbz-mw8b") -> dict:
    dashed = _pin_for_socrata(pin)
    rows = _socrata_get(dataset_id, {
        "$where": "pin = @p OR replace(pin,'-','') = @p_nodash OR pin IS NULL",
        "$order": "seller_name ASC",
        "$limit": "5000",
        "@p": dashed,
        "@p_nodash": dashed.replace("-", ""),
    })
    return {"_status": "ok", "normalized": {"rows": rows}, "_meta": {"dataset": dataset_id, "count": len(rows)}}

def fetch_ptax_additional_pins(pin: str, dataset_id: str = "ay2h-5hx3") -> dict:
    dashed = _pin_for_socrata(pin)
    rows = _socrata_get(dataset_id, {
        "$where": "pin = @p OR replace(pin,'-','') = @p_nodash",
        "$order": "pin ASC",
        "$limit": "5000",
        "@p": dashed,
        "@p_nodash": dashed.replace("-", ""),
    })
    return {"_status": "ok", "normalized": {"rows": rows}, "_meta": {"dataset": dataset_id, "count": len(rows)}}

def fetch_ptax_personal_property(pin: str, dataset_id: str = "b46z-jwev") -> dict:
    dashed = _pin_for_socrata(pin)
    rows = _socrata_get(dataset_id, {
        "$where": "pin = @p OR replace(pin,'-','') = @p_nodash",
        "$order": "declaration_id DESC",
        "$limit": "5000",
        "@p": dashed,
        "@p_nodash": dashed.replace("-", ""),
    })
    return {"_status": "ok", "normalized": {"rows": rows}, "_meta": {"dataset": dataset_id, "count": len(rows)}}


# =================================================================


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



