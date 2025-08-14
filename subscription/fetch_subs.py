"""
subscription/fetch_subs.py
- Logs into the CCAO Subscription site
- Reads Downloads.aspx grid (tokens + rows)
- Downloads township ZIPs by simulating the image-button POST
"""

import io
import os
import re
import zipfile
import hashlib
import datetime as dt
from typing import Dict, List, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup

BASE = "https://data.cookcountyassessoril.gov"

# Expect these env vars in Render:
# CCAO_USER, CCAO_PASS  (only if login form is required in your tenant)
# Some subscribers are already authenticated via session; our code tolerates both.

HEADERS = {
    "User-Agent": "Mozilla/5.0 (PINTool/Subscriptions)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

def _normalize_pin_14(pin: str) -> str:
    # fallback: use utils.normalize_pin if present
    only = re.sub(r"[^0-9]", "", pin or "")
    return only.zfill(14)[:14] if only else ""

def new_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s

def ensure_logged_in(sess: requests.Session) -> None:
    # Many orgs land directly on the page if already authenticated.
    r = sess.get(f"{BASE}/Downloads.aspx", timeout=30)
    r.raise_for_status()
    if "Database Subscription Site" in r.text and "__VIEWSTATE" in r.text:
        return
    # If your tenant requires a login step, add it here, e.g.:
    # sess.post(BASE + "/Login.aspx", data={"UserName": os.environ["CCAO_USER"], "Password": os.environ["CCAO_PASS"]})

def get_grid_and_tokens(sess: requests.Session) -> Tuple[Dict[str, str], List[Dict]]:
    r = sess.get(f"{BASE}/Downloads.aspx", timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    vs  = soup.select_one("#__VIEWSTATE")["value"]
    ev  = soup.select_one("#__EVENTVALIDATION")["value"]
    vsg = soup.select_one("#__VIEWSTATEGENERATOR")["value"]

    rows = []
    grid = soup.select_one("#ctl00_content2_gvDataSummary")
    if not grid:
        raise RuntimeError("Could not find DataSummary grid")

    trs = grid.select("tr")[1:]  # skip header
    for tr in trs:
        tds = tr.select("td")
        if len(tds) < 10:
            continue
        township   = tds[0].get_text(strip=True)
        town_code  = int(tds[1].get_text(strip=True))
        pass_no    = int(tds[2].get_text(strip=True))
        head_cnt   = int(tds[3].get_text(strip=True))
        detail_cnt = int(tds[4].get_text(strip=True))
        mail_date  = tds[5].get_text(strip=True) or None
        last_app   = tds[6].get_text(strip=True) or None
        certified  = tds[7].get_text(strip=True) or None
        last_upd   = tds[8].get_text(strip=True) or None
        img        = tds[9].select_one("input[type=image]")
        ctrl_name  = img["name"]  # e.g. ctl00$content2$gvDataSummary$ctl02$ctl00

        rows.append(dict(
            township=township, town_code=town_code, pass_no=pass_no,
            head_cnt=head_cnt, detail_cnt=detail_cnt,
            mail_date=mail_date, last_appeal_date=last_app,
            certified=certified, last_update=last_upd,
            ctrl_name=ctrl_name
        ))
    tokens = {"__VIEWSTATE": vs, "__EVENTVALIDATION": ev, "__VIEWSTATEGENERATOR": vsg}
    return tokens, rows

def download_town_zip(sess: requests.Session, tokens: Dict[str, str], ctrl_name: str) -> bytes:
    # ASP.NET image button requires .x and .y coordinates sent with the control name
    data = {
        "__EVENTTARGET": "",
        "__EVENTARGUMENT": "",
        "__VIEWSTATE": tokens["__VIEWSTATE"],
        "__EVENTVALIDATION": tokens["__EVENTVALIDATION"],
        "__VIEWSTATEGENERATOR": tokens["__VIEWSTATEGENERATOR"],
        f"{ctrl_name}.x": "12",
        f"{ctrl_name}.y": "8",
    }
    r = sess.post(f"{BASE}/Downloads.aspx", data=data, timeout=120)
    r.raise_for_status()
    return r.content

def sha256(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def unzip_header_detail(zip_bytes: bytes) -> Tuple[pd.DataFrame, pd.DataFrame]:
    zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    names = zf.namelist()
    # Guess names; adjust if your ZIP uses different casing/tokens.
    header_name = next(n for n in names if re.search(r"head|header", n, re.I))
    detail_name = next(n for n in names if re.search(r"det|detail", n, re.I))
    df_head  = pd.read_csv(zf.open(header_name), dtype=str, low_memory=False)
    df_det   = pd.read_csv(zf.open(detail_name),  dtype=str, low_memory=False)

    # Canonical columns
    # Try to locate likely fields (these vary, so store raw_json too)
    # Expect at least a key pin in header, a key & child pin in detail.
    for col in df_head.columns:
        if re.search(r"key.*pin|head.*pin", col, re.I):
            key_col = col
            break
    else:
        key_col = df_head.columns[0]

    for col in df_det.columns:
        if re.search(r"key.*pin|head.*pin", col, re.I):
            det_key_col = os
