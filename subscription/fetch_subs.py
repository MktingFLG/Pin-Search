#fetch_subs.py
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
from utils import undashed_pin  # strict 14-digit or raises

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
    vs_el  = soup.select_one("#__VIEWSTATE")
    ev_el  = soup.select_one("#__EVENTVALIDATION")
    vsg_el = soup.select_one("#__VIEWSTATEGENERATOR")
    if not (vs_el and ev_el and vsg_el):
        raise RuntimeError("Subscription page missing VIEWSTATE/EVENTVALIDATION tokens")
    vs, ev, vsg = vs_el.get("value",""), ev_el.get("value",""), vsg_el.get("value","")

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
    last = None
    for _ in range(3):
        r = sess.post(f"{BASE}/Downloads.aspx", data=data, timeout=120)
        last = r
        if r.status_code == 200 and r.headers.get("Content-Type","").lower().startswith(("application/zip","application/octet-stream")):
            return r.content
    if last is not None:
        last.raise_for_status()
    raise RuntimeError("Failed to download ZIP (no ZIP content-type)")

def sha256(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def unzip_header_detail(zip_bytes: bytes) -> Tuple[pd.DataFrame, pd.DataFrame]:
    zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    names = zf.namelist()
    header_name = next((n for n in names if re.search(r"(?:^|/)head|header", n, re.I)), None)
    detail_name = next((n for n in names if re.search(r"(?:^|/)det|detail",  n, re.I)), None)
    if not (header_name and detail_name):
        raise RuntimeError(f"ZIP missing header/detail files: {names}")

    df_head_raw = pd.read_csv(zf.open(header_name), dtype=str, low_memory=False)
    df_det_raw  = pd.read_csv(zf.open(detail_name),  dtype=str, low_memory=False)

    # Column detection (tolerant to casing/variants)
    def pick(colnames, *patterns):
        for c in colnames:
            txt = str(c or "").strip()
            if any(re.search(p, txt, re.I) for p in patterns):
                return c
        return None

    head_key = pick(df_head_raw.columns, r"^key.*pin$", r"^head.*pin$", r"^dt_key_pin$", r"key\s*pin")
    if not head_key:
        # fall back: first column
        head_key = df_head_raw.columns[0]

    det_key  = pick(df_det_raw.columns, r"^key.*pin$", r"^dt_key_pin$", r"key\s*pin")
    det_pin  = pick(df_det_raw.columns,  r"^pin$", r"^dt_pin$", r"pin\s*#?", r"^child.*pin$")
    if not (det_key and det_pin):
        raise RuntimeError("Detail CSV missing key/pin columns")

    # Normalize to strict 14-digit undashed, drop invalid rows
    def norm14(s: str) -> str:
        s = re.sub(r"\D", "", str(s or ""))
        return s if len(s) == 14 else ""

    df_head = df_head_raw.copy()
    df_head["__key_pin"] = df_head_raw[head_key].map(norm14)
    df_head["__raw"]     = df_head_raw.to_dict(orient="records")
    df_head = df_head[df_head["__key_pin"] != ""]

    df_det = df_det_raw.copy()
    df_det["__key_pin"] = df_det_raw[det_key].map(norm14)
    df_det["__pin"]     = df_det_raw[det_pin].map(norm14)
    df_det["__raw"]     = df_det_raw.to_dict(orient="records")
    df_det = df_det[(df_det["__key_pin"] != "") & (df_det["__pin"] != "")]

    # De-duplicate to keep DB upserts lean
    df_head = df_head.drop_duplicates(subset=["__key_pin"])
    df_det  = df_det.drop_duplicates(subset=["__pin"])
    return df_head, df_det
