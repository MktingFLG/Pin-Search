#import_subs.py
"""
subscription/import_subs.py
- DB utilities to upsert manifest, header, and detail tables
"""

import os
import json
import psycopg2
import psycopg2.extras
from typing import Dict
import pandas as pd
from datetime import datetime

SCHEMA_FILE = os.path.join(os.path.dirname(__file__), "schema.sql")

def get_conn():
    dsn = os.environ["DATABASE_URL"]  # Render provides this
    return psycopg2.connect(dsn)

def ensure_schema():
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SET LOCAL statement_timeout = '15s';")
        with open(SCHEMA_FILE, "r", encoding="utf-8") as f:
            cur.execute(f.read())
        conn.commit()

def upsert_manifest(row: Dict, layout_version: int, zip_sha256: str):
    sql = """
    INSERT INTO assessor_manifest (town_code, pass, last_update, head_pins_count, detail_pins_count, layout_version, zip_sha256)
    VALUES (%(town_code)s, %(pass_no)s, to_date(%(last_date)s, 'YYYY-MM-DD'), %(head_cnt)s, %(detail_cnt)s, %(layout_version)s, %(zip_sha256)s)
    ON CONFLICT (town_code) DO UPDATE SET
      pass              = EXCLUDED.pass,
      last_update       = EXCLUDED.last_update,
      head_pins_count   = EXCLUDED.head_pins_count,
      detail_pins_count = EXCLUDED.detail_pins_count,
      layout_version    = EXCLUDED.layout_version,
      zip_sha256        = EXCLUDED.zip_sha256,
      imported_at       = now();
    """
    params = {
        "town_code": row["town_code"],
        "pass_no": row["pass_no"],
        "last_date": row["last_update"],
        "head_cnt": row["head_cnt"],
        "detail_cnt": row["detail_cnt"],
        "layout_version": layout_version,
        "zip_sha256": zip_sha256,
    }
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        conn.commit()

def town_needs_update(row: Dict) -> bool:
    sql = "SELECT last_update, zip_sha256 FROM assessor_manifest WHERE town_code=%s"
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (row["town_code"],))
        hit = cur.fetchone()
    if not hit:
        return True
    last_update, old_hash = hit
    # If the date string changed or the file hash will differ, we’ll update.
    # At this check stage we only know the date; hash is verified post-download.
    return str(last_update) != row["last_update"]

def bulk_upsert_header(df_head: pd.DataFrame, town_code: int):
    if df_head.empty:
        return
    records = []
    for _, r in df_head.iterrows():
        records.append((
            town_code,
            r["__key_pin"],
            r.get("CLASS") or r.get("Class") or None,
            r.get("ADDR") or r.get("Address") or None,
            json.dumps(r["__raw"]),
        ))
    sql = """
    INSERT INTO assessor_header_v22 (town_code, key_pin, class, addr, raw_json)
    VALUES %s
    ON CONFLICT (key_pin) DO UPDATE SET
      town_code = EXCLUDED.town_code,
      class     = EXCLUDED.class,
      addr      = EXCLUDED.addr,
      raw_json  = EXCLUDED.raw_json;
    """
    with get_conn() as conn, conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, records, page_size=1000)
        conn.commit()

def bulk_upsert_detail(df_det: pd.DataFrame, town_code: int):
    if df_det.empty:
        return
    records = []
    for _, r in df_det.iterrows():
        records.append((
            town_code,
            r["__key_pin"],
            r["__pin"],
            r.get("UNIT_NO") or r.get("Unit") or None,
            json.dumps(r["__raw"]),
        ))
    sql = """
    INSERT INTO assessor_detail_v22 (town_code, key_pin, pin, unit_no, raw_json)
    VALUES %s
    ON CONFLICT (pin) DO UPDATE SET
      town_code = EXCLUDED.town_code,
      key_pin   = EXCLUDED.key_pin,
      unit_no   = EXCLUDED.unit_no,
      raw_json  = EXCLUDED.raw_json;
    """
    with get_conn() as conn, conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, records, page_size=2000)
        conn.commit()
