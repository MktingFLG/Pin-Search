#refresh_all.py
"""
subscription/refresh_all.py
- Scheduled job entrypoint (Render worker/cron)
- Detects updated township rows, downloads ZIPs, imports header/detail, updates manifest
"""

import os
import sys
from datetime import datetime

import pandas as pd

from subscription.fetch_subs import (
    new_session, ensure_logged_in, get_grid_and_tokens,
    download_town_zip, unzip_header_detail, sha256
)
from subscription.import_subs import (
    ensure_schema, upsert_manifest, town_needs_update,
    bulk_upsert_header, bulk_upsert_detail
)

LAYOUT_VERSION = 22

def main():
    ensure_schema()

    sess = new_session()
    ensure_logged_in(sess)
    tokens, rows = get_grid_and_tokens(sess)

    # Parse last_update strings to YYYY-MM-DD for DB
    for r in rows:
        try:
            r["last_update"] = pd.to_datetime(r["last_update"]).date().isoformat()
        except Exception:
            r["last_update"] = None

    # Process each township if update detected
    for r in rows:
        if not r["last_update"]:
            continue
        if not town_needs_update(r):
            continue

        # Re-fetch tokens just before download to keep VIEWSTATE valid
        tokens, _ = get_grid_and_tokens(sess)
        print(f"⏬ Downloading town {r['town_code']} ({r['township']}) …", flush=True)
        try:
            tokens, _ = get_grid_and_tokens(sess)
            zip_bytes = download_town_zip(sess, tokens, r["ctrl_name"])
        except Exception as e:
            print(f"❌ Download failed for town {r['town_code']}: {e}", flush=True)
            continue
        digest = sha256(zip_bytes)
        # Optional: if manifest already has this hash, skip (in case last_update date didn’t change)
        # (You can keep town_needs_update as-is; this is a belt-and-suspenders guard)

        print("🔓 Unzipping …", flush=True)
        try:
            df_head, df_det = unzip_header_detail(zip_bytes)
        except Exception as e:
            print(f"❌ Unzip/parse failed for town {r['town_code']}: {e}", flush=True)
            continue

        print("📥 Importing header/detail …", flush=True)
        bulk_upsert_header(df_head, r["town_code"])
        bulk_upsert_detail(df_det,  r["town_code"])

        upsert_manifest(r, LAYOUT_VERSION, digest)
        print(f"✅ Imported town {r['town_code']} – last_update {r['last_update']}", flush=True)

    print("Done.")

if __name__ == "__main__":
    sys.exit(main())
