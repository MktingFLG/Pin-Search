# app.py
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pathlib import Path
import os

from orchestrator import get_pin_summary

print("ILLINOIS_APP_TOKEN set:", bool(os.getenv("ILLINOIS_APP_TOKEN")))

app = FastAPI(title="PIN Tool API", version="0.1.0")

# CORS (tighten later)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["*"],
)

APP_VERSION = "2025-08-14-01"

@app.get("/api/health")
def health():
    return {"status": "ok", "version": APP_VERSION}

@app.get("/api/pin/{pin}")
def pin_summary(pin: str, fresh: int = Query(0, ge=0, le=1)):
    """
    fresh=1 → bypass cache (always revalidate sources)
    fresh=0 → use smart revalidation and TTLs
    """
    data = get_pin_summary(pin, fresh=bool(fresh))
    return JSONResponse(data)

# ---- Static docs (served at "/") ----
STATIC_DIR = Path(__file__).parent / "docs"
print("Serving docs from:", STATIC_DIR.resolve(), "exists:", STATIC_DIR.exists())
app.mount("/", StaticFiles(directory=str(STATIC_DIR), html=True), name="static")

# ---- Subscription: manifest & associations ----
import psycopg2
import psycopg2.extras

def _db():
    return psycopg2.connect(os.environ["DATABASE_URL"])

@app.get("/subs/manifest")
def subs_manifest():
    sql = """
      SELECT town_code, pass, last_update, head_pins_count, detail_pins_count
      FROM assessor_manifest
      ORDER BY town_code
    """
    with _db() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(sql)
        rows = cur.fetchall()
        return [dict(r) for r in rows]

def normalize_pin(pin: str) -> str:
    only = "".join(ch for ch in pin if ch.isdigit())
    return only.zfill(14)[:14]

@app.get("/subs/associations/{pin}")
def get_associations(pin: str):
    p = normalize_pin(pin)
    with _db() as conn, conn.cursor() as cur:
        # 1) If pin appears as an associated child, resolve its key
        cur.execute("SELECT key_pin FROM assessor_detail_v22 WHERE pin=%s", (p,))
        row = cur.fetchone()
        if row:
            key = row[0]
        else:
            # 2) Maybe it is the key itself
            cur.execute("SELECT key_pin FROM assessor_header_v22 WHERE key_pin=%s", (p,))
            row2 = cur.fetchone()
            if not row2:
                raise HTTPException(status_code=404, detail="PIN not found in subscription tables")
            key = row2[0]

        # 3) Pull the full group
        cur.execute("SELECT pin FROM assessor_detail_v22 WHERE key_pin=%s ORDER BY pin", (key,))
        assoc = [r[0] for r in cur.fetchall()]
        return {"key_pin": key, "associated": assoc}
