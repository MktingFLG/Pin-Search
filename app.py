# app.py
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pathlib import Path
import os
from utils import normalize_pin, undashed_pin  # canonical helpers

from orchestrator import get_pin_summary
import json
from utils import undashed_pin
from assessor_assoc import get_associated_pins


print("ILLINOIS_APP_TOKEN set:", bool(os.getenv("ILLINOIS_APP_TOKEN")))

app = FastAPI(title="PIN Tool API", version="0.1.0")

# CORS (tighten later)
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("ALLOWED_ORIGINS", "*").split(","),
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
    try:
        # validate strictly; normalize once
        pin_dash = normalize_pin(pin)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    try:
        data = get_pin_summary(pin_dash, fresh=bool(fresh))
        return JSONResponse(data)
    except Exception as e:
        # avoid leaking internals
        raise HTTPException(status_code=502, detail="Failed to assemble PIN summary")

# ---- Static docs (served at "/") ----
STATIC_DIR = Path(__file__).parent / "docs"
print("Serving docs from:", STATIC_DIR.resolve(), "exists:", STATIC_DIR.exists())
if STATIC_DIR.exists():
    app.mount("/", StaticFiles(directory=str(STATIC_DIR), html=True), name="static")
else:
    print("Docs directory missing; static mount skipped.")


@app.get("/subs/manifest")
def subs_manifest():
    mpath = Path(__file__).parent / "data" / "manifest.json"
    # If you use ASSESSOR_DATA_DIR:
    # mpath = (Path(os.getenv("ASSESSOR_DATA_DIR")) / "manifest.json") if os.getenv("ASSESSOR_DATA_DIR") else mpath
    if not mpath.exists():
        return []
    try:
        data = json.loads(mpath.read_text(encoding="utf-8"))
    except Exception:
        return []
    out = []
    for town_code, row in (data.get("towns") or {}).items():
        out.append({
            "town_code": int(town_code),
            "pass": None,
            "last_update": row.get("last_update"),
            "head_pins_count": row.get("head_pins"),
            "detail_pins_count": row.get("detail_pins"),
        })
    out.sort(key=lambda r: r["town_code"])
    return out



@app.get("/subs/associations/{pin}")
def get_associations(pin: str):
    p14 = undashed_pin(pin)
    group = get_associated_pins(p14)  # returns [key, *children] (undashed 14)
    if not group:
        raise HTTPException(status_code=404, detail="PIN not found in local index")
    key = group[0]
    assoc = sorted(set(group[1:]))
    return {"key_pin": key, "associated": assoc}

