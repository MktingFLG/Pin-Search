# app.py
import os, re, json
from pathlib import Path
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from utils import normalize_pin, undashed_pin

from assessor_assoc import get_associated_pins


print("ILLINOIS_APP_TOKEN set:", bool(os.getenv("ILLINOIS_APP_TOKEN")))
APP_VERSION = "2025-08-14-01"

app = FastAPI(title="PIN Tool API", version="0.1.0", docs_url="/swagger", redoc_url=None)

app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("ALLOWED_ORIGINS", "*").split(","),
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["*"],
)

@app.get("/api/health")
def health():
    return {"status": "ok", "version": APP_VERSION}

@app.get("/api/pin/{pin}")
def pin_summary(pin: str, fresh: int = Query(0, ge=0, le=1)):
    try:
        pin_dash = normalize_pin(pin)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    try:
        # LAZY import here
        from orchestrator import get_pin_summary
        data = get_pin_summary(pin_dash, fresh=bool(fresh))
        return JSONResponse(data)
    except Exception as e:
        # log the actual error for debugging
        print("pin_summary error:", repr(e))
        raise HTTPException(status_code=502, detail="Failed to assemble PIN summary")

@app.get("/ptab/pin/{pin}")
def ptab_pin(pin: str, years: str | None = None):
    try:
        year_list = [int(y) for y in years.split(",")] if years else None
        # LAZY import here
        from fetchers import fetch_ptab_by_pin
        res = fetch_ptab_by_pin(pin, years=year_list, expand_associated=True)
        return JSONResponse(content=res)
    except Exception as e:
        print("ptab_pin error:", repr(e))
        raise HTTPException(status_code=500, detail=f"Failed to fetch PTAB data: {e}")

@app.get("/permits/{pin}")
def api_ccao_permits(pin: str, year_min: int | None = None, year_max: int | None = None):
    try:
        # LAZY import here
        from fetchers import fetch_ccao_permits
        res = fetch_ccao_permits(pin, year_min=year_min, year_max=year_max)
        if res.get("_status") != "ok":
            raise HTTPException(status_code=502, detail="Permit fetch failed")
        return JSONResponse(res)
    except Exception as e:
        print("permits error:", repr(e))
        raise

@app.get("/api/ping")
def api_ping():
    return {"ok": True}


@app.get("/subs/associations/{pin}")
def get_associations(pin: str):
    p14 = undashed_pin(pin)
    group = get_associated_pins(p14)
    if not group:
        raise HTTPException(status_code=404, detail="PIN not found in local index")
    key = group[0]
    assoc = sorted(set(group[1:]))
    return {"key_pin": key, "associated": assoc}

@app.get("/subs/manifest")
def subs_manifest():
    mpath = Path(__file__).parent / "data" / "manifest.json"
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

# Static site (avoid clobbering Swagger /docs)
STATIC_DIR = Path(__file__).parent / "docs"
if STATIC_DIR.exists():
    app.mount("/site", StaticFiles(directory=str(STATIC_DIR), html=True), name="static-docs")

# --- SINGLE root route: send users to Swagger ---
@app.get("/")
def root():
    return RedirectResponse(url="/swagger")
