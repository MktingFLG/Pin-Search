print("🚀 Starting app.py", flush=True)

import os
for k in ("OMP_NUM_THREADS","OPENBLAS_NUM_THREADS","MKL_NUM_THREADS","NUMEXPR_NUM_THREADS"):
    os.environ.setdefault(k, "1")
os.environ.setdefault("PYTHONOPTIMIZE", "1")

import asyncio
from typing import Optional, List
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

from utils import undashed_pin, normalize_pin
import fetchers   
from fetchers import _socrata_get



# ---------------- App & middleware ----------------
app = FastAPI(title="PIN Tool API", version="1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)

# ---------------- Health ----------------
@app.get("/api/health")
def health():
    return {"status": "ok"}

# ---------------- Helpers ----------------
def _must_pin(pin: str) -> str:
    p14 = undashed_pin(pin)
    if not p14:
        raise HTTPException(status_code=400, detail="PIN must be 14 digits.")
    return p14


# ---------------- API used by the frontend ----------------
@app.get("/pin/{pin}/summary")
def pin_summary(pin: str, fresh: bool = False):
    from orchestrator import get_pin_summary   # ✅ lazy import
    _must_pin(pin)
    return get_pin_summary(pin, fresh=fresh)

@app.get("/api/pin/{pin}")
def api_pin(pin: str, fresh: bool = False):
    from orchestrator import get_pin_summary   # ✅ lazy import
    _must_pin(pin)
    return get_pin_summary(pin, fresh=fresh)


# ---------------- Assessor & other source endpoints (unchanged) ----------------
@app.get("/assessor/profile")
def assessor_profile(pin: str, jur: str = "016", taxyr: str = "2025"):
    from fetchers import fetch_assessor_profile
    _must_pin(pin)
    return fetch_assessor_profile(pin, jur, taxyr)


@app.get("/rod/bundle")
def rod_bundle(pin: str, top_n: int = 3):
    _must_pin(pin)
    return fetchers.fetch_recorder_bundle(pin, top_n=top_n)

@app.get("/ptab/by-pin")
def ptab_by_pin(pin: str, years: Optional[List[int]] = Query(default=None)):
    _must_pin(pin)
    return fetchers.fetch_ptab_by_pin(pin, years or [])

@app.get("/ccao/permits")
def ccao_permits(pin: str, year_min: Optional[int] = None, year_max: Optional[int] = None):
    _must_pin(pin)
    return fetchers.fetch_ccao_permits(pin, year_min, year_max)

@app.get("/ptax/main")
def ptax_main(pin: str):
    _must_pin(pin)
    return fetchers.fetch_ptax_main(pin)

@app.get("/delinquent")
def delinquent(pin: str):
    _must_pin(pin)
    return fetchers.fetch_delinquent(pin)

# ---------------- Big parallel bundle (unchanged) ----------------
async def _to_thread(fn, *a, **kw):
    return await asyncio.to_thread(fn, *a, **kw)

@app.get("/pin/{pin}/bundle")
async def everything_bundle(pin: str, jur: str = "016", taxyr: str = "2025", top_n_deeds: int = 3):
    _must_pin(pin)
    tasks = [
        _to_thread(fetchers.fetch_assessor_profile, pin, jur, taxyr),
        _to_thread(fetchers.fetch_assessor_values, pin, jur, taxyr),
        _to_thread(fetchers.fetch_assessor_location, pin, jur, taxyr),
        _to_thread(fetchers.fetch_assessor_land, pin, jur, taxyr),
        _to_thread(fetchers.fetch_assessor_residential, pin, jur, taxyr),
        _to_thread(fetchers.fetch_assessor_other_structures, pin, jur, taxyr),
        _to_thread(fetchers.fetch_assessor_commercial_building, pin, jur, taxyr),
        _to_thread(fetchers.fetch_assessor_exemptions, pin, jur, taxyr),
        _to_thread(fetchers.fetch_assessor_maildetail, pin, jur, taxyr),
        _to_thread(fetchers.fetch_assessor_notice_summary, pin, jur, taxyr),
        _to_thread(fetchers.fetch_assessor_appeals_coes, pin, jur, taxyr),
        _to_thread(fetchers.fetch_permits, pin, jur, taxyr),
        _to_thread(fetchers.fetch_assessor_hie_additions, pin, jur, taxyr),
        _to_thread(fetchers.fetch_recorder_bundle, pin, top_n_deeds),
        _to_thread(fetchers.fetch_ptab_by_pin, pin, []),
        _to_thread(fetchers.fetch_ccao_permits, pin, None, None),
        _to_thread(fetchers.fetch_ptax_main, pin),
        _to_thread(fetchers.fetch_delinquent, pin),
    ]
    async def run_in_batches(tasks, batch_size=4):
        results = []
        for i in range(0, len(tasks), batch_size):
            batch = tasks[i:i+batch_size]
            part = await asyncio.gather(*batch, return_exceptions=True)
            results.extend(part)
        return results

    # run in safer batches of 4
    results = await run_in_batches(tasks, batch_size=4)

    def _norm(x):
        return {"_status": "error", "_meta": {"error": str(x)}, "normalized": {}} if isinstance(x, Exception) else x

    keys = [
        "assessor_profile","assessor_values","assessor_location","assessor_land",
        "assessor_residential","assessor_other_structures","assessor_commercial_building",
        "assessor_exemptions","assessor_maildetail","assessor_notice_summary",
        "assessor_appeals_coes","assessor_permits","assessor_hie_additions",
        "rod_bundle","ptab","ccao_permits","ptax_main","delinquent",
    ]
    return {"_status": "ok", "_meta": {"pin": normalize_pin(pin)}, "bundle": {k: _norm(v) for k, v in zip(keys, results)}}


# ---------------- UI routes ----------------
@app.get("/", include_in_schema=False)
def root():
    # Serve your docs/index.html (adjust path if your HTML lives elsewhere)
    return FileResponse("docs/index.html")



@app.get("/api/prc/{pin}")
def api_prc(pin: str):
    from fetchers import fetch_prc_link   
    _must_pin(pin)
    return fetch_prc_link(pin)


@app.get("/api/nearby-mini/{pin}")
def api_nearby_mini(pin: str, radius: float = Query(5.0, ge=0), limit: int = Query(100, ge=1, le=500)):
    from fetchers import fetch_pin_geom_arcgis, fetch_nearby_candidates, fetch_tax_bill_latest, fetch_ptax_main
    _must_pin(pin)

    subject = fetch_pin_geom_arcgis(pin).get("normalized", {})
    nearby = fetch_nearby_candidates(pin, radius_mi=radius, limit=limit).get("normalized", {})
    tax = fetch_tax_bill_latest(pin).get("normalized", {})
    ptax = fetch_ptax_main(pin).get("normalized", {})

    # inject latest sale into subject + nearby
    def attach_sale(d):
        if not d: return d
        sale = (ptax.get("sales") or [])
        if sale:
            latest = sale[0]  # assume sorted
            d["latest_sale_date"] = latest.get("date")
            d["latest_sale_price"] = latest.get("price")
        return d

    subject = attach_sale(subject)
    for f in (nearby.get("features") or []):
        f = attach_sale(f)

    return {"pin": normalize_pin(pin), "subject": subject, "nearby": nearby, "tax_bill": tax}



@app.get("/api/pin/{pin}/fast")
def api_pin_fast(pin: str):
    from fetchers import fetch_ptax_main, fetch_nearby_candidates
    _must_pin(pin)
    return {
        "pin": normalize_pin(pin),
        "ptax": fetch_ptax_main(pin),
        "nearby": fetch_nearby_candidates(pin, radius_mi=5.0, limit=50),
    }

@app.get("/api/latest-commercial-sales")
def api_latest_commercial_sales(limit: int = 200):
    from fetchers import fetch_assessor_profile
    try:
        rows = _socrata_get("it54-y4c6", {
            "$select": "declaration_id,date_recorded,line_1_primary_pin,line_7_property_advertised,line_13_net_consideration,latitude,longitude",
            "$order": "date_recorded DESC",
            "$limit": str(limit),
        })

        sales = []
        seen = set()
        for r in rows:
            pin = r.get("line_1_primary_pin")
            if not pin or pin in seen:
                continue
            seen.add(pin)

            assessor = {}
            try:
                assessor = fetch_assessor_profile(pin).get("normalized", {})
            except Exception:
                pass

            sales.append({
                "declaration_id": r.get("declaration_id"),
                "date_recorded": r.get("date_recorded"),
                "pin": pin,
                "address": r.get("line_7_property_advertised"),
                "sale_price": r.get("line_13_net_consideration"),
                "class": assessor.get("class"),
                "use_description": assessor.get("property_use"),
                "lat": float(r["latitude"]) if r.get("latitude") else None,
                "lon": float(r["longitude"]) if r.get("longitude") else None,
            })

        return {"_status": "ok", "sales": sales}

    except Exception as e:
        return {"_status": "error", "error": str(e), "sales": []}


