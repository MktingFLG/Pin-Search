# orchestrator.py
from datetime import datetime, timedelta
from typing import Dict, Any
import hashlib
import json

from fetchers import (
    fetch_bor, fetch_cv, fetch_sales, fetch_permits,
    fetch_assessor_values, fetch_assessor_mail_exempt
)
from utils import normalize_pin  # reuse your function if you have one

# Simple in-memory cache (works fine on one dyno). You can swap to Redis later.
_CACHE: Dict[str, Dict[str, Any]] = {}
# Per-source default TTLs (short, to avoid stale data)
TTL = {
    "BOR": timedelta(hours=6),
    "CV": timedelta(hours=12),
    "SALES": timedelta(hours=12),
    "PERMITS": timedelta(hours=24),
    "ASSR_VALUES": timedelta(hours=12),
    "ASSR_MAIL": timedelta(hours=24),
}

def _cache_key(pin: str) -> str:
    return hashlib.sha1(pin.encode("utf-8")).hexdigest()

def get_pin_summary(pin: str, fresh: bool = False) -> Dict[str, Any]:
    pin_norm = normalize_pin(pin) if "normalize_pin" in globals() else pin.strip()
    ck = _cache_key(pin_norm)
    now = datetime.utcnow()

    # Try cache
    if not fresh and ck in _CACHE:
        entry = _CACHE[ck]
        # Still return cached if *all* sources are within TTL; otherwise partial revalidate below
        if all(now - entry["stamps"].get(k, now) <= TTL[k] for k in TTL):
            return entry["data"]

    # load previous if any (for partial reuse)
    prev = _CACHE.get(ck, {"data": {}, "stamps": {}})

    # Revalidate each source independently (ETag/If-Modified-Since inside fetchers)
    bor = fetch_bor(pin_norm, force=fresh or _expired(prev, "BOR", now))
    cv = fetch_cv(pin_norm, force=fresh or _expired(prev, "CV", now))
    sales = fetch_sales(pin_norm, force=fresh or _expired(prev, "SALES", now))
    permits = fetch_permits(pin_norm, force=fresh or _expired(prev, "PERMITS", now))
    assr_vals = fetch_assessor_values(pin_norm, force=fresh or _expired(prev, "ASSR_VALUES", now))
    assr_mail = fetch_assessor_mail_exempt(pin_norm, force=fresh or _expired(prev, "ASSR_MAIL", now))

    # Derived metrics (keep simple; expand as needed)
    derived = _compute_derived(bor, cv, sales)

    # Shape output based on config-driven sections
    payload = {
        "pin": pin_norm,
        "generated_at": now.isoformat() + "Z",
        "sections": {
            "property_info": _shape_property_info(bor, cv, assr_mail),
            "assessed_market_values": _shape_assessed(assr_vals),
            "sales": _shape_sales(sales, cv),
            "permits": _shape_permits(permits),
            "nearby": _shape_nearby(bor, cv),  # placeholder: you can wire your logic
            "links": _shape_links(pin_norm),
        },
        "derived": derived,
        "source_status": {
            "BOR": bor.get("_status", "ok"),
            "CV": cv.get("_status", "ok"),
            "SALES": sales.get("_status", "ok"),
            "PERMITS": permits.get("_status", "ok"),
            "ASSR_VALUES": assr_vals.get("_status", "ok"),
            "ASSR_MAIL": assr_mail.get("_status", "ok"),
        }
    }

    _CACHE[ck] = {
        "data": payload,
        "stamps": {k: now for k in TTL}
    }
    return payload

def _expired(prev, key: str, now) -> bool:
    ts = prev["stamps"].get(key)
    return not ts or (now - ts) > TTL[key]

def _compute_derived(bor, cv, sales) -> Dict[str, Any]:
    # Example: compute rate per sqft or sales $/sqft when possible
    out = {}
    try:
        bldg_sqft = cv.get("building_sqft") or 0
        land_sqft = cv.get("land_sqft") or 0
        bldg_av = bor.get("building_av") or 0
        land_av = bor.get("land_av") or 0
        if bldg_sqft and bldg_av:
            out["rate_per_sqft"] = round(bldg_av / bldg_sqft, 2)
        elif land_sqft and land_av:
            out["rate_per_sqft"] = round(land_av / land_sqft, 2)
        sale_price = sales.get("latest", {}).get("price")
        if sale_price and (bldg_sqft or land_sqft):
            denom = bldg_sqft or land_sqft
            out["sale_price_per_sqft"] = round(sale_price / denom, 2)
    except Exception:
        pass
    return out

def _shape_property_info(bor, cv, assr_mail):
    return {
        "Class": bor.get("class"),
        "Property Use": cv.get("property_use"),
        "Address": cv.get("address") or bor.get("address"),
        "Building SqFt": cv.get("building_sqft"),
        "Land SqFt": cv.get("land_sqft"),
        "Investment Rating": cv.get("investment_rating"),
        "Age": cv.get("age"),
        "Taxpayer Name": assr_mail.get("taxpayer"),
        "Mailing Address": assr_mail.get("mailing_address"),
    }

def _shape_assessed(assr_vals):
    return {"values_summary": assr_vals.get("summary", [])[:3]}

def _shape_sales(sales, cv):
    latest = sales.get("latest") or {}
    return {
        "Latest Sale Date": latest.get("date"),
        "Latest Sale Price": latest.get("price"),
        "Notes": latest.get("notes"),
        "History": sales.get("history", [])[:5],
    }

def _shape_permits(permits):
    return {"rows": permits.get("rows", [])[:25]}

def _shape_nearby(bor, cv):
    # stub; wire your class+distance logic here later
    return {"rows": []}

def _shape_links(pin_norm):
    return {
        "CookViewer": f"https://maps.cookcountyil.gov/cookviewer/?search={pin_norm}",
        "PRC": f"https://data.cookcountyassessor.com/ (requires login)",
    }
