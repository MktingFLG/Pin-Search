# orchestrator.py
from datetime import datetime, timedelta
from typing import Dict, Any
import hashlib
import json



from datetime import datetime

from utils import normalize_pin, undashed_pin

# orchestrator.py (add near top)
import os

PIN_MINIMAL = os.getenv("PIN_MINIMAL", "0") == "1"
# optional: fine-grained allow-list, e.g. "BOR,CV,DELINQUENT"
PIN_SOURCES = {s.strip().upper() for s in os.getenv("PIN_SOURCES", "").split(",") if s.strip()}

def _enabled(name: str) -> bool:
    if PIN_MINIMAL:
        # only these in minimal mode (tweak as you like)
        return name in {"ASSR_PROFILE", "ASSR_MAILDETAIL", "DELINQUENT"}
    if PIN_SOURCES:
        return name in PIN_SOURCES
    return True  # default: everything on

def _fetch_if(name: str, fn_name: str, *args, **kwargs):
    """Import the fetcher lazily and call it only if the source is enabled."""
    if not _enabled(name):
        return {}
    mod = __import__("fetchers", fromlist=[fn_name])
    fn = getattr(mod, fn_name)
    return fn(*args, **kwargs)



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
    "ASSR_PROFILE": timedelta(hours=24),
    "ASSR_MAILDETAIL": timedelta(hours=24),
    "ASSR_EXEMPT": timedelta(hours=24),
    "ASSR_LOCATION": timedelta(hours=24),
    "ASSR_LAND": timedelta(hours=24),
    "ASSR_RESIDENTIAL": timedelta(hours=24),
    "ASSR_OBY": timedelta(hours=24),
    "ASSR_COMM_BLDG": timedelta(hours=24),
    "ASSR_PROP_ASSOCIATION" : timedelta(hours=24),
    "ASSR_SALES": timedelta(hours=24),
    "ASSR_NOTICE_SUMMARY": timedelta(hours=24), 
    "ASSR_APPEALS": timedelta(hours=24), 
    "ASSR_HIE_ADDN" : timedelta(hours=24),
    "PTAX_MAIN": timedelta(hours=6),
    "PTAX_ADDL_BUYERS": timedelta(hours=12),
    "PTAX_ADDL_SELLERS": timedelta(hours=12),
    "PTAX_ADDL_PINS": timedelta(hours=12),
    "PTAX_PERS": timedelta(hours=12),
    "ROD": timedelta(hours=6),
    "PTAB": timedelta(hours=24), 
    "PERMITS_CCAO": timedelta(hours=24),
    "DELINQUENT": timedelta(hours=24),
    "PRC": timedelta(hours=24),

}

TTL.pop("ASSR_MAIL", None)




def _cache_key(pin: str) -> str:
    return hashlib.sha1(pin.encode("utf-8")).hexdigest()

def get_pin_summary(pin: str, fresh: bool = False) -> Dict[str, Any]:
    try:
        pin_dash = normalize_pin(pin)     # display
        pin_raw  = undashed_pin(pin_dash) # 14-digit for fetchers
    except Exception:
        return {
            "pin": (pin or "").strip(),
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "error": "Invalid PIN: must be 14 digits (Cook County).",
            "sections": {},
            "source_status": {}
        }

    ck = _cache_key(pin_raw)
    now = datetime.utcnow()

    # Try cache
    if not fresh and ck in _CACHE:
        entry = _CACHE[ck]
        # Still return cached if *all* sources are within TTL; otherwise partial revalidate below
        if all(now - entry["stamps"].get(k, now) <= TTL[k] for k in TTL):
            return entry["data"]

    # load previous if any (for partial reuse)
    prev = _CACHE.get(ck, {"data": {}, "stamps": {}})

    # --- Revalidate each source (LAZY + GATED) ---
    bor         = _fetch_if("BOR",               "fetch_bor",               pin_raw, force=fresh or _expired(prev, "BOR", now))
    cv          = _fetch_if("CV",                "fetch_cv",                pin_raw, force=fresh or _expired(prev, "CV", now))
    sales       = _fetch_if("SALES",             "fetch_sales",             pin_raw, force=fresh or _expired(prev, "SALES", now))
    permits     = _fetch_if("PERMITS",           "fetch_permits",           pin_raw, force=fresh or _expired(prev, "PERMITS", now))
    assr_vals   = _fetch_if("ASSR_VALUES",       "fetch_assessor_values",   pin_raw, force=fresh or _expired(prev, "ASSR_VALUES", now))
    assr_profile= _fetch_if("ASSR_PROFILE",      "fetch_assessor_profile",  pin_raw, force=fresh or _expired(prev, "ASSR_PROFILE", now))
    assr_maildetail=_fetch_if("ASSR_MAILDETAIL", "fetch_assessor_maildetail",pin_raw, force=fresh or _expired(prev, "ASSR_MAILDETAIL", now))
    assr_exempt = _fetch_if("ASSR_EXEMPT",       "fetch_assessor_exemptions",pin_raw, force=fresh or _expired(prev, "ASSR_EXEMPT", now))
    assr_location=_fetch_if("ASSR_LOCATION",     "fetch_assessor_location", pin_raw, force=fresh or _expired(prev, "ASSR_LOCATION", now))
    assr_land   = _fetch_if("ASSR_LAND",         "fetch_assessor_land",     pin_raw, force=fresh or _expired(prev, "ASSR_LAND", now))
    assr_res    = _fetch_if("ASSR_RESIDENTIAL",  "fetch_assessor_residential", pin_raw, force=fresh or _expired(prev, "ASSR_RESIDENTIAL", now))
    assr_oby    = _fetch_if("ASSR_OBY",          "fetch_assessor_other_structures", pin_raw, force=fresh or _expired(prev, "ASSR_OBY", now))
    comm_bldg   = _fetch_if("ASSR_COMM_BLDG",    "fetch_assessor_commercial_building", pin_raw, force=fresh or _expired(prev, "ASSR_COMM_BLDG", now))
    prop_assoc  = _fetch_if("ASSR_PROP_ASSOCIATION", "fetch_assessor_prop_association", pin_raw, force=fresh or _expired(prev, "ASSR_PROP_ASSOCIATION", now))
    assr_sales_dalet=_fetch_if("ASSR_SALES",     "fetch_assessor_sales_datalet", pin_raw, force=fresh or _expired(prev, "ASSR_SALES", now))
    notice_sum  = _fetch_if("ASSR_NOTICE_SUMMARY","fetch_assessor_notice_summary", pin_raw, force=fresh or _expired(prev, "ASSR_NOTICE_SUMMARY", now))
    appeals     = _fetch_if("ASSR_APPEALS",      "fetch_assessor_appeals_coes", pin_raw, force=fresh or _expired(prev, "ASSR_APPEALS", now))
    assr_hie    = _fetch_if("ASSR_HIE_ADDN",     "fetch_assessor_hie_additions", pin_raw, force=fresh or _expired(prev, "ASSR_HIE_ADDN", now))
    ptab        = _fetch_if("PTAB",              "fetch_ptab_by_pin",       pin_raw, years=None, expand_associated=True)
    permits_ccao= _fetch_if("PERMITS_CCAO",      "fetch_ccao_permits",      pin_raw) if (fresh or _expired(prev, "PERMITS_CCAO", now)) else prev["data"].get("sections", {}).get("permits_ccao", {})
    delinquent  = _fetch_if("DELINQUENT",        "fetch_delinquent",        pin_raw)
    prc         = _fetch_if("PRC",               "fetch_prc_link",          pin_raw)


    rod = {}
    assoc_und, assoc_pins = [], []
    pins_for_ptax = [pin_raw]
    ptax_main = {}
    ptax_buyers = ptax_sellers = ptax_pins = ptax_personal = {}
    buyers_rows = sellers_rows = addlpins_rows = pp_rows = []
    bundle, ptax_summary_rows = {}, []


    # --- Associated PINs: GitHub index first; ROD fallback if empty ---
    from fetchers import get_assessor_associated_pins, fetch_recorder_bundle

    assoc_und = [p for p in (get_assessor_associated_pins(pin_raw) or [])
                 if isinstance(p, str) and p.isdigit() and len(p) == 14]

    if not assoc_und:
        # Not found (or invalid) in remote index → try Recorder of Deeds
        rod = fetch_recorder_bundle(pin_raw, top_n=1)
        rod_norm = (rod or {}).get("normalized", {}) or {}
        assoc_pins = rod_norm.get("associated_pins_dashed") or []
        assoc_und = [undashed_pin(p) for p in assoc_pins if undashed_pin(p)]
    else:
        # Still fetch ROD once so UI has deeds, but do not change assoc_und
        rod = fetch_recorder_bundle(pin_raw, top_n=1)

    # For UI convenience
    assoc_pins = [normalize_pin(p) for p in assoc_und]  # dashed


    # Build PTAX search input = base + associated (unique, capped at 20)
    pins_for_ptax = [pin_raw] + assoc_und
    seen = set()
    pins_for_ptax = [p for p in pins_for_ptax if not (p in seen or seen.add(p))][:20]

    # --- PTAX main for ALL pins at once ---
    ptax_main = _fetch_if("PTAX_MAIN", "fetch_ptax_main_multi", pins_for_ptax) or {}
    main_rows = (ptax_main.get("normalized", {}) or {}).get("rows") or []

    # --- Cover declarations where our pins appear ONLY as Additional PINs ---
    from fetchers import fetch_ptax_decl_ids_by_addl_pin, fetch_ptax_main_by_declaration_ids

    # 1) Find extra declaration_ids via Additional PINs table
    decl_extra = fetch_ptax_decl_ids_by_addl_pin(pins_for_ptax) or {}
    extra_ids = (decl_extra.get("normalized", {}) or {}).get("declaration_ids", []) or []

    # 2) Add the missing main rows for those declarations
    have_ids = {r.get("declaration_id") for r in main_rows if r.get("declaration_id")}
    missing_ids = sorted(set(extra_ids) - have_ids)

    if missing_ids:
        ptax_main_extra = _fetch_if("PTAX_MAIN", "fetch_ptax_main_by_declaration_ids", missing_ids) or {}
        extra_rows = (ptax_main_extra.get("normalized", {}) or {}).get("rows") or []
        if extra_rows:
            main_rows.extend(extra_rows)


    # Which of our input pins matched each declaration_id?
    from utils import normalize_pin as _norm
    pin_match_map = {}
    for r in main_rows:
        did = r.get("declaration_id")
        ptxt = (r.get("line_1_primary_pin") or "").strip()
        if did and ptxt:
            pin_match_map.setdefault(did, set()).add(_norm(ptxt))

    # --- PTAX secondary tables by declaration_id ---
    decl_ids = [r.get("declaration_id") for r in main_rows if r.get("declaration_id")]
    ptax_buyers   = _fetch_if("PTAX_ADDL_BUYERS",  "fetch_ptax_additional_buyers",  declaration_ids=decl_ids) or {}
    ptax_sellers  = _fetch_if("PTAX_ADDL_SELLERS", "fetch_ptax_additional_sellers", declaration_ids=decl_ids) or {}
    ptax_pins     = _fetch_if("PTAX_ADDL_PINS",    "fetch_ptax_additional_pins",    declaration_ids=decl_ids) or {}
    ptax_personal = _fetch_if("PTAX_PERS",         "fetch_ptax_personal_property",  declaration_ids=decl_ids) or {}

    buyers_rows   = (ptax_buyers.get("normalized", {}) or {}).get("rows")  or []
    sellers_rows  = (ptax_sellers.get("normalized", {}) or {}).get("rows") or []
    addlpins_rows = (ptax_pins.get("normalized", {}) or {}).get("rows")    or []
    pp_rows       = (ptax_personal.get("normalized", {}) or {}).get("rows") or []

    from fetchers import merge_ptax_by_declaration
    bundle, ptax_summary_rows, set_matches = merge_ptax_by_declaration(
        main_rows, buyers_rows, sellers_rows, addlpins_rows, pp_rows, normalize_pin_fn=_norm
    )
    set_matches(pin_match_map)



    # Derived metrics (keep simple; expand as needed)
    derived = _compute_derived(bor, cv, sales)
    
    ptab_rows = (ptab.get("normalized", {}) or {}).get("rows")
    if ptab_rows is None:
        ptab_rows = ptab.get("rows", [])
    # Shape output based on config-driven sections
    payload = {
        "pin": pin_dash,
        "generated_at": now.isoformat() + "Z",
        "sections": {
            "property_info": _shape_property_info(bor, cv, assr_maildetail, assr_profile),
            "assessor_profile": assr_profile.get("normalized", {}),
            "assessor_maildetail": assr_maildetail.get("normalized", {}),
            "assessor_exemptions": assr_exempt.get("normalized", {}),  
            "value_summary": (assr_vals.get("normalized", {}) or {}).get("value_summary_raw", []),
            "property_location": assr_location.get("normalized", {}),
            "land": assr_land.get("normalized", {}),
            "residential_building": assr_res.get("normalized", {}),
            "assessed_market_values": _shape_assessed(assr_vals),
            "other_structures": assr_oby.get("normalized", {}),
            "commercial_building": comm_bldg.get("normalized", {}),
            "divisions_consolidations": prop_assoc.get("normalized", {}),
            "assessor_sales_datalet": assr_sales_dalet.get("normalized", {}), 
            "sales": _shape_sales(sales, cv, assr_sales_dalet.get("normalized", {})),
            "notice_summary": notice_sum.get("normalized", {}),
            "appeals_coes": appeals.get("normalized", {}),
            "permits": _shape_permits(permits),
            "hie_additions": assr_hie.get("normalized", {}),
            "ptax": {
                "base_pin": pin_raw,
                "associated_pins": assoc_pins,
                "pin_search_input": pins_for_ptax,
                "by_declaration_id": bundle,
                "rows": ptax_summary_rows,
            },
            "recorder_of_deeds": (rod or {}).get("normalized", {}),
            "ptab": ptab_rows,
            "permits_ccao": (permits_ccao or {}).get("normalized", {}),
            "nearby": _shape_nearby(bor, cv),
            "links": _shape_links(pin_raw, prc),
            "delinquent": delinquent,
            "prc": (prc or {}).get("normalized", {}),



        },



        "derived": derived,
        "source_status": {
            "BOR": bor.get("_status", "ok"),
            "CV": cv.get("_status", "ok"),
            "SALES": sales.get("_status", "ok"),
            "PERMITS": permits.get("_status", "ok"),
            "ASSR_VALUES": assr_vals.get("_status", "ok"),
            "ASSR_PROFILE": assr_profile.get("_status", "ok"),  # <-- add
            "ASSR_MAILDETAIL": assr_maildetail.get("_status", "ok"),
            "ASSR_EXEMPT": assr_exempt.get("_status", "ok"),
            "ASSR_LOCATION": assr_location.get("_status", "ok"),
            "ASSR_LAND": assr_land.get("_status", "ok"),
            "ASSR_RESIDENTIAL": assr_res.get("_status", "ok"),
            "ASSR_OBY": assr_oby.get("_status", "ok"),
            "ASSR_COMM_BLDG": comm_bldg.get("_status", "ok"),
            "ASSR_PROP_ASSOCIATION": prop_assoc.get("_status","ok"),
            "ASSR_SALES": assr_sales_dalet.get("_status", "ok"),
            "ASSR_NOTICE_SUMMARY": notice_sum.get("_status", "ok"),
            "ASSR_APPEALS": appeals.get("_status", "ok"),
            "ASSR_HIE_ADDN": assr_hie.get("_status", "ok"),
            "PTAX_MAIN": ptax_main.get("_status", "ok"),
            "PTAX_ADDL_BUYERS": ptax_buyers.get("_status", "ok"),
            "PTAX_ADDL_SELLERS": ptax_sellers.get("_status", "ok"),
            "PTAX_ADDL_PINS": ptax_pins.get("_status", "ok"),
            "PTAX_PERS": ptax_personal.get("_status", "ok"),
            "ROD": rod.get("_status", "ok"),
            "PTAB_COUNT": len(ptab_rows or []),
            "PERMITS_CCAO": permits_ccao.get("_status", "ok"),
            "DELINQUENT": "ok" if isinstance(delinquent, dict) else "empty",
            "PRC": prc.get("_status", "ok"),


        }

    }

    stamps = prev.get("stamps", {}).copy()
    for key, obj in [
        ("BOR", bor), ("CV", cv), ("SALES", sales), ("PERMITS", permits),
        ("ASSR_VALUES", assr_vals), ("ASSR_PROFILE", assr_profile),
        ("ASSR_MAILDETAIL", assr_maildetail), ("ASSR_EXEMPT", assr_exempt),
        ("ASSR_LOCATION", assr_location), ("ASSR_LAND", assr_land),
        ("ASSR_RESIDENTIAL", assr_res), ("ASSR_OBY", assr_oby),
        ("ASSR_COMM_BLDG", comm_bldg), ("ASSR_PROP_ASSOCIATION", prop_assoc),
        ("ASSR_SALES", assr_sales_dalet), ("ASSR_NOTICE_SUMMARY", notice_sum),
        ("ASSR_APPEALS", appeals), ("ASSR_HIE_ADDN", assr_hie),
        ("PTAX_MAIN", ptax_main), ("ROD", rod),("PTAB", ptab),
        ("PERMITS_CCAO", permits_ccao),("PRC", prc),
    ]:
        if obj and obj.get("_status", "ok") == "ok":
            stamps[key] = now
    _CACHE[ck] = {"data": payload, "stamps": stamps}
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

def _shape_property_info(bor, cv, mail_src, assr_profile):
    prof = (assr_profile or {}).get("normalized", {})
    mail_norm = (mail_src or {}).get("normalized", {})
    taxpayer = mail_norm.get("Taxpayer Name")
    mailing = None
    if mail_norm:
        parts = [
            mail_norm.get("Mailing Address (line1)"),
            mail_norm.get("Mailing City"),
            mail_norm.get("Mailing State"),
            mail_norm.get("Mailing Zip"),
        ]
        mailing = ", ".join([p for p in parts if p])
    return {
        "Class": bor.get("class"),
        "Class (Assessor Profile)": prof.get("PIN Info • Class"),
        "Property Use": cv.get("property_use"),
        "Address": (
            cv.get("address")
            or bor.get("address")
            or prof.get("PIN Info • Property Address")
            or prof.get("Address (header)")
        ),
        "Building SqFt": cv.get("building_sqft"),
        "Land SqFt": cv.get("land_sqft"),
        "Investment Rating": cv.get("investment_rating"),
        "Age": cv.get("age"),
        "Town Name": prof.get("PIN Info • Town Name"),
        "Neighborhood": prof.get("PIN Info • Neighborhood") or prof.get("Neighborhood (header)"),
        "Tax District": prof.get("PIN Info • Tax District"),
        "Taxpayer Name": taxpayer,
        "Mailing Address": mailing,
    }



def _shape_assessed(assr_vals):
    # New fetcher returns:
    # {"normalized": {"rows":[{...combined fields...}], "summary":[...], "detail":{...}}, ...}
    norm = (assr_vals or {}).get("normalized") or {}
    rows = norm.get("rows") or []
    if rows:
        return rows
    # Fallbacks if rows didn't build for some reason
    if norm.get("summary"):
        return norm["summary"]
    return []



def _shape_sales(sales, cv, assr_sales_norm=None):
    """
    Prefer your external sales dataset. If it's empty, fall back to the
    Assessor Sales datalet (summary_rows + details).
    """
    latest = (sales or {}).get("latest") or {}
    history = (sales or {}).get("history") or []

    # If your dataset is empty, try the Assessor datalet
    if (not latest) and assr_sales_norm:
        rows = assr_sales_norm.get("summary_rows") or []
        if rows:
            # sort by date desc if possible
            def _pdate(s):
                try:
                    return datetime.strptime((s or "").strip(), "%m/%d/%Y")
                except Exception:
                    return datetime.min

            rows_sorted = sorted(rows, key=lambda r: _pdate(r.get("Sale Date")), reverse=True)
            r0 = rows_sorted[0]

            latest = {
                "date": r0.get("Sale Date"),
                "price": r0.get("Sale Price"),
                "doc": r0.get("Document #"),
                "instrument_type": r0.get("Instr. Type"),
                "seller": r0.get("Grantor/Seller"),
                "buyer": r0.get("Grantee/Buyer"),
                "source": "assessor_datalet",
            }

            history = [
                {
                    "date": r.get("Sale Date"),
                    "price": r.get("Sale Price"),
                    "doc": r.get("Document #"),
                    "instrument_type": r.get("Instr. Type"),
                    "seller": r.get("Grantor/Seller"),
                    "buyer": r.get("Grantee/Buyer"),
                }
                for r in rows_sorted
            ]

    return {
        "Latest Sale Date": latest.get("date"),
        "Latest Sale Price": latest.get("price"),
        "Notes": latest.get("notes"),
        "History": history[:5],
    }


def _shape_permits(permits):
    # accept both new + old fetcher shapes
    norm = (permits or {}).get("normalized") or {}
    rows = norm.get("rows") or permits.get("rows") or []
    out = {"rows": rows[:25]}
    # optional: expose count if UI wants it (harmless if ignored)
    try:
        out["count"] = len(rows)
    except Exception:
        pass
    return out


def _shape_nearby(bor, cv):
    # stub; wire your class+distance logic here later
    return {"rows": []}

def _shape_links(pin_raw, prc=None):
    prc_url = None
    if prc and prc.get("_status") == "ok":
        prc_url = (prc.get("normalized") or {}).get("url")
    if not prc_url:
        # fallback: construct directly if needed
        prc_url = f"https://data.cookcountyassessoril.gov/viewcard/viewcard.aspx?pin={pin_raw}"

    return {
        "CookViewer": f"https://maps.cookcountyil.gov/cookviewer/?search={pin_raw}",
        "PRC": prc_url,
    }

