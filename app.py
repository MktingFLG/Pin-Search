import os, asyncio
from typing import Optional, List
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware

try:
    from dotenv import load_dotenv; load_dotenv()
except Exception:
    pass

import fetchers
from utils import undashed_pin, normalize_pin
# near your other imports
from fetchers import fetch_prc_link

app = FastAPI(title="PIN Tool API", version="1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"]
)

def _must_pin(pin: str) -> str:
    p14 = undashed_pin(pin)
    if not p14:
        raise HTTPException(status_code=400, detail="PIN must be 14 digits.")
    return p14

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/assessor/profile")
def assessor_profile(pin: str, jur: str = "016", taxyr: str = "2025"):
    _must_pin(pin)
    return fetchers.fetch_assessor_profile(pin, jur, taxyr)

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

async def _to_thread(fn, *a, **kw): return await asyncio.to_thread(fn, *a, **kw)

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
    results = await asyncio.gather(*tasks, return_exceptions=True)
    def _norm(x): return {"_status":"error","_meta":{"error":str(x)},"normalized":{}} if isinstance(x, Exception) else x
    keys = [
        "assessor_profile","assessor_values","assessor_location","assessor_land",
        "assessor_residential","assessor_other_structures","assessor_commercial_building",
        "assessor_exemptions","assessor_maildetail","assessor_notice_summary",
        "assessor_appeals_coes","assessor_permits","assessor_hie_additions",
        "rod_bundle","ptab","ccao_permits","ptax_main","delinquent",
    ]
    return {"_status":"ok","_meta":{"pin": normalize_pin(pin)}, "bundle": {k:_norm(v) for k,v in zip(keys,results)}}


from fastapi.responses import HTMLResponse, RedirectResponse
from orchestrator import get_pin_summary

@app.get("/")
def root():
    # send people somewhere useful instead of 404
    return RedirectResponse(url="/docs", status_code=307)

@app.get("/pin/{pin}/summary")
def pin_summary(pin: str, fresh: bool = False):
    _must_pin(pin)
    return get_pin_summary(pin, fresh=fresh)

@app.get("/ui", response_class=HTMLResponse)
@app.get("/pin/{pin}/ui", response_class=HTMLResponse)
def ui(pin: str = ""):
    # very small single-file UI
    html = f"""
<!doctype html>
<meta charset="utf-8"/>
<title>PIN Tool UI</title>
<style>
  body {{ font: 14px/1.4 system-ui, sans-serif; margin: 20px; }}
  input[type=text] {{ width: 320px; padding: 6px 8px; }}
  button {{ padding: 6px 10px; }}
  .row {{ margin: 8px 0; }}
  #out {{ white-space: pre; border: 1px solid #ddd; padding: 12px; border-radius: 6px; overflow:auto }}
</style>
<div class="row">
  <label>PIN: <input id="pin" type="text" value="{pin}"/></label>
  <button id="go">Fetch</button>
  <label style="margin-left:12px"><input type="checkbox" id="pretty"> Pretty‑print</label>
</div>
<div id="status"></div>
<pre id="out">{{}}</pre>
<script>
async function fetchPin(p) {{
  if (!p) return;
  const qs = new URLSearchParams(window.location.search);
  const fresh = qs.get("fresh")==="1" ? "&fresh=true" : "";
  const url = `/pin/${{encodeURIComponent(p)}}/summary?` + fresh;
  document.getElementById('status').textContent = "Loading " + url + " …";
  try {{
    const r = await fetch(url);
    const j = await r.json();
    const pretty = document.getElementById('pretty').checked;
    document.getElementById('out').textContent = pretty ? JSON.stringify(j, null, 2) : JSON.stringify(j);
    document.getElementById('status').textContent = "";
    history.replaceState(null, "", `/pin/${{encodeURIComponent(p)}}/ui`);
  }} catch (e) {{
    document.getElementById('status').textContent = "Error: " + e;
  }}
}}
document.getElementById('go').onclick = () => fetchPin(document.getElementById('pin').value);
document.getElementById('pin').addEventListener('keydown', (e) => {{ if (e.key==='Enter') document.getElementById('go').click(); }});
{ f"fetchPin('{pin}');" if pin else "" }
</script>
"""
    return HTMLResponse(html)



# assuming app = FastAPI() already exists

@app.get("/api/prc/{pin}")
def api_prc(pin: str):
    return fetch_prc_link(pin)


