# app.py
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from orchestrator import get_pin_summary

app = FastAPI(title="PIN Tool API", version="0.1.0")

# Allow GitHub Pages domain to call this API (adjust to your pages domain)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten later to your Pages URL
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["*"],
)

@app.get("/api/health")
def health():
    return {"status": "ok"}

@app.get("/api/pin/{pin}")
def pin_summary(pin: str, fresh: int = Query(0, ge=0, le=1)):
    """
    fresh=1 → bypass cache (always revalidate sources)
    fresh=0 → use smart revalidation and TTLs
    """
    data = get_pin_summary(pin, fresh=bool(fresh))
    return JSONResponse(data)
