# app.py
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from orchestrator import get_pin_summary
import os
from fastapi.staticfiles import StaticFiles
from pathlib import Path
print("ILLINOIS_APP_TOKEN set:", bool(os.getenv("ILLINOIS_APP_TOKEN")))


app = FastAPI(title="PIN Tool API", version="0.1.0")

# Allow GitHub Pages domain to call this API (adjust to your pages domain)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten later to your Pages URL
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

# Serve files from ./public and make index.html respond at "/"
STATIC_DIR = Path(__file__).parent / "docs"
print("Serving docs from:", STATIC_DIR.resolve(), "exists:", STATIC_DIR.exists())

app.mount("/", StaticFiles(directory=str(STATIC_DIR), html=True), name="static")
