# fetchers.py
"""
Thin adapters for each data source.
Replace stubs with your actual BOR, CV, Sales, Assessor scraping, and Permits functions.
"""

def fetch_bor(pin: str, force: bool = False) -> dict:
    # TODO: Replace with your real BOR dataset fetch
    return {"class": None, "building_av": None, "land_av": None, "address": None, "_status": "stub"}

def fetch_cv(pin: str, force: bool = False) -> dict:
    # TODO: Replace with your real Commercial Valuation dataset fetch
    return {"property_use": None, "building_sqft": None, "land_sqft": None,
            "investment_rating": None, "age": None, "address": None, "_status": "stub"}

def fetch_sales(pin: str, force: bool = False) -> dict:
    # TODO: Replace with your real Parcel Sales dataset fetch
    return {"latest": {}, "history": [], "_status": "stub"}

def fetch_permits(pin: str, force: bool = False) -> dict:
    # TODO: Replace with your real Permits dataset fetch
    return {"rows": [], "_status": "stub"}

def fetch_assessor_values(pin: str, force: bool = False) -> dict:
    # TODO: Replace with your real Assessor Values scraping function
    return {"summary": [], "_status": "stub"}

def fetch_assessor_mail_exempt(pin: str, force: bool = False) -> dict:
    # TODO: Replace with your real Assessor mailing & exemptions scraping function
    return {"taxpayer": None, "mailing_address": None, "_status": "stub"}
