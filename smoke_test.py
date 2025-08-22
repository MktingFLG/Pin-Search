import importlib, re

print("Importing fetchers...")
f = importlib.import_module("fetchers")
print("OK")

# Check BASE URLs
assert f.PTAB_BASE.startswith("https://"), "PTAB_BASE must be https"
assert f.PROFILE_BASE.startswith("https://"), "PROFILE_BASE must be https"
assert f.VALUES_BASE.startswith("https://"), "VALUES_BASE must be https"
print("Base URLs OK")

# Money cleaner
from fetchers import _clean_money_to_float as m
cases = {
    "$1,234.56": 1234.56,
    "  -$2,500 ": -2500.0,
    "N/A": 0.0,
    "": 0.0,
    ".": 0.0,
    "-": 0.0,
}
for s, want in cases.items():
    got = m(s)
    assert abs(got - want) < 1e-9, (s, got, want)
print("Money cleaner OK")

# PIN helpers
u = importlib.import_module("utils")
assert u.undashed_pin("12-34-567-890-1234") == "12345678901234"
assert u.normalize_pin("12345678901234") == "12-34-567-890-1234"
print("utils OK")

print("All local checks passed ✅")
