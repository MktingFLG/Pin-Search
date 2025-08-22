# utils.py
import re

# Cook County PIN helpers
# Formats supported:
#   - "18-36-205-071-0000"
#   - "18362050710000"
#   - with/without spaces

_PIN_DIGITS = 14
_PIN_DASHED_RE = re.compile(r"^\d{2}-\d{2}-\d{3}-\d{3}-\d{4}$")

def only_digits(s: str) -> str:
    return re.sub(r"\D+", "", s or "")

def is_valid_pin(pin: str) -> bool:
    """Basic validation: exactly 14 digits (Cook County)."""
    d = only_digits(pin)
    return len(d) == _PIN_DIGITS

def normalize_pin(pin: str) -> str:
    """Return dashed XX-XX-XXX-XXX-XXXX or '' if invalid."""
    d = undashed_pin(pin)
    if not d:
        return ""
    return f"{d[0:2]}-{d[2:4]}-{d[4:7]}-{d[7:10]}-{d[10:14]}"

def undashed_pin(pin: str) -> str:
    """Return 14-digit numeric PIN or '' if invalid."""
    if pin is None:
        return ""
    d = re.sub(r"\D", "", str(pin))
    return d if len(d) == 14 else ""



def try_normalize_pin(pin: str):
    try:
        dash = normalize_pin(pin)
        return True, dash, undashed_pin(dash), None
    except Exception as e:
        return False, None, None, str(e)
