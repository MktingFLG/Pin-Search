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
    """
    Return a 14-digit normalized PIN with dashes: xx-xx-xxx-xxxx.
    Raises ValueError if not 14 digits after stripping.
    """
    d = only_digits(pin)
    if len(d) != _PIN_DIGITS:
        raise ValueError(f"PIN must have {_PIN_DIGITS} digits after stripping, got {len(d)}: {pin!r}")
    return f"{d[0:2]}-{d[2:4]}-{d[4:7]}-{d[7:10]}-{d[10:14]}"

def undashed_pin(pin: str) -> str:
    """Return the 14-digit string without dashes (some APIs want this)."""
    d = only_digits(pin)
    if len(d) != _PIN_DIGITS:
        raise ValueError(f"PIN must have {_PIN_DIGITS} digits after stripping, got {len(d)}: {pin!r}")
    return d
