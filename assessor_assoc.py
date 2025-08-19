# assessor_assoc.py
import json
from pathlib import Path

DATA_DIR = Path("data")
INDEX_DIR = DATA_DIR / "_index"
PIN_TO_KEY_JSON  = INDEX_DIR / "pin_to_key.json"
KEY_TO_CHILD_JSON = INDEX_DIR / "key_to_children.json"

_pin_to_key = None
_key_to_children = None

def _load():
    global _pin_to_key, _key_to_children
    if _pin_to_key is None or _key_to_children is None:
        _pin_to_key = json.loads(PIN_TO_KEY_JSON.read_text(encoding="utf-8")) if PIN_TO_KEY_JSON.exists() else {}
        _key_to_children = json.loads(KEY_TO_CHILD_JSON.read_text(encoding="utf-8")) if KEY_TO_CHILD_JSON.exists() else {}

def get_associated_pins(pin_undashed: str) -> list[str]:
    """
    Returns all PINs (undashed, 14 digits) associated to the given PIN’s KEY, including the PIN itself.
    If the PIN has no key in the index, returns [pin] if it exists; else [].
    """
    _load()
    pin = "".join(ch for ch in (pin_undashed or "") if ch.isdigit())
    if not pin:
        return []
    key = _pin_to_key.get(pin)
    if not key:
        # If we have no mapping but pin occurs as a child of itself (standalone), return it.
        if pin in _key_to_children.get(pin, []):
            return [pin]
        return []
    return _key_to_children.get(key, [pin])
