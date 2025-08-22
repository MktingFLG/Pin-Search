# fix_fetchers_v2.py
import pathlib, re

p = pathlib.Path("fetchers.py")
src = p.read_text(encoding="utf-8")

# Plain string replacements (no regex escapes to fight)
plain = {
    "undashed\\_pin": "undashed_pin",
    "normalize\\_pin": "normalize_pin",
    "*RAW_DIR": "_RAW_DIR",
    "def *save_raw_html": "def _save_raw_html",
    "col*{i+1}": "col_{i+1}",
    "http\\://": "http://",
    "https\\://": "https://",
    'PTAB_BASE = "[https://www.ptab.illinois.gov/asi]"': 'PTAB_BASE = "https://www.ptab.illinois.gov/asi"',
    'PROFILE_BASE = "[https://assessorpropertydetails.cookcountyil.gov/datalets/datalet.aspx]"':
        'PROFILE_BASE = "https://assessorpropertydetails.cookcountyil.gov/datalets/datalet.aspx"',
    'VALUES_BASE = "[https://assessorpropertydetails.cookcountyil.gov/datalets/datalet.aspx]"':
        'VALUES_BASE  = "https://assessorpropertydetails.cookcountyil.gov/datalets/datalet.aspx"',
}
for k, v in plain.items():
    src = src.replace(k, v)

# Ensure money cleaner has the correct body (replace the whole function body by name)
def replace_money_cleaner(text: str) -> str:
    pat = re.compile(r"(def\s+_clean_money_to_float\s*\(val:\s*str\)\s*->\s*float:\s*\n)(?:[ \t].*\n)+", re.MULTILINE)
    new_body = (
        "def _clean_money_to_float(val: str) -> float:\n"
        "    if val is None:\n"
        "        return 0.0\n"
        "    s = str(val)\n"
        "    s = re.sub(r\"[^\\d.\\-]\", \"\", s)  # keep digits, dot, minus\n"
        "    if s in {\"\", \".\", \"-\"}:\n"
        "        return 0.0\n"
        "    try:\n"
        "        return float(s)\n"
        "    except Exception:\n"
        "        return 0.0\n"
    )
    return pat.sub(new_body + "\n", text, count=1)

src = replace_money_cleaner(src)

# Add DEFAULT_UA once (if missing)
if "DEFAULT_UA =" not in src:
    src = src.replace(
        "import pandas as _pd",
        'import pandas as _pd\n\nDEFAULT_UA = "PIN-Tool/1.0 (+https://example.com)"',
        1,
    )

# Normalize requests.get UA headers
src = re.sub(
    r'headers=\{"User-Agent":\s*"[^"]*"\}',
    'headers={"User-Agent": DEFAULT_UA}',
    src
)

# Keep only one lru_cache import
lines, seen = [], False
for line in src.splitlines():
    if line.strip() == "from functools import lru_cache":
        if seen: 
            continue
        seen = True
    lines.append(line)
src = "\n".join(lines)

# Drop Iterable from typing (unused)
src = src.replace(
    "from typing import Any, Dict, List, Optional, Tuple, Iterable",
    "from typing import Any, Dict, List, Optional, Tuple",
)

p.write_text(src, encoding="utf-8")
print("Done: fix_fetchers_v2 complete")
