# fix_fetchers.py
import re, sys, pathlib

p = pathlib.Path("fetchers.py")
src = p.read_text(encoding="utf-8")

replacements = {
    r"undashed\\_pin": "undashed_pin",
    r"normalize\\_pin": "normalize_pin",
    r"\*RAW_DIR": "_RAW_DIR",
    r"def \*save_raw_html": "def _save_raw_html",
    r'col\*{i\+1}': "col_{i+1}",
    r'http\\:\/\/': "http://",
    r'https\\:\/\/': "https://",
    r'PTAB_BASE\s*=\s*"\[https:\/\/www\.ptab\.illinois\.gov\/asi\]"': 'PTAB_BASE = "https://www.ptab.illinois.gov/asi"',
    r'PROFILE_BASE\s*=\s*"\[https:\/\/assessorpropertydetails\.cookcountyil\.gov\/datalets\/datalet\.aspx\]"': 'PROFILE_BASE = "https://assessorpropertydetails.cookcountyil.gov/datalets/datalet.aspx"',
    r'VALUES_BASE\s*=\s*"\[https:\/\/assessorpropertydetails\.cookcountyil\.gov\/datalets\/datalet\.aspx\]"': 'VALUES_BASE  = "https://assessorpropertydetails.cookcountyil.gov/datalets/datalet.aspx"',
}

for pat, repl in replacements.items():
    src = re.sub(pat, repl, src)

# Ensure money cleaner regex is correct
src = re.sub(
    r"def\s+_clean_money_to_float\(val:\s*str\)\s*->\s*float:[\s\S]*?return\s+0\.0\s*\n\s*",
    """def _clean_money_to_float(val: str) -> float:
    if val is None:
        return 0.0
    s = str(val)
    s = re.sub(r"[^\\d.\\-]", "", s)  # keep digits, dot, minus
    if s in {"", ".", "-"}:
        return 0.0
    try:
        return float(s)
    except Exception:
        return 0.0
""",
    src,
    count=1,
)

# Add DEFAULT_UA once (if not present)
if "DEFAULT_UA =" not in src:
    src = src.replace(
        "import pandas as _pd",
        'import pandas as _pd\n\nDEFAULT_UA = "PIN-Tool/1.0 (+https://example.com)"',
        1
    )

# Normalize requests.get headers (best effort)
src = re.sub(
    r'headers=\{"User-Agent":\s*"[^"]*"\}',
    'headers={"User-Agent": DEFAULT_UA}',
    src
)

# Remove duplicate from functools import lru_cache lines
lines = src.splitlines()
seen_lru = False
out_lines = []
for line in lines:
    if line.strip() == "from functools import lru_cache":
        if seen_lru:
            continue
        seen_lru = True
    out_lines.append(line)
src = "\n".join(out_lines)

# Remove Iterable from typing import (if present and unused)
src = re.sub(
    r"from typing import Any, Dict, List, Optional, Tuple, Iterable",
    "from typing import Any, Dict, List, Optional, Tuple",
    src
)

p.write_text(src, encoding="utf-8")

# Report remaining code fences and escaped backticks (you’ll remove manually)
bad = []
for i, line in enumerate(src.splitlines(), start=1):
    if line.strip() == "```":
        bad.append(i)
if bad:
    print("Remove stray ``` on lines:", bad)

print("Done. Backed up as fetchers.py.bak")
