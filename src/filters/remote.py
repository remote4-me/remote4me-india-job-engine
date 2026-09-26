import re
from pathlib import Path
from src.utils import norm_text

BASE = Path(__file__).resolve().parents[2]
CITIES = [x.strip().lower() for x in (BASE / "config/india_cities.txt").read_text(encoding="utf-8").splitlines() if x.strip()]

REMOTE_PATTERNS = [
    r"\bremote\b", r"\bwork\s*from\s*home\b", r"\bwfh\b",
    r"\bfully\s+remote\b", r"\bremote[- ]first\b", r"\btelecommute\b",
    r"\bwork\s+from\s+anywhere\b", r"\banywhere\s+in\s+the\s+world\b",
    r"\bworldwide\b", r"\bglobal\s+remote\b", r"\bremote\s*[-–—|:]\s*india\b",
]
HYBRID_PATTERNS = [r"\bhybrid\b", r"\bon[- ]site\b", r"\bonsite\b", r"\bin[- ]office\b"]
FOREIGN_ONLY_PATTERNS = [
    r"\b(usa|us|united states)\s+only\b", r"\b(canada|canadian)\s+only\b",
    r"\buk\s+only\b", r"\bunited kingdom\s+only\b", r"\baustralia\s+only\b",
    r"\bsingapore\s+only\b", r"\beurope\s+only\b", r"\beu\s+only\b",
    r"\bnorth america\s+only\b", r"\bapac\s+only\b",
]

INDIA_PATTERNS = [
    r"\bindia\b", r"\bindian\b", r"\bremote\s*[-–—|:]?\s*india\b",
    r"\bindia\s*[-–—|:]?\s*remote\b", r"\bpan\s*india\b",
    r"\banywhere\s+in\s+india\b", r"\bwork\s+from\s+india\b",
    r"\bwfh\s+india\b", r"\bbased\s+in\s+india\b", r"\bindia[- ]based\b",
]


def city_signal(text):
    t = norm_text(text)
    for c in CITIES:
        if c in {"india", "remote india", "remote - india", "work from home india"}:
            continue
        if re.search(r"(?<![a-z])" + re.escape(c) + r"(?![a-z])", t):
            return True
    return False


def is_remote_job(title="", location="", description="", work_model="", remote=False, country_code=""):
    text = norm_text(" ".join([title, location, description, work_model]))
    if any(re.search(p, text) for p in HYBRID_PATTERNS):
        if not remote and not re.search(r"\bfully\s+remote\b", text):
            return False
    if any(re.search(p, text) for p in FOREIGN_ONLY_PATTERNS):
        return False
    return bool(remote or any(re.search(p, text) for p in REMOTE_PATTERNS))


def india_remote_job(title="", location="", description="", work_model="", remote=False, country_code=""):
    text = norm_text(" ".join([title, location, description, work_model]))
    cc = norm_text(country_code)
    if not is_remote_job(title, location, description, work_model, remote, country_code):
        return False
    if cc in {"in", "ind", "india"}:
        return True
    if any(re.search(p, text) for p in INDIA_PATTERNS):
        return True
    # A remote listing with a known Indian city is treated as India-eligible.
    if city_signal(text):
        return True
    # Keep worldwide/global listings out of the strict India-only result unless
    # the listing also contains an India/city signal.
    return False
