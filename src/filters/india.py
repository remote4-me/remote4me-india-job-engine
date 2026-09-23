import re
from pathlib import Path
from src.utils import norm_text

BASE = Path(__file__).resolve().parents[2]
CITIES = [x.strip().lower() for x in (BASE/"config/india_cities.txt").read_text(encoding="utf-8").splitlines() if x.strip()]

INDIA_PATTERNS = [
    r"\bindia\b", r"\bindian\b", r"\bremote\s*[-–—]?\s*india\b",
    r"\bwork\s+from\s+home\s+[-–—]?\s*india\b", r"\bwfh\s+india\b"
]

FOREIGN_PATTERNS = [
    r"\bunited states\b", r"\busa\b", r"\bcanada\b", r"\buk\b", r"\bunited kingdom\b",
    r"\baustralia\b", r"\bsingapore\b", r"\bphilippines\b", r"\beurope\b", r"\bgermany\b",
    r"\bfrance\b", r"\bnetherlands\b", r"\bireland\b", r"\bnew zealand\b"
]

def india_score(text):
    t = norm_text(text)
    score = 0
    if any(re.search(p,t) for p in INDIA_PATTERNS): score += 5
    for c in CITIES:
        if re.search(r"(?<![a-z])"+re.escape(c)+r"(?![a-z])", t):
            score += 2
            if score >= 7: break
    return score

def foreign_only(text):
    t = norm_text(text)
    foreign = any(re.search(p,t) for p in FOREIGN_PATTERNS)
    india = india_score(t) > 0
    return foreign and not india

def is_india_job(title, location, description=""):
    combined = " ".join([title or "", location or "", description or ""])
    if foreign_only(combined): return False
    return india_score(combined) > 0
