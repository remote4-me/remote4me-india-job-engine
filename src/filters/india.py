import re
from pathlib import Path
from src.utils import norm_text

BASE = Path(__file__).resolve().parents[2]
CITIES = [x.strip().lower() for x in (BASE/"config/india_cities.txt").read_text(encoding="utf-8").splitlines() if x.strip()]

INDIA_PATTERNS = [
    r"\bindia\b", r"\bindian\b", r"\bremote\s*[-–—]?\s*india\b",
    r"\bindia\s*[-–—]?\s*remote\b", r"\bwork\s+from\s+home\s*[-–—]?\s*india\b",
    r"\bwork\s+from\s+india\b", r"\bwfh\s+india\b", r"\bpan\s*india\b",
]

FOREIGN_ONLY_PATTERNS = [
    r"\bunited states\s+only\b", r"\busa\s+only\b", r"\bcanada\s+only\b",
    r"\buk\s+only\b", r"\bunited kingdom\s+only\b", r"\baustralia\s+only\b",
    r"\bsingapore\s+only\b", r"\beurope\s+only\b", r"\beu\s+only\b",
    r"\bnorth america\s+only\b",
]


def india_score(text, country_code=""):
    t = norm_text(text)
    if norm_text(country_code) == "in":
        return 10
    score = 0
    if any(re.search(p, t) for p in INDIA_PATTERNS):
        score += 5
    for c in CITIES:
        if re.search(r"(?<![a-z])" + re.escape(c) + r"(?![a-z])", t):
            score += 2
            if score >= 7:
                break
    if any(re.search(p, t) for p in FOREIGN_ONLY_PATTERNS) and score == 0:
        return 0
    return score


def is_india_job(title, location, description="", country_code=""):
    combined = " ".join([title or "", location or "", description or ""])
    return india_score(combined, country_code) > 0
