import re
from pathlib import Path
from src.utils import norm_text

BASE = Path(__file__).resolve().parents[2]
CITIES = [x.strip().lower() for x in (BASE / "config/india_cities.txt").read_text(encoding="utf-8").splitlines() if x.strip()]

INDIA_PATTERNS = [
    r"\bindia\b", r"\bindian\b", r"\bremote\s*[-–—|:]?\s*india\b",
    r"\bindia\s*[-–—|:]?\s*remote\b", r"\bwork\s+from\s+(?:home\s+)?[-–—|:]?\s*india\b",
    r"\bwork\s+from\s+india\b", r"\bwfh\s+india\b", r"\bpan\s*india\b",
    r"\bbased\s+in\s+india\b", r"\bindia[- ]based\b", r"\banywhere\s+in\s+india\b",
    r"\bhire\s+(?:in|across)\s+india\b", r"\beligible\s+to\s+work\s+in\s+india\b",
    r"\bindia\s+(?:only|preferred|eligible|location|locations)\b",
]

FOREIGN_ONLY_PATTERNS = [
    r"\bunited states\s+only\b", r"\busa\s+only\b", r"\bcanada\s+only\b",
    r"\buk\s+only\b", r"\bunited kingdom\s+only\b", r"\baustralia\s+only\b",
    r"\bsingapore\s+only\b", r"\beurope\s+only\b", r"\beu\s+only\b",
    r"\bnorth america\s+only\b", r"\bapac\s+only\b",
]

FOREIGN_LOCATION_PATTERNS = [
    r"\b(?:united states|usa|u\.s\.|canada|uk|united kingdom|australia|singapore|ireland|germany|france|spain|portugal|netherlands|prague|tokyo|uruguay|brazil)\b",
]

NON_CITY_LINES = {"india", "remote india", "remote - india", "work from home india"}


def city_signal(text):
    t = norm_text(text)
    hits = []
    for c in CITIES:
        if c in NON_CITY_LINES:
            continue
        if re.search(r"(?<![a-z])" + re.escape(c) + r"(?![a-z])", t):
            hits.append(c)
            if len(hits) >= 3:
                break
    return (2 if hits else 0), hits


def explicit_india_signal(text):
    t = norm_text(text)
    return any(re.search(p, t) for p in INDIA_PATTERNS)


def india_score(text, country_code=""):
    t = norm_text(text)
    cc = norm_text(country_code)
    if cc in {"in", "ind", "india"}:
        return 10
    score = 5 if explicit_india_signal(t) else 0
    city_score, _ = city_signal(t)
    score += city_score
    if any(re.search(p, t) for p in FOREIGN_ONLY_PATTERNS) and not explicit_india_signal(t):
        return 0
    return score


def is_india_job(title, location, description="", country_code=""):
    """Strict India eligibility.

    Location/country metadata is preferred over incidental mentions in the job text.
    A foreign-only location is rejected unless the listing explicitly states India
    eligibility as an allowed location.
    """
    loc = norm_text(location)
    desc = norm_text(description)
    title_t = norm_text(title)
    cc = norm_text(country_code)

    if cc in {"in", "ind", "india"}:
        return True

    # If a structured location names a foreign country/city and does not also
    # identify India, do not let generic description text make it an India job.
    if loc and any(re.search(p, loc) for p in FOREIGN_LOCATION_PATTERNS) and not explicit_india_signal(loc):
        return False

    # Explicit India in the actual location is strong evidence.
    if explicit_india_signal(loc):
        return True
    if city_signal(loc)[0] > 0:
        return True

    # For a generic Remote location, require an explicit India-eligibility signal
    # in the title/description rather than a casual company/location mention.
    if explicit_india_signal(" ".join([title_t, desc])):
        return True

    return False
