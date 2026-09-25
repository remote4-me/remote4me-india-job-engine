import re
from src.utils import norm_text

REMOTE_PATTERNS = [
    r"\bremote\b", r"\bwork\s*from\s*home\b", r"\bwfh\b",
    r"\bfully\s+remote\b", r"\bremote[- ]first\b", r"\btelecommute\b",
    r"\bwork\s+from\s+anywhere\b", r"\banywhere\s+in\s+the\s+world\b",
    r"\bworldwide\b", r"\bglobal\s+remote\b",
]
HYBRID_PATTERNS = [r"\bhybrid\b", r"\bon[- ]site\b", r"\bonsite\b", r"\bin[- ]office\b"]
FOREIGN_ONLY_PATTERNS = [
    r"\b(usa|us|united states)\s+only\b",
    r"\b(canada|canadian)\s+only\b",
    r"\buk\s+only\b", r"\bunited kingdom\s+only\b",
    r"\baustralia\s+only\b", r"\bsingapore\s+only\b",
    r"\beurope\s+only\b", r"\beu\s+only\b",
    r"\bnorth america\s+only\b",
]
WORLDWIDE_PATTERNS = [
    r"\bworldwide\b", r"\bwork\s+from\s+anywhere\b",
    r"\banywhere\s+in\s+the\s+world\b", r"\ball\s+countries\b",
    r"\bglobal\b",
]


def is_remote_job(title="", location="", description="", work_model="", remote=False, country_code=""):
    text = norm_text(" ".join([title, location, description, work_model]))

    if any(re.search(p, text) for p in HYBRID_PATTERNS):
        # A listing explicitly marked remote + hybrid can be ambiguous; require a strong remote signal.
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

    if cc == "in":
        return True

    india_signals = [
        r"\bindia\b", r"\bindian\b", r"\bremote\s*[-–—]\s*india\b",
        r"\bindia\s*[-–—]\s*remote\b", r"\bpan\s*india\b",
        r"\banywhere\s+in\s+india\b", r"\bwork\s+from\s+india\b",
        r"\bwfh\s+india\b",
    ]

    if any(re.search(p, text) for p in india_signals):
        return True

    # Worldwide/anywhere roles are candidates for India, but we keep them separate in the signal.
    if any(re.search(p, text) for p in WORLDWIDE_PATTERNS):
        return True

    return False
