import re
from pathlib import Path
from src.utils import norm_text

BASE = Path(__file__).resolve().parents[2]
DATA = __import__("json").loads((BASE/"config/keywords.json").read_text(encoding="utf-8"))


def _contains(text, phrase):
    p = norm_text(phrase)
    if not p:
        return False
    if re.match(r"^[a-z0-9 +/&-]+$", p):
        return p in text
    return p in text


def relevance(title, description="", company=""):
    text = norm_text(" ".join([title or "", description or "", company or ""]))
    title_t = norm_text(title)
    score = 0
    hits = []

    for k in DATA["primary"]:
        if _contains(title_t, k):
            score += 10
            hits.append(k)
        elif _contains(text, k):
            score += 5
            hits.append(k)

    for k in DATA["context"]:
        if _contains(text, k):
            score += 1

    for k in DATA["negative"]:
        if _contains(title_t, k):
            score -= 10
        elif _contains(text, k):
            score -= 3

    return max(0, score), sorted(set(hits))[:10]


def is_support_job(title, description="", company=""):
    score, _ = relevance(title, description, company)
    return score >= 5
