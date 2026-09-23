import re
from pathlib import Path
from src.utils import norm_text

BASE = Path(__file__).resolve().parents[2]
DATA = __import__("json").loads((BASE/"config/keywords.json").read_text(encoding="utf-8"))

def relevance(title, description="", company=""):
    text = norm_text(" ".join([title or "", description or ""]))
    title_t = norm_text(title)
    score = 0
    hits = []
    for k in DATA["primary"]:
        if k in title_t:
            score += 8
            hits.append(k)
        elif k in text:
            score += 4
            hits.append(k)
    for k in DATA["context"]:
        if k in text: score += 1
    for k in DATA["negative"]:
        if k in title_t: score -= 8
        elif k in text: score -= 2
    return max(0, score), sorted(set(hits))[:8]

def is_support_job(title, description="", company=""):
    score, hits = relevance(title, description, company)
    return score >= 4
