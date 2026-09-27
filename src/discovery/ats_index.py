import json
import os
import re
import time
from urllib.parse import urlencode

import requests

from src.utils import clean_url, safe_int, sleep_delay

UA = "remote4me-india-job-engine/6.0 (+https://remote4.me)"
INDEX_INFO = "https://index.commoncrawl.org/collinfo.json"

# Common Crawl is used only for DISCOVERY of public ATS boards. The live ATS
# endpoints remain the source of current jobs.
PATTERNS = {
    "greenhouse": ["https://boards.greenhouse.io/", "https://job-boards.greenhouse.io/"],
    "lever": ["https://jobs.lever.co/"],
    "ashby": ["https://jobs.ashbyhq.com/"],
    "workable": ["https://apply.workable.com/", "https://jobs.workable.com/"],
}


def _latest_index():
    try:
        r = requests.get(INDEX_INFO, headers={"User-Agent": UA}, timeout=20)
        r.raise_for_status()
        rows = r.json()
        if isinstance(rows, list) and rows:
            # Common Crawl returns newest collections first in normal operation;
            # still sort by crawl id for safety.
            ids = [str(x.get("id", "")).strip() for x in rows if isinstance(x, dict) and x.get("id")]
            if ids:
                return sorted(ids, reverse=True)[0]
    except Exception as e:
        print("Common Crawl index discovery error:", e)
    return ""


def _board_from_url(ats, url):
    try:
        from urllib.parse import urlparse
        p = urlparse(url)
        host = p.netloc.lower().split(":")[0]
        path = [x for x in p.path.split("/") if x]
        if not path:
            return ""
        if ats == "greenhouse" and host in {"boards.greenhouse.io", "job-boards.greenhouse.io"}:
            return path[0]
        if ats == "lever" and host == "jobs.lever.co":
            return path[0]
        if ats == "ashby" and host == "jobs.ashbyhq.com":
            return path[0]
        if ats == "workable" and host in {"apply.workable.com", "jobs.workable.com"}:
            return path[0]
    except Exception:
        pass
    return ""


def discover_boards(max_per_ats=None):
    if os.getenv("COMMON_CRAWL_DISCOVERY", "1").strip().lower() in {"0", "false", "no", "off"}:
        return []
    max_per_ats = max_per_ats or safe_int("COMMON_CRAWL_MAX_BOARDS", 250)
    index_id = _latest_index()
    if not index_id:
        return []

    out = []
    seen = set()
    for ats, prefixes in PATTERNS.items():
        for prefix in prefixes:
            params = {
                "url": prefix,
                "matchType": "prefix",
                "output": "json",
                "filter": "status:200",
                "collapse": "urlkey",
                "limit": str(max_per_ats),
            }
            url = f"https://index.commoncrawl.org/{index_id}-index?{urlencode(params)}"
            try:
                r = requests.get(url, headers={"User-Agent": UA}, timeout=30)
                if r.status_code == 503:
                    print("Common Crawl rate limited; stopping ATS discovery for this run")
                    return out
                r.raise_for_status()
                for line in r.text.splitlines():
                    try:
                        row = json.loads(line)
                    except Exception:
                        continue
                    captured = clean_url(row.get("url", ""))
                    board = _board_from_url(ats, captured)
                    if not board:
                        continue
                    key = f"{ats}|{board}".lower()
                    if key in seen:
                        continue
                    seen.add(key)
                    out.append({
                        "company": board.replace("-", " ").replace("_", " ").title(),
                        "domain": "",
                        "career_url": captured,
                        "ats": ats,
                        "board": f"{prefix}{board}",
                        "source": "commoncrawl_ats",
                    })
            except Exception as e:
                print("Common Crawl ATS query error", ats, prefix, e)
            time.sleep(max(1.5, float(os.getenv("COMMON_CRAWL_DELAY", "2.0"))))
    print(f"Common Crawl ATS discovery: boards={len(out)}")
    return out
