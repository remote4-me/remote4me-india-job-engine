import requests
import feedparser
from src.utils import clean_url

FEEDS = [
    "https://weworkremotely.com/categories/remote-customer-support-jobs.rss",
    "https://weworkremotely.com/categories/all-other-remote-jobs.rss",
]
UA = "remote4me-india-job-engine/4.0 (+https://remote4.me)"


def fetch_jobs():
    out = []
    seen = set()
    for feed_url in FEEDS:
        try:
            r = requests.get(feed_url, headers={"User-Agent": UA}, timeout=30)
            r.raise_for_status()
            f = feedparser.parse(r.content)
        except Exception as e:
            print("WWR error:", e)
            continue
        for e in f.entries:
            url = clean_url(getattr(e, "link", ""))
            if not url or url in seen:
                continue
            seen.add(url)
            summary = getattr(e, "summary", "") or ""
            out.append({
                "url": url,
                "title": getattr(e, "title", ""),
                "snippet": summary,
                "description": summary,
                "company": "",
                "location": "",
                "country": "",
                "country_code": "",
                "remote": True,
                "work_model": "Remote",
                "posted": getattr(e, "published", "") or "",
                "employment_type": "",
                "salary": "",
                "source": "weworkremotely",
                "source_url": url,
            })
    print(f"WWR: jobs={len(out)}")
    return out
