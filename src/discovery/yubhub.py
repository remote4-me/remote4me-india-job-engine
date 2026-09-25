import feedparser
import requests
from src.utils import clean_url

FEEDS = [
    "https://feeds.yubhub.co/facet/category/customer-support.rss",
    "https://feeds.yubhub.co/facet/category/customer-service.rss",
    "https://feeds.yubhub.co/facet/category/customer-success.rss",
    "https://feeds.yubhub.co/facet/skill/customer-service.rss",
    "https://feeds.yubhub.co/facet/skill/customer-success.rss",
    "https://feeds.yubhub.co/facet/skill/troubleshooting.rss",
    "https://feeds.yubhub.co/facet/skill/account-management.rss",
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
            print("YubHub error:", feed_url, e)
            continue
        for e in f.entries:
            url = clean_url(getattr(e, "link", ""))
            if not url:
                continue
            key = url
            if key in seen:
                continue
            seen.add(key)
            summary = getattr(e, "summary", "") or ""
            out.append({
                "url": url,
                "title": getattr(e, "title", ""),
                "snippet": summary,
                "description": summary,
                "company": getattr(e, "author", "") or "",
                "location": "",
                "country": "",
                "country_code": "",
                "remote": True,
                "work_model": "Remote",
                "posted": getattr(e, "published", "") or "",
                "employment_type": "",
                "salary": "",
                "source": "yubhub",
                "source_url": url,
            })
    print(f"YubHub: jobs={len(out)}")
    return out
