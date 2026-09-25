import requests
from src.utils import clean_url

BASE_URL = "https://devglobaljobs.com/api/v1/jobs"
UA = "remote4me-india-job-engine/4.0 (+https://remote4.me)"


def fetch_jobs():
    queries = [
        {"limit": 100, "country": "India"},
        {"limit": 100, "search": "customer support"},
        {"limit": 100, "search": "customer service"},
        {"limit": 100, "search": "technical support"},
        {"limit": 100, "search": "customer success"},
    ]
    out = []
    seen = set()
    for params in queries:
        try:
            r = requests.get(BASE_URL, params=params, headers={"User-Agent": UA}, timeout=30)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            print("DevGlobal error:", e)
            continue
        jobs = data.get("jobs") or data.get("data") or []
        for j in jobs:
            url = clean_url(j.get("url") or j.get("job_url") or j.get("link") or "")
            if not url:
                continue
            key = str(j.get("id") or url)
            if key in seen:
                continue
            seen.add(key)
            desc = j.get("description") or j.get("snippet") or ""
            out.append({
                "url": url,
                "title": j.get("title") or j.get("job_title") or "",
                "snippet": j.get("snippet") or "",
                "description": desc,
                "company": j.get("company") or j.get("company_name") or "",
                "location": j.get("location") or j.get("country") or "",
                "country": j.get("country") or "",
                "country_code": j.get("country_code") or "",
                "remote": bool(j.get("remote")) or "remote" in str(j.get("location", "")).lower(),
                "work_model": j.get("work_model") or j.get("workplace_type") or "",
                "posted": j.get("posted_at") or j.get("date_posted") or j.get("pubDate") or "",
                "employment_type": j.get("type") or j.get("employment_type") or "",
                "salary": j.get("salary") or "",
                "source": "devglobal",
                "source_url": url,
            })
    print(f"DevGlobal: jobs={len(out)}")
    return out
