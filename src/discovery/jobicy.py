import requests
from src.utils import clean_url

BASE_URL = "https://jobicy.com/api/v2/remote-jobs"
UA = "remote4me-india-job-engine/4.0 (+https://remote4.me)"


def fetch_jobs():
    """Fetch several broad support-oriented slices from Jobicy's public API."""
    queries = [
        {"count": 200, "industry": "supporting"},
        {"count": 200, "tag": "customer support"},
        {"count": 200, "tag": "customer success"},
        {"count": 200, "tag": "technical support"},
    ]
    out = []
    seen = set()
    for params in queries:
        try:
            r = requests.get(BASE_URL, params=params, headers={"User-Agent": UA}, timeout=30)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            print("Jobicy error:", e)
            continue
        for j in data.get("jobs", []):
            url = clean_url(j.get("url", ""))
            if not url:
                continue
            key = str(j.get("id") or url)
            if key in seen:
                continue
            seen.add(key)
            desc = j.get("jobDescription", "") or ""
            out.append({
                "url": url,
                "title": j.get("jobTitle", ""),
                "snippet": j.get("jobExcerpt", ""),
                "description": desc,
                "company": j.get("companyName", ""),
                "location": j.get("jobGeo", ""),
                "country": "",
                "country_code": "",
                "remote": True,
                "work_model": "Remote",
                "posted": j.get("pubDate", ""),
                "employment_type": ", ".join(j.get("jobType", []) or []),
                "salary": "",
                "source": "jobicy",
                "source_url": url,
            })
    print(f"Jobicy: jobs={len(out)}")
    return out
