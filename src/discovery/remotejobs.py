import requests
from src.utils import clean_url

BASE_URL = "https://remotejobs.org/api/v1/jobs"
UA = "remote4me-india-job-engine/4.0 (+https://remote4.me)"


def fetch_jobs(max_pages=4):
    out = []
    seen = set()
    offset = 0
    for _ in range(max_pages):
        params = {"category": "customer-support", "limit": 50, "offset": offset}
        try:
            r = requests.get(BASE_URL, params=params, headers={"User-Agent": UA}, timeout=30)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            print("RemoteJobs error:", e)
            break
        rows = data.get("data", [])
        for j in rows:
            url = clean_url(j.get("url") or j.get("apply_url") or "")
            if not url:
                continue
            key = str(j.get("id") or url)
            if key in seen:
                continue
            seen.add(key)
            company = j.get("company") or {}
            if not isinstance(company, dict):
                company = {"name": str(company)}
            out.append({
                "url": url,
                "title": j.get("title", ""),
                "snippet": j.get("description", "")[:1000],
                "description": j.get("description", ""),
                "company": company.get("name", ""),
                "company_website": company.get("website", ""),
                "location": j.get("location", ""),
                "country": "",
                "country_code": "",
                "remote": True,
                "work_model": "Remote",
                "posted": j.get("posted_at", ""),
                "employment_type": j.get("type", ""),
                "salary": j.get("salary_text", ""),
                "source": "remotejobs.org",
                "source_url": url,
            })
        pagination = data.get("pagination", {})
        if not pagination.get("has_more"):
            break
        offset += 50
    print(f"RemoteJobs.org: jobs={len(out)}")
    return out
