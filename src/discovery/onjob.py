import os
import requests

from src.utils import clean_url

BASE_URL = "https://onjob.io/feeds/jobs.json"
UA = "remote4me-india-job-engine/4.0 (+https://remote4.me)"


def fetch_jobs(max_pages=None):
    """Fetch OnJob's public paginated JSON feed."""
    if max_pages is None:
        try:
            max_pages = int(os.getenv("ONJOB_MAX_PAGES", "20"))
        except Exception:
            max_pages = 20

    out = []
    seen_pages = set()
    url = BASE_URL
    page_count = 0

    while url and page_count < max_pages and url not in seen_pages:
        seen_pages.add(url)
        page_count += 1
        try:
            r = requests.get(url, headers={"User-Agent": UA}, timeout=30)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            print("OnJob error:", e)
            break

        for j in data.get("jobs", []):
            job_url = clean_url(j.get("url") or j.get("applyUrl") or "")
            if not job_url:
                continue
            out.append({
                "url": job_url,
                "title": j.get("title", ""),
                "snippet": j.get("descriptionHtml", ""),
                "description": j.get("descriptionHtml", ""),
                "company": j.get("company", ""),
                "location": j.get("location", ""),
                "country": j.get("country", ""),
                "country_code": j.get("countryCode", ""),
                "remote": bool(j.get("remote", False)),
                "work_model": "Remote" if j.get("remote") else "",
                "posted": j.get("datePosted", ""),
                "employment_type": j.get("employmentType", ""),
                "salary": j.get("salary", ""),
                "source": "onjob",
                "source_url": job_url,
            })

        nxt = data.get("next")
        if nxt:
            url = clean_url(nxt)
        else:
            url = ""

    print(f"OnJob: pages={page_count} jobs={len(out)}")
    return out
