import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from src.discovery.devglobal import fetch_jobs as fetch_devglobal
from src.discovery.jobicy import fetch_jobs as fetch_jobicy
from src.discovery.onjob import fetch_jobs as fetch_onjob
from src.discovery.remotejobs import fetch_jobs as fetch_remotejobs
from src.discovery.wwr import fetch_jobs as fetch_wwr
from src.discovery.yubhub import fetch_jobs as fetch_yubhub
from src.discovery.search import search, build_queries
from src.discovery.wayback import discover_from_seed_domains
from src.filters.india import is_india_job
from src.filters.remote import india_remote_job
from src.filters.support import relevance
from src.filters.direct import direct_support_job, direct_india_remote
from src.resolver.official import resolve
from src.storage.sheets import write, read_companies
from src.ats.crawler import discover_and_crawl
from src.utils import clean_url, domain, root_domain, fingerprint, safe_int

BASE = Path(__file__).resolve().parents[1]
KW = json.loads((BASE / "config/keywords.json").read_text(encoding="utf-8"))
CITIES = (BASE / "config/india_cities.txt").read_text(encoding="utf-8").splitlines()
TARGETS = [x.strip() for x in (BASE / "config/search_targets.txt").read_text(encoding="utf-8").splitlines() if x.strip()]


def extract_company(url, title="", supplied=""):
    if supplied:
        return supplied.strip()
    d = domain(url)
    if not d:
        return ""
    bits = d.split(".")
    return bits[-2].replace("-", " ").title() if len(bits) >= 2 else bits[0].title()


def normalize(raw):
    url = clean_url(raw.get("url", ""))
    title = raw.get("title", "") or ""
    desc = raw.get("description", "") or raw.get("snippet", "") or ""
    company = extract_company(url, title, raw.get("company", ""))
    location = raw.get("location", "") or raw.get("country", "") or ""
    country_code = raw.get("country_code", "") or ""
    remote = bool(raw.get("remote", False))
    work_model = raw.get("work_model", "") or ""
    if not work_model and remote:
        work_model = "Remote"
    return {
        "url": url, "title": title.strip(), "snippet": desc, "description": desc,
        "company": company, "location": location, "country_code": country_code,
        "remote": remote, "work_model": work_model, "posted": raw.get("posted", "") or "",
        "employment_type": raw.get("employment_type", "") or "", "salary": raw.get("salary", "") or "",
        "company_website": raw.get("company_website", "") or "", "source": raw.get("source", "unknown"),
        "source_url": clean_url(raw.get("source_url", "") or url), "apply_url": clean_url(raw.get("apply_url", "") or url),
    }


def company_row(company, domain_url, career_url, ats, board, source, now):
    return [company, root_domain(domain_url or career_url or board), career_url or board, ats, board, "India", source, now]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    maxverify = safe_int("MAX_OFFICIAL_CHECKS", 250)
    max_search_queries = safe_int("MAX_SEARCH_QUERIES_PER_RUN", 80)
    max_results_per_query = safe_int("MAX_RESULTS_PER_QUERY", 20)
    max_ats_companies = safe_int("MAX_ATS_COMPANIES", 120)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    raw, errors, source_counts = [], 0, {}
    source_loaders = [
        ("onjob", fetch_onjob), ("jobicy", fetch_jobicy), ("devglobal", fetch_devglobal),
        ("remotejobs.org", fetch_remotejobs), ("yubhub", fetch_yubhub), ("weworkremotely", fetch_wwr),
    ]
    for name, loader in source_loaders:
        try:
            rows = loader(); raw.extend(rows); source_counts[name] = len(rows)
        except Exception as e:
            errors += 1; source_counts[name] = 0; print(f"{name} error: {e}")

    search_enabled = bool(os.getenv("BRAVE_SEARCH_API_KEY", "").strip() or os.getenv("SERPER_API_KEY", "").strip())
    search_count = 0
    if search_enabled:
        for q in build_queries(KW["primary"], CITIES, TARGETS)[:max_search_queries]:
            try:
                rows = search(q, max_results_per_query); raw.extend(rows); search_count += len(rows)
            except Exception as e:
                errors += 1; print("search error", q, e)
    else:
        print("Search engine layer skipped: no Brave/Serper key configured")
    source_counts["search"] = search_count

    seed_file = BASE / "data/seed_domains.txt"
    seed_domains = [x.strip() for x in seed_file.read_text(encoding="utf-8").splitlines() if x.strip() and not x.startswith("#")] if seed_file.exists() else []
    if seed_domains:
        try:
            rows = discover_from_seed_domains(seed_domains, per_domain=safe_int("WAYBACK_MAX_URLS_PER_QUERY", 100))
            raw.extend(rows); source_counts["wayback"] = len(rows)
        except Exception as e:
            errors += 1; source_counts["wayback"] = 0; print("wayback error", e)

    unique = {}
    for item in raw:
        p = normalize(item)
        if not p["url"] or not p["title"]:
            continue
        unique[fingerprint(p["title"], p["company"], p["location"], p["url"])] = p

    support = []
    india_count = 0
    remote_count = 0
    for p in unique.values():
        if not is_india_job(p["title"], p["location"], p["description"], p["country_code"]):
            continue
        india_count += 1
        if not india_remote_job(p["title"], p["location"], p["description"], p["work_model"], p["remote"], p["country_code"]):
            continue
        remote_count += 1
        score, hits = relevance(p["title"], p["description"], p["company"])
        if score >= 5:
            p["score"], p["hits"] = score, hits
            support.append(p)
    support.sort(key=lambda x: (x.get("score", 0), bool(x.get("company")), bool(x.get("posted")), bool(x.get("description"))), reverse=True)

    verified, review, discovered, companies = [], [], [], []
    seen_official, seen_discovery = set(), set()

    for p in support:
        if p["url"] in seen_discovery: continue
        seen_discovery.add(p["url"])
        discovered.append([now, p["title"], p["company"], p["location"], p["posted"], p["url"], "", p["source"], "queued_for_verification"])

    for p in support[:maxverify]:
        official = None
        try: official = resolve(p["url"], p["company"], p.get("company_website", ""))
        except Exception as e: errors += 1; print("resolver error", p["url"], e)
        for row in discovered:
            if row[5] == p["url"]:
                row[6] = official or ""; row[8] = "verified" if official else "needs_review"; break
        if official:
            key = official.rstrip("/").lower()
            if key not in seen_official:
                seen_official.add(key)
                verified.append([now, p["title"], p["company"], p["location"] or "India", "Remote", p["posted"], official, p["source"], ", ".join(p.get("hits", []))])
                companies.append(company_row(p["company"], official, official, "", "", p["source"], now))
        else:
            review.append([now, p["title"], p["company"], p["location"], p["url"], "Remote India support job found; official employer/ATS URL not resolved yet", p["source"]])

    # DIRECT ATS LAYER: crawl persistent companies plus companies discovered in this run.
    persistent = [] if args.dry_run else read_companies(limit=max_ats_companies * 2)
    queue, qseen = [], set()
    for c in persistent:
        key = "|".join(str(c.get(k, "")).lower() for k in ("company", "domain", "career_url", "ats", "board"))
        if key not in qseen:
            qseen.add(key); queue.append(c)
    for row in companies:
        c = {"company": row[0], "domain": row[1], "career_url": row[2], "ats": row[3], "board": row[4]}
        key = "|".join(str(c.get(k, "")).lower() for k in ("company", "domain", "career_url", "ats", "board"))
        if key not in qseen:
            qseen.add(key); queue.append(c)

    direct_jobs, direct_companies, direct_stats = [], [], {"companies_checked": 0, "ats_detected": 0, "jobs_found": 0, "india_remote_support": 0}
    for c in queue[:max_ats_companies]:
        direct_stats["companies_checked"] += 1
        try:
            detected, jobs = discover_and_crawl(c.get("company", ""), c.get("domain", ""), c.get("career_url", ""), c.get("ats", ""), c.get("board", ""))
            ats, board, board_url = detected
            if ats:
                direct_stats["ats_detected"] += 1
                direct_companies.append(company_row(c.get("company", ""), c.get("domain", ""), c.get("career_url", "") or board_url, ats, board_url or board, "direct_ats", now))
            for job in jobs:
                direct_stats["jobs_found"] += 1
                if not direct_india_remote(job):
                    continue
                ok, score, hits = direct_support_job(job.get("title", ""), job.get("description", ""), job.get("company", ""))
                if not ok:
                    continue
                direct_stats["india_remote_support"] += 1
                apply_url = job.get("apply_url") or job.get("url")
                if not apply_url or apply_url.lower() in seen_official:
                    continue
                seen_official.add(apply_url.lower())
                direct_jobs.append([now, job["title"], job["company"], job["location"] or "India", "Remote", job["posted"], apply_url, job["source"], ", ".join(hits)])
        except Exception as e:
            errors += 1
            print("direct ATS error", c.get("company"), e)

    verified.extend(direct_jobs)
    companies.extend(direct_companies)

    result = {
        "verified": verified, "discovered": discovered, "review": review, "companies": companies,
        "log": [now, len(unique) + direct_stats["jobs_found"], india_count, len(support) + direct_stats["india_remote_support"], len(verified), len(review), errors],
    }

    print(json.dumps({
        "sources": source_counts, "raw": len(raw), "unique": len(unique), "india": india_count,
        "remote_india": remote_count, "support": len(support), "verified": len(verified),
        "review": len(review), "errors": errors, "direct_ats": direct_stats,
    }, indent=2))
    if not args.dry_run:
        write(result)


if __name__ == "__main__":
    main()
