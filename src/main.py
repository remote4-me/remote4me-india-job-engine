import argparse
import json
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
from src.filters.india import is_india_job, india_score
from src.filters.remote import india_remote_job
from src.filters.support import relevance
from src.resolver.official import resolve
from src.storage.sheets import write
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
        "url": url,
        "title": title.strip(),
        "snippet": desc,
        "description": desc,
        "company": company,
        "location": location,
        "country_code": country_code,
        "remote": remote,
        "work_model": work_model,
        "posted": raw.get("posted", "") or "",
        "employment_type": raw.get("employment_type", "") or "",
        "salary": raw.get("salary", "") or "",
        "company_website": raw.get("company_website", "") or "",
        "source": raw.get("source", "unknown"),
        "source_url": clean_url(raw.get("source_url", "") or url),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    maxverify = safe_int("MAX_OFFICIAL_CHECKS", 350)
    max_search_queries = safe_int("MAX_SEARCH_QUERIES_PER_RUN", 80)
    max_results_per_query = safe_int("MAX_RESULTS_PER_QUERY", 20)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    raw = []
    errors = 0
    source_counts = {}

    # Keyless public discovery sources.
    source_loaders = [
        ("onjob", fetch_onjob),
        ("jobicy", fetch_jobicy),
        ("devglobal", fetch_devglobal),
        ("remotejobs.org", fetch_remotejobs),
        ("yubhub", fetch_yubhub),
        ("weworkremotely", fetch_wwr),
    ]

    for name, loader in source_loaders:
        try:
            rows = loader()
            raw.extend(rows)
            source_counts[name] = len(rows)
        except Exception as e:
            errors += 1
            source_counts[name] = 0
            print(f"{name} error: {e}")

    # Optional search-engine layer. It is completely skipped when no key is configured.
    import os
    search_enabled = bool(os.getenv("BRAVE_SEARCH_API_KEY", "").strip() or os.getenv("SERPER_API_KEY", "").strip())
    search_count = 0
    if search_enabled:
        queries = build_queries(KW["primary"], CITIES, TARGETS)[:max_search_queries]
        for q in queries:
            try:
                rows = search(q, max_results_per_query)
                raw.extend(rows)
                search_count += len(rows)
            except Exception as e:
                errors += 1
                print("search error", q, e)
    else:
        print("Search engine layer skipped: no Brave/Serper key configured")
    source_counts["search"] = search_count

    # Wayback is a company/ATS discovery layer, not a current-job source.
    seed_file = BASE / "data/seed_domains.txt"
    seed_domains = [x.strip() for x in seed_file.read_text(encoding="utf-8").splitlines()
                    if x.strip() and not x.startswith("#")] if seed_file.exists() else []
    if seed_domains:
        try:
            rows = discover_from_seed_domains(seed_domains, per_domain=safe_int("WAYBACK_MAX_URLS_PER_QUERY", 100))
            raw.extend(rows)
            source_counts["wayback"] = len(rows)
        except Exception as e:
            errors += 1
            source_counts["wayback"] = 0
            print("wayback error", e)

    unique = {}
    for raw_item in raw:
        p = normalize(raw_item)
        if not p["url"] or not p["title"]:
            continue
        # Stable fingerprint before official resolution.
        key = fingerprint(p["title"], p["company"], p["location"], p["url"])
        unique[key] = p

    india = []
    remote = []
    support = []
    for p in unique.values():
        combined = " ".join([p["title"], p["location"], p["description"]])
        if not is_india_job(p["title"], p["location"], p["description"], p["country_code"]):
            continue
        india.append(p)
        if not india_remote_job(
            p["title"], p["location"], p["description"], p["work_model"], p["remote"], p["country_code"]
        ):
            continue
        remote.append(p)
        score, hits = relevance(p["title"], p["description"], p["company"])
        if score >= 5:
            p["score"] = score
            p["hits"] = hits
            support.append(p)

    # Higher relevance first; prefer source records with richer metadata.
    support.sort(key=lambda x: (
        x.get("score", 0),
        bool(x.get("company")),
        bool(x.get("posted")),
        bool(x.get("description")),
    ), reverse=True)

    verified = []
    review = []
    discovered = []
    companies = []
    seen_official = set()
    seen_discovery = set()

    # Keep the entire filtered discovery pool in DISCOVERED_JOBS.
    # Only the first MAX_OFFICIAL_CHECKS are fetched for employer/ATS verification
    # during this run; the remainder stay queued for a later run.
    verify_queue = support[:maxverify]

    for p in support:
        discovery_url = p["url"]
        if discovery_url in seen_discovery:
            continue
        seen_discovery.add(discovery_url)

        discovered.append([
            now, p["title"], p["company"], p["location"], p["posted"],
            discovery_url, "", p["source"], "queued_for_verification",
        ])

    for p in verify_queue:
        discovery_url = p["url"]
        official = None
        try:
            official = resolve(
                discovery_url,
                p["company"],
                p.get("company_website", ""),
            )
        except Exception as e:
            errors += 1
            print("resolver error", discovery_url, e)

        # Update the matching discovered row in memory.
        for row in discovered:
            if row[5] == discovery_url:
                row[6] = official or ""
                row[8] = "verified" if official else "needs_review"
                break

        if official:
            official_key = official.rstrip("/").lower()
            if official_key not in seen_official:
                seen_official.add(official_key)
                verified.append([
                    now,
                    p["title"],
                    p["company"],
                    p["location"] or "India",
                    "Remote",
                    p["posted"],
                    official,
                    p["source"],
                    ", ".join(p.get("hits", [])),
                ])
                companies.append([
                    p["company"], root_domain(official), official, "",
                    "India", p["source"], now,
                ])
        else:
            review.append([
                now, p["title"], p["company"], p["location"], discovery_url,
                "Remote India support job found; official employer/ATS URL not resolved yet",
                p["source"],
            ])

    result = {
        "verified": verified,
        "discovered": discovered,
        "review": review,
        "companies": companies,
        "log": [
            now,
            len(unique),
            len(india),
            len(support),
            len(verified),
            len(review),
            errors,
        ],
    }

    print(json.dumps({
        "sources": source_counts,
        "raw": len(raw),
        "unique": len(unique),
        "india": len(india),
        "remote_india": len(remote),
        "support": len(support),
        "verified": len(verified),
        "review": len(review),
        "errors": errors,
    }, indent=2))

    if not args.dry_run:
        write(result)


if __name__ == "__main__":
    main()
