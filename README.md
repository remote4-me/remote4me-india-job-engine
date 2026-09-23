# Remote4.me India Job Discovery Engine v3

India-only, high-recall customer-support job discovery pipeline.

## What it does

Discovery sources are deliberately broad:

- Search-engine discovery through optional Brave Search API or Serper API
- Public RSS/Atom feeds when available
- Internet Archive / Wayback CDX for discovering company/career/ATS URLs
- Direct ATS discovery for Greenhouse, Lever, Ashby, Workable and other public ATS URL patterns
- A configurable list of job portals as *search targets* (not direct scraping targets)
- A growing Indian-company/domain database

Then the pipeline:

1. Normalizes discovered jobs.
2. Detects India eligibility.
3. Detects customer-support / customer-success relevance.
4. Resolves the job to an official company career/ATS page.
5. Verifies the official URL.
6. Deduplicates jobs.
7. Writes verified jobs, raw discoveries and review items to Google Sheets.
8. Stores lightweight state in `data/` so GitHub Actions stays fast.

## Safety / access model

This project does NOT bypass CAPTCHA, login walls, paywalls, robots restrictions, IP blocks or anti-bot systems.

LinkedIn, Indeed, Naukri and similar sites are used as search/discovery targets only when their public pages are surfaced through a permitted search provider or feed. The crawler does not attempt to defeat their controls.

Wayback is used primarily for company/career/ATS discovery. An archived job is NOT treated as a current job unless a current official page is found.

## Recommended search provider

For broad discovery, set ONE of:

- `BRAVE_SEARCH_API_KEY`
- `SERPER_API_KEY`

The engine works without either key, but discovery coverage will be much smaller.

## Google Sheets

Create a Google Cloud service account and download its JSON credentials.

Recommended GitHub secrets:

- `GOOGLE_SERVICE_ACCOUNT_JSON`
- `GOOGLE_SHEET_ID`

The first run creates/updates these tabs:

- `VERIFIED_JOBS`
- `DISCOVERED_JOBS`
- `NEEDS_REVIEW`
- `COMPANIES`
- `RUN_LOG`

## GitHub Actions

The workflow is scheduled several times per day and can also be run manually.

GitHub scheduled workflows run from the default branch. Keep the repository active so scheduled workflows do not become disabled in public repositories.

## Local test

```bash
python -m pip install -r requirements.txt
python -m src.main --dry-run
```

## Environment variables

Optional:

- `BRAVE_SEARCH_API_KEY`
- `SERPER_API_KEY`
- `GOOGLE_SERVICE_ACCOUNT_JSON`
- `GOOGLE_SHEET_ID`
- `JOB_MAX_AGE_DAYS=7`
- `MAX_SEARCH_QUERIES_PER_RUN=120`
- `MAX_RESULTS_PER_QUERY=20`
- `WAYBACK_MAX_URLS_PER_QUERY=300`
- `MAX_OFFICIAL_CHECKS=250`
- `REQUEST_TIMEOUT=15`
- `CRAWL_DELAY=0.8`

The defaults are intentionally conservative so a GitHub runner is unlikely to time out.

## Important

No scraper can guarantee "all" Indian jobs. The objective here is high recall while staying lightweight and avoiding anti-bot bypassing.

