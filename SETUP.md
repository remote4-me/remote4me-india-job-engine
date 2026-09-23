# Setup

## 1. Create repository

Create a new GitHub repository, for example:

`remote4me-india-job-engine`

Upload this project.

## 2. Search provider

For broad discovery, use one of these:

### Option A — Brave Search API
Create an API key and add it to GitHub Secrets as:

`BRAVE_SEARCH_API_KEY`

### Option B — Serper
Add:

`SERPER_API_KEY`

You only need one.

The engine first tries Brave and falls back to Serper.

## 3. Google Sheets

Create a Google Cloud service account with access to Google Sheets.

Share your destination Google Sheet with the service account email as Editor.

Add these GitHub repository secrets:

`GOOGLE_SERVICE_ACCOUNT_JSON`

Paste the complete service-account JSON as the secret value.

`GOOGLE_SHEET_ID`

This is the long ID from:

https://docs.google.com/spreadsheets/d/THIS_PART/edit

## 4. Run manually first

GitHub → Actions → Test → Run workflow.

Then:

GitHub → Actions → Remote4.me India Job Engine → Run workflow.

Check the Action logs.

## 5. Search volume

The default run uses 120 queries × up to 20 results.

Increase carefully:

`MAX_SEARCH_QUERIES_PER_RUN=180`

`MAX_OFFICIAL_CHECKS=300`

Do NOT make these unlimited. A lightweight crawler with a high-quality resolver is more reliable than a huge uncontrolled crawl.

## 6. Wayback

Wayback is included as a discovery layer.

For company discovery, add domains to:

`data/seed_domains.txt`

The CDX API supports domain/prefix queries, filtering and pagination. The engine keeps the Wayback workload intentionally small per run.

## 7. Adding more portals

Edit:

`config/search_targets.txt`

Add a domain only if it is useful as a discovery target and its public access/terms permit the intended use.

Do not add login-only or anti-bot bypass targets.

## 8. Expanding Indian locations

`config/india_cities.txt` contains a broad city list.

You can add more city names at any time.

The location detector also accepts India / Remote India / Work-from-home India.

## 9. Important behavior

The engine does NOT claim that every discovered job is current.

Only a job with a resolvable current official employer/ATS URL goes to `VERIFIED_JOBS`.

Unresolved candidates remain in `NEEDS_REVIEW`.

This is deliberate: high recall without silently publishing third-party or stale links.
