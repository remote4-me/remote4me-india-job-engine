# Remote4.me India Job Engine v5

Multi-source discovery engine for India-eligible remote customer-support and customer-success roles.

## v5 changes
- Expanded India city/location dictionary with 300+ additional aliases and tier-2/tier-3 cities.
- Remote filter recognizes Indian city + remote combinations.
- Broader support/customer-success/technical-support vocabulary.
- Search query builder expands city coverage while remaining capped by workflow limits.
- No paid search/API key is required for the keyless discovery sources.

See `SETUP.md` for GitHub Actions configuration.
# Remote4.me India Remote Support Job Engine v4

A multi-source, high-recall discovery engine for **India-only remote customer support, customer success, technical support, product support, onboarding, implementation and related roles**.

## Core goal

Discover broadly, then filter hard:

```text
Public APIs / feeds / ATS discovery
        ↓
Normalize
        ↓
India eligibility
        ↓
Remote eligibility
        ↓
Support-role relevance
        ↓
Deduplicate
        ↓
Official employer / ATS URL resolution
        ↓
Google Sheets
```

## Keyless sources included

- OnJob public JSON feed
- Jobicy public remote-jobs API
- Dev Global Jobs public API
- RemoteJobs.org public API
- YubHub support/customer-service/customer-success feeds
- We Work Remotely customer-support RSS + all-other RSS
- Existing Wayback discovery layer
- Existing optional search-engine layer

These sources have different attribution/usage requirements. Keep source attribution where a source requires it, and do not present a third-party listing as if it were originally posted by Remote4.me.

## Direct employer / ATS links

The engine tries to resolve discovery URLs to an employer career page or ATS application URL. It only requests public pages and does not bypass CAPTCHA, login walls, paywalls, robots restrictions, IP blocks or anti-bot systems.

A source may only expose its own listing URL. In that case the job is placed in `NEEDS_REVIEW` rather than inventing an employer URL.

## Search-engine layer

`BRAVE_SEARCH_API_KEY` and `SERPER_API_KEY` are optional. If neither exists, the engine skips search-engine queries and uses the keyless sources.

If a permitted search provider is later added, the engine can discover public Indeed/LinkedIn/Naukri/etc. result pages and then attempt official-source resolution. Those sites are discovery targets, not anti-bot scraping targets.

## Google Sheets tabs

- `VERIFIED_JOBS` — official employer/ATS URL resolved
- `DISCOVERED_JOBS` — every filtered India-remote-support candidate found; queued/verified/review status
- `NEEDS_REVIEW` — candidate found but official URL not resolved
- `COMPANIES` — employer/ATS domains discovered
- `RUN_LOG` — per-run counts

The storage layer now uses stable job URLs rather than the run timestamp as the deduplication key, so the same job is not appended on every run.

## GitHub Actions

The main workflow runs four times per day and can also be started manually.

Keyless sources require no new GitHub secrets. Existing Google Sheets secrets remain:

- `GOOGLE_SERVICE_ACCOUNT_JSON`
- `GOOGLE_SHEET_ID`

Optional:

- `BRAVE_SEARCH_API_KEY`
- `SERPER_API_KEY`

## Important limitation

No job engine can guarantee every job on the internet. Counts shown by job boards such as Indeed are search-result counts, not necessarily unique, current, India-eligible remote support jobs. Remote4.me therefore uses multiple sources and a verification layer rather than copying a displayed result count.
