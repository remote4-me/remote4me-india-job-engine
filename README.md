# Remote4.me Job Crawler V11

India-first, high-coverage, direct-link job discovery for Customer Support, Customer Success, Technical Support and adjacent roles.

## Architecture

- Weekly discovery: OpenJobs + India ATS directory + career-page directory + Wellfound + Cutshort + Common Crawl/Wayback. Saves `data/companies.json` and `data/boards.json`.
- Daily crawl: GitHub Actions matrix, one ATS per job, plus a company-careers shard and a feed/direct-resolution job.
- Publish: only direct employer/ATS URLs are written to `JOBS`; public feeds are used for discovery and direct-link resolution.
- `STATS` records per-source counts, errors, runtime and registry updates.
- Company/ATS registry is batch-updated and retains boards that are alive even when zero jobs match the support filter. Boards are retired only after repeated failures.

## Sources

Public feeds supported: Himalayas, Remotive, Arbeitnow, Working Nomads, We Work Remotely RSS, Jobicy, Remote OK, Startup Jobs, RemoteJobs.org; optional Adzuna/Jooble keys can be provided.

Direct ATS adapters: Greenhouse, Lever, Ashby, Workable, SmartRecruiters, Recruitee, Breezy, Teamtailor, BambooHR, Personio, Workday.

Nonstandard Indian ATS/career systems are discovered by exact company career routes and parsed with JSON-LD / public HTML where a documented unauthenticated feed is not available. Oracle Recruiting, Darwinbox, Keka and similar systems are not treated as authenticated APIs.

## Workday

Uses the public Workday CXS endpoint used by public career sites: `POST /wday/cxs/{tenant}/{site}/jobs` with a hard page size of 20. Boards are discovered from exact ATS URLs and public indexes, not guessed.

## Google Sheets

Required secrets:
- `GOOGLE_SERVICE_ACCOUNT_JSON`
- `GOOGLE_SHEET_ID`
- `STARTUP_JOBS_API_KEY`

Optional:
- `ADZUNA_APP_ID`
- `ADZUNA_APP_KEY`
- `JOOBLE_API_KEY`

The first 10 JOBS columns are kept compatible with the existing sheet. Extra columns track India Eligibility, Direct, First Seen, Last Seen, Status and Missed Runs.

## GitHub Actions

`discover.yml` runs weekly and commits updated `data/*.json`. `daily.yml` runs at 19:00 IST and uses a matrix for each ATS plus careers/feeds. Only the publish job writes to Sheets.

Run `discover.yml` once before the first daily run; the daily workflow also bootstraps discovery automatically if the manifest is empty.
