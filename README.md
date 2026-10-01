# Remote4.me India Remote / Hybrid Job Engine v4

A lightweight GitHub Actions crawler that does **not require Brave, Serper, Google Custom Search, or another paid search API**.

## What it does

1. Uses free public job feeds: Himalayas, Jobicy and Remote OK.
2. Automatically discovers public ATS company boards from the Internet Archive (Wayback Machine), with Common Crawl as a fallback.
3. Validates discovered ATS board slugs against live public ATS endpoints.
4. Crawls the live ATS directly for fresh jobs.
5. Filters for India + Remote OR Hybrid + support/success/technical-support related roles.
6. Writes new jobs to the existing Google Sheet.
7. Caches discovered ATS boards in `data/boards.json` so later runs don't rediscover everything.
8. Refreshes ATS discovery every 7 days by default.

## ATS platforms

The initial registry includes public-board endpoints for:

- Greenhouse
- Lever
- Ashby
- SmartRecruiters
- Recruitee
- Breezy HR
- Teamtailor
- Workable

The primary three (Greenhouse, Lever, Ashby) are especially valuable because their published job APIs are public and keyless. Greenhouse documents unauthenticated GET job-board access; Lever documents its public postings endpoint; Ashby documents its public job-posting endpoint.

## No paid search API

The workflow does NOT use:

- Brave Search API
- Serper API
- Google Custom Search API
- JobsPipe
- Remotive

It uses the Internet Archive's public CDX index for ATS-board discovery and Common Crawl only as a fallback.

## GitHub secrets

Only these are required for the Google Sheets output:

- `GOOGLE_SERVICE_ACCOUNT_JSON`
- `GOOGLE_SHEET_ID`

No search API key is required.

## Google Sheet

The crawler uses the existing `JOBS` sheet if it exists. It does not need a new spreadsheet.

Columns:

`Added | Job Title | Company | Location | Work Type | Posted | Job URL | Source | Score | Matched Keywords`

## Filters

Accepted:

- India
- Remote
- Hybrid
- customer support / customer service
- customer success
- customer experience
- customer operations
- customer onboarding / implementation
- customer adoption / retention / enablement
- technical support
- product support
- application / software / SaaS support
- IT support / helpdesk / service desk
- L1 / L2 support
- chat / email / phone support
- client / member / user support or success
- support operations / quality / training / knowledge
- technical account / customer solutions where the role is support-oriented

Rejected:

- USA-only / other-country-only jobs
- office-only / on-site-only jobs
- old jobs outside the 7-day window when a usable posting date exists
- unrelated roles that only mention a support keyword incidentally

## ATS discovery cache

`data/boards.json` is committed back to the repository by GitHub Actions. This is deliberate: it lets the next run reuse thousands of discovered company board slugs instead of starting over.

A scheduled run refreshes discovery every 7 days. You can force a refresh from GitHub Actions → Run workflow → `force_ats_refresh = true`.

## Important limitation

ATS vendors generally expose a public API **per company board**, not a universal "all companies" endpoint. The discovery layer therefore uses public web-archive indexes to find company board slugs and then validates them against the live ATS API. This can miss a brand-new company until its board appears in an archive. That is why the board cache and periodic refresh are important.

## Source/attribution rules

Before publishing third-party feed results on Remote4.me, follow each source's current terms and attribution requirements. For direct ATS results, the `Job URL` is the employer's own ATS/application page.
