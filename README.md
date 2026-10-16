# Remote4.me V13.7 — India-Only Support Job Discovery Crawler

India-only Customer Support / Customer Success / Technical Support crawler. It searches nationwide across Indian cities and supports eligible India-wide remote roles; it does not intentionally collect US jobs.

## Sources

### Direct/public feeds used as discovery
- Himalayas
- Remotive
- Arbeitnow
- Working Nomads
- We Work Remotely RSS
- Jobicy
- Remote OK
- Startup Jobs
- Job Opportunities API
- Jobvetta (India-wide support-role search; 4 rotating queries/day by default, within its 50-request daily quota)
- RemoteJobs.org / optional Adzuna / Jooble when configured

Feed results are never accepted as final JOBS URLs when the URL remains an aggregator. The crawler attempts to resolve them to an ATS/employer URL and drops unresolved aggregator URLs.

### Direct ATS / career crawling
- Greenhouse
- Lever
- Ashby
- Workable
- SmartRecruiters
- Recruitee
- Breezy
- Teamtailor
- BambooHR
- Personio
- Workday
- Nonstandard / company career routes
- Indian/enterprise ATS detection: Darwinbox, Keka, Zoho Recruit, Freshteam, Oracle Recruiting, Rippling, Pinpoint, Phenom, Eightfold, Avature, Jobvite, JazzHR, Bullhorn, CEIPAL, Vincere, UKG, Zwayam, PeopleStrong, HROne, Manatal, Fountain, Jobylon, TalentLyft, Recruiterflow, iSmartRecruit, Comeet, InHire, Paylocity, Crelate, iCIMS, Taleo, ADP, SuccessFactors.

For platforms without a verified public API, V12 uses the public career URL plus a targeted Playwright/XHR fallback instead of inventing an undocumented API endpoint.

## Job Opportunities API

Set the GitHub repository secret:

`JOB_OPPORTUNITIES_API_KEY`

V12 uses a safe default of 30 returned records/day, about 900 records per 30-day month, because the Explore plan is metered at 1,000 records/month.

The API query is restricted to India, remote/hybrid, live roles, ATS/company-career provenance, and rotates support-related title terms.

## Required GitHub secrets

- `GOOGLE_SERVICE_ACCOUNT_JSON`
- `GOOGLE_SHEET_ID`
- `STARTUP_JOBS_API_KEY`
- `JOB_OPPORTUNITIES_API_KEY`
- `JOBVETTA_API_KEY` (optional; add from Jobvetta dashboard)

## Daily workflow

`.github/workflows/daily.yml` runs at 19:00 IST and uses the committed discovery manifests.

The career job installs Chromium only for the Playwright fallback.

## Important source policy

Himalayas, Startup Jobs, Remotive, Jobicy, Remote OK and WWR have their own attribution/usage requirements. V12 treats them as discovery sources and prefers the employer/ATS URL. If a direct employer/ATS URL cannot be resolved, the aggregator URL is excluded from JOBS.

### Jobvetta India-wide coverage

The daily feeds job rotates four nationwide support-related searches (no city restriction) and requests up to 10 records per query, for a maximum of 40 search results/day. It performs at most 20 optional detail lookups/day to recover full descriptions and employer/ATS links while staying below the 50-call daily limit under normal use. It only accepts jobs explicitly marked Remote or Hybrid, rejects unknown work models, and treats Jobvetta-hosted pages as discovery leads unless an external employer/ATS URL is explicitly present in the API response. The API key stays in GitHub Secrets.

### LinkedIn and Indeed

The crawler does not log in to or directly scrape LinkedIn/Indeed pages, bypass CAPTCHAs, or evade access controls. Search discovery is aimed at employer career pages and ATS listings so the final job URL can be verified at the source. Use official/licensed APIs or written permission if you want direct ingestion from either platform.


## Applying this upgrade to an existing private repository

This ZIP is an **overlay** for your existing repository, not a replacement for the entire repository. Copy `crawler.py` to the repository root and `.github/workflows/daily.yml` to `.github/workflows/daily.yml`. Keep your existing `requirements.txt`, `config/` files, discovery manifests, and other source files. Review the diff before committing.

## Required setup

Add `JOBVETTA_API_KEY` as a GitHub Actions repository secret if you have a Jobvetta API key. It is optional; the crawler skips Jobvetta when the secret is absent. Keep all API keys in GitHub Secrets, never hard-code them in the source.

## Verification status

The delivered Python file passed `py_compile`, and the workflow YAML parsed successfully. This does not replace a live GitHub Actions run: API responses and credentials must be verified in your repository.
