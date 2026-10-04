# Remote4.me V12 — Direct Job + India ATS + JS Career Crawler

India-first Customer Support / Customer Success / Technical Support crawler.

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

## Daily workflow

`.github/workflows/daily.yml` runs at 19:00 IST and uses the committed discovery manifests.

The career job installs Chromium only for the Playwright fallback.

## Important source policy

Himalayas, Startup Jobs, Remotive, Jobicy, Remote OK and WWR have their own attribution/usage requirements. V12 treats them as discovery sources and prefers the employer/ATS URL. If a direct employer/ATS URL cannot be resolved, the aggregator URL is excluded from JOBS.
