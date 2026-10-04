# Remote4.me India Job Crawler V7

India-first public job discovery crawler for Remote4.me. It keeps only Customer Support, Customer Success, Technical Support, Product Support and closely related roles that are explicitly India + Remote/Hybrid, with a 7-day freshness window.

## Sources

### Public APIs / feeds
- Himalayas public JSON search API (India + support queries)
- Remotive public API
- Arbeitnow public API
- Working Nomads public JSON feed
- We Work Remotely public RSS feed
- Jobicy public API
- Remote OK public JSON feed
- Startup Jobs API (free key)

### Discovery
- Wellfound India role pages
- Cutshort India role pages
- Company website -> Careers/Jobs -> ATS discovery
- Internet Archive + Common Crawl ATS discovery

### Direct ATS/career crawling
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
- Generic public career-page fallback

## GitHub secrets

Use these repository secrets:

```text
GOOGLE_SERVICE_ACCOUNT_JSON
GOOGLE_SHEET_ID
STARTUP_JOBS_API_KEY
```

The workflow maps the first two into the environment names expected by the crawler.

## Schedule

Daily at 7:00 PM IST (`30 13 * * *`).

## Important source handling

Aggregator/API records are discovery/review inputs. Remote4.me's workflow is manual review; use the employer's own public career/ATS URL for the final job post whenever it can be verified. Some public feeds have attribution or redistribution conditions, so review the applicable source terms before publishing any sourced listing.

## No paid search APIs

No Brave, Serper, JobsPipe or other paid search API is required.
