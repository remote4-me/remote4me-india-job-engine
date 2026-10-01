# Remote4.me Job Crawler V5

India-first crawler for Customer Support, Customer Success, Technical Support, Product Support, Customer Experience, onboarding, implementation and related roles.

## What it does

- India only
- Remote + Hybrid
- Direct ATS/career links where available
- Jobicy + Remote OK public feeds
- Wellfound and Cutshort public India pages as discovery seeds
- Internet Archive + Common Crawl ATS discovery
- Direct ATS feeds for Greenhouse, Lever, Ashby, Workable, SmartRecruiters, Recruitee, Breezy, Teamtailor, BambooHR and Personio
- Company website -> Careers -> ATS discovery
- Persistent company registry in the `COMPANIES` Google Sheet
- Deduplicated jobs in `JOBS`
- Runs daily at 7:00 PM IST

## GitHub secrets

Keep the existing secrets:

- `GOOGLE_CREDENTIALS`
- `SPREADSHEET_ID`

No Brave, Serper or JobsPipe key is required.

## Important source note

The crawler intentionally does not log into or directly scrape LinkedIn/Indeed. Public ATS/career pages are preferred because they are closer to the employer's source of truth. Archive indexes can be used for discovery.

Use each source according to its current terms and attribution requirements. In particular, some public job APIs have restrictions on redistribution/aggregation; the crawler currently uses Jobicy and Remote OK as public feed sources and does not wire the Himalayas API into this V5 aggregation pipeline.


## Startup Jobs API

This version optionally uses the public Startup Jobs REST API for additional India Remote + Hybrid support jobs. The API key is **not stored in the repository**. Add a GitHub Actions secret named `STARTUP_JOBS_API_KEY`.

The free API documents a 20 requests/minute limit and a 14-day listing window. The crawler paginates the India Remote and India Hybrid streams and then applies the crawler's stricter 7-day freshness and support-role filters. Startup Jobs also requires attribution and a dofollow link back to its job URL; the crawler keeps the Startup Jobs listing URL for these records.

Because the API key was pasted into chat, regenerate/revoke that key and store the replacement only in GitHub Secrets.

## India location coverage

The India location dictionary has been expanded substantially with state/UT names, major metros, tier-2/tier-3 cities, district headquarters and common city aliases. This improves India detection on ATS feeds where the source gives only a city/state string.
