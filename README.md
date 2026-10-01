# Remote4.me India Remote Job Engine

Lightweight GitHub Actions crawler for **India + Remote OR Hybrid** customer support, customer success, technical support, product support and related roles.

## Data sources

Free/public sources currently wired in:

- Himalayas public JSON API
- Jobicy public API
- Remote OK public JSON feed
- Optional JobsPipe API (API key + quota)
- Brave Search discovery
- Serper discovery
- ATS discovery for Greenhouse, Lever, Ashby, Workable, SmartRecruiters, Recruitee, Teamtailor and more

## Important attribution

If you publish data from a source, follow that source's terms and attribution requirements. Himalayas requires a visible link back to Himalayas and source mention; Jobicy asks users of its public API to keep the Jobicy listing URL unless using its paid direct-application API; Remotive requires attribution/linking and has restrictions on third-party redistribution. Remote OK asks aggregators to credit Remote OK and link to the original job post. See each source's current terms before publishing.

## GitHub secrets

Create these repository secrets:

- `GOOGLE_SERVICE_ACCOUNT_JSON`
- `GOOGLE_SHEET_ID`
- `BRAVE_SEARCH_API_KEY`
- `SERPER_API_KEY`
- `JOBSPIPE_API_KEY` (optional)

The crawler works without JobsPipe. It is included because it can add another normalized source with 30+ underlying sources, but its free quota is limited.

## Google Sheet

Share your Google Sheet with the service-account email contained in your JSON.

The crawler creates:

### JOBS
Added, Job Title, Company, Location, Work Type, Posted, Job URL, Source, Score, Matched Keywords

### COMPANIES
Company, Domain, Career/ATS URL, ATS, Last Checked, Jobs Found, Status

## Filtering

A job must normally satisfy:

- India eligibility detected
- Remote OR Hybrid eligibility detected
- No strong office/on-site-only signal
- Fresh within 7 days
- Support/success-related score >= 35

The role dictionary is deliberately broad and can be expanded in `config/keywords.json`.

## Run locally

```bash
pip install -r requirements.txt
python crawler.py
```

For a safe test:

```bash
DRY_RUN=true python crawler.py
```

## Notes

This project intentionally prefers public APIs/feeds and search-discovered career/ATS URLs rather than scraping login-protected sites. Search discovery is a supplement; direct ATS/API data is preferred when available.


## ATS discovery diagnostic

The crawler prints `ATS URLs discovered: N` and `ATS FOUND:` lines. If it says no search API key is configured, add `BRAVE_SEARCH_API_KEY` and/or `SERPER_API_KEY` to GitHub Actions secrets. API feeds and ATS discovery are separate pipelines.
