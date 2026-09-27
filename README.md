# Remote4.me India Direct Employer / ATS Job Engine

This is the next-stage backend for remote4.me: a discovery + direct employer/ATS crawler for **India-only remote customer support and customer-success roles**.

## Architecture

```text
Public feeds / permitted discovery / Wayback
                    ↓
              Normalize + dedupe
                    ↓
            India + remote eligibility
                    ↓
          Support-role relevance
                    ↓
        Resolve employer / ATS source
                    ↓
          Persistent COMPANIES index
                    ↓
      Greenhouse / Lever / Ashby / Workable
                    ↓
          Crawl current live jobs
                    ↓
       Strict India + remote + support
                    ↓
         Official application URL
                    ↓
              Google Sheets
                    ↓
          Manual WordPress review
```

The important difference from a simple aggregator is that a feed result is treated as a **discovery signal**. Once the employer/ATS is identified, the engine stores that source in `COMPANIES` and crawls the public employer/ATS source on later runs.

## Current direct ATS adapters

- Greenhouse
- Lever
- Ashby
- Workable

The crawler only uses publicly accessible pages/endpoints and does not bypass CAPTCHA, login walls, paywalls, robots restrictions, IP blocks, or anti-bot protections.

## Quality rules

- India eligibility is based primarily on structured location/country data and explicit India eligibility.
- A foreign-only structured location is rejected even if the description contains incidental India text.
- Direct ATS jobs require a support/customer-success style signal in the title.
- Obvious sales, software-engineering, developer, recruiting, marketing, and other unrelated titles are rejected unless they have a genuine support title signal.
- Resolver results such as `/policies`, `/privacy`, `/about`, `/addons`, `/blog`, etc. are not treated as official job sources.
- Direct ATS application URLs are trusted only when they come from the detected public ATS source.

## Google Sheets

- `VERIFIED_JOBS` — jobs with an official employer/ATS application URL
- `DISCOVERED_JOBS` — filtered discovery candidates and their verification status
- `NEEDS_REVIEW` — candidates that still need source verification
- `COMPANIES` — persistent employer/ATS crawl queue
- `RUN_LOG` — run statistics

## Scaling model

The crawler is intentionally incremental. Each successful employer/ATS discovery is persisted in `COMPANIES`. Later runs revisit those sources instead of rediscovering them from scratch. Increase `MAX_ATS_COMPANIES` only after result quality is clean.

This is the same general sourcing pattern publicly described by remote-job platforms: employer career pages and ATS sources, normalized/filtered, deduplicated, checked for a usable application destination, and refreshed over time. It does not reproduce any proprietary implementation.
