# Remote4.me crawler V14.8 — search-first, cleaner ATS discovery

This package upgrades the existing Remote4.me crawler without requiring new paid search API keys. Existing API providers remain optional and are used when configured.

## What changed

- **Keyless SERP fallback:** the `--discover` stage runs a bounded public Bing RSS search pass even when search-provider API secrets are absent. Queries rotate across India-first remote/hybrid jobs, ATS pages, job boards, and public social hiring posts. Four of the default twelve keyless queries are reserved for social searches each run. This is a fallback, not a guaranteed search service; public RSS endpoints can throttle or change.
- **Individual job-page seeds:** exact ATS vacancy URLs from SERP results are queued as individual high-priority career seeds instead of being expanded into a whole ATS-board crawl. `/apply` variants are canonicalized to avoid crawling both forms of the same Workday vacancy.
- **Public social discovery:** search-engine-indexed public LinkedIn, X/Twitter, and Reddit hiring posts can appear as review-only discovery leads. They are never treated as verified direct-apply jobs automatically.
- **Cleaner ATS route detection:** platform marketing, app, help, developer, documentation, static asset, candidate portal, event, and form URLs are filtered. Manatal's general marketing/support/app domains are no longer treated as career boards; only the job-board host is accepted.
- **Better fallback coverage:** plausible career pages are not dropped during preflight merely because they return 403/429/5xx or a transient network error. Clearly dead 404/410 URLs and non-HTML assets are still rejected. Later parsing, role, location, workplace, freshness, and link checks remain in force.
- **Lower browser contention:** Playwright browser concurrency is capped to reduce pipe-closure errors from launching too many Chromium instances at once.
- **Verification budget fix:** already-direct rows no longer consume the limited budget reserved for verifying aggregator-to-employer links.
- **Better logs:** the crawler prints the configured search providers separately from the no-key fallback, makes clear that SERP discovery runs in the `--discover` prepare stage, and reports useful ATS routes rather than raw route counts.
- **Existing sheet structure preserved:** `JOBS` remains remote-only; India-only hybrid jobs stay in `HYBRID`; discovery leads continue to go to `DISCOVERY` and `HYBRID_DISCOVERY`.

## Install

Replace these files in your repository:

- `crawler.py` → repository root `crawler.py`
- `daily.yml` → `.github/workflows/daily.yml`
- `discover.yml` → `.github/workflows/discover.yml` (or your existing weekly-discovery workflow path)

Keep your existing `requirements.txt` and `data/` files. The workflow expects the existing Google Sheets secrets (`GOOGLE_SERVICE_ACCOUNT_JSON`, `GOOGLE_SHEET_ID`) and optionally uses whatever SERP/feed secrets you already have. No new secret is required for the public RSS fallback.

Commit and push the files, then run the **daily workflow** manually once. Check the `prepare` job's `SEARCH DISCOVERY` / `SEARCH PROVIDER RESULTS` lines, the career job's `CAREER SUMMARY` line, and the final `VERIFIED AGGREGATOR APPLY LINKS` / `FEEDS DIRECT CANDIDATES` counters.

## Important limitations

- The no-key fallback uses a public Bing RSS-style endpoint, not Google HTML scraping. It may be blocked, throttled, or changed by the provider; no-key search results are not guaranteed on every run.
- Public social posts are discovery leads only. The crawler does not log in, bypass access controls, or publish a job solely because a social post exists.
- Search result snippets and ATS URLs are candidates, not proof of eligibility. The original listing must still support the target role, India eligibility, remote/hybrid arrangement, freshness, and a valid direct job URL before it enters the final direct-job output.
- This package was syntax-checked, its workflow YAML parsed, and targeted logic smoke tests were run. It was **not** run as a live GitHub Action against your Google Sheet from this environment, so actual live job yield and provider availability remain unverified.
