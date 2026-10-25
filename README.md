# Remote4.me India Support Job Engine — Stable Workflow Pack

This release is a conservative stabilization of the uploaded crawler, not a wholesale rewrite. It preserves the existing `crawler.py`, Google Sheets integration, data manifests, ATS adapters, and manual-review workflow while reducing the discovery/page-scan budgets that previously risked consuming the GitHub Actions time limit.

## Scope

- Target: India-eligible remote roles and India-based hybrid roles.
- Role families: customer support, customer service, technical/product/application support, IT/helpdesk/service desk, customer success, onboarding, implementation, customer experience, support operations and related leadership/account roles. Actual inclusion depends on the existing crawler's title/description rules.
- Sources: existing public ATS adapters, configured public feeds, employer career pages, and optional search discovery.
- LinkedIn: discovery leads only where configured; listings should be verified against the employer/ATS page. X/Twitter is not enabled as a source in this workflow.
- Publishing: Google Sheets review queue only; this does not publish jobs to WordPress.
- Schedule: daily workflow at 17:00 India Standard Time (`30 11 * * *` UTC). Weekly discovery runs Sundays at 09:00 IST.

## What changed in this package

1. Bounded daily discovery: reduced search queries, URL cap, candidate cap, validation-per-ATS and discovery concurrency.
2. Archive/Common Crawl refresh is throttled instead of being scheduled for every run.
3. Reduced career-page and homepage work so the direct ATS matrix remains the primary daily collection pass.
4. Preserved parallel per-ATS jobs and the existing remote/hybrid Google Sheets merge.
5. Added local regression checks for workflow schedules, time budgets, and key safety expectations.
6. Replaced the old V15.0 README with current setup and limitations.

## Install

1. Back up your current repository and `data/` folder.
2. Replace the repository files with the contents of this ZIP, preserving any newer manifest data you have generated since exporting the uploaded project.
3. Keep GitHub Secrets and Variables in repository Settings; they are not included in this ZIP. Required/optional values are referenced by the workflows, including `GOOGLE_SERVICE_ACCOUNT_JSON`, `GOOGLE_SHEET_ID`, `SERPER_API_KEY`, and optional feed/API keys.
4. Confirm the manual company sheet name in the `MANUAL_COMPANY_SHEET_NAME` repository variable if you use it.
5. Commit and run **Actions → Remote4.me India Support Job Engine — Stable Daily → Run workflow**.
6. Inspect the `prepare`, ATS shards, `careers`, `feeds`, and `publish` jobs. Confirm the output tabs and row classifications before relying on the schedule.

## Important limitations

- This environment cannot run your GitHub Actions against your live Google Sheet or external ATS services. Syntax and local regression checks are not a live end-to-end guarantee.
- The crawler's core source is large and has legacy functions. This release deliberately avoids a risky full rewrite without live fixture coverage. It does not guarantee a specific number of jobs.
- “Remote” is not proof of India eligibility. Unknown or contradictory location evidence should be manually checked before posting.
- Do not add API keys to source files. Store them in GitHub Secrets.
- If an ATS reports zero jobs, use its per-board status/error summary to distinguish a genuinely empty board from request, parsing, or eligibility-filter failures.

## Run local checks

```bash
python -m py_compile crawler.py
python tests/test_release.py
```


## Search and source policy (Stable Release)

- LinkedIn is excluded from search queries and search-result promotion. X/Twitter remains disabled.
- The daily discovery stage uses a small rotating set of ATS-focused queries. If `SERPER_API_KEY` is configured, Google organic results can be queried through Serper; a bounded Bing RSS fallback works without an API key. The crawler does not scrape Google HTML.
- Google Jobs is a separate SerpApi feature, capped at one query per UTC day by default. Set `GOOGLE_JOBS_ENABLED=0` to disable it completely.
- Public feeds include Himalayas, Remotive, RemoteOK, Jobvetta (up to four requests/day by default), Arbeitnow, Working Nomads, We Work Remotely RSS, Jobicy, Startup Jobs, Job Opportunities API, Remote Landers, and RemoteJobs.org. Some feeds are global; the crawler keeps only India-eligible remote/hybrid candidates.
- Aggregator results are discovery candidates until the official employer/ATS application URL is verified. Do not publish an aggregator URL as a direct employer apply link without review.
- Jobvetta's free allowance is 50 requests per key per UTC day; this crawler defaults to four. Job Opportunities API has its own budget; check its API dashboard/terms before raising limits.
- Google does not offer a new, general-purpose, free unauthenticated Google Search API. Existing Custom Search JSON API access is being retired; use the bounded Serper integration if available, otherwise the no-key Bing RSS fallback.

## After deployment

Check the `prepare` job logs for `SEARCH DISCOVERY: providers=...` and `fallback_queries=...`. Search-provider configuration is only required in `prepare`; ATS shard logs intentionally do not need those keys. Check `feeds` for the Startup Jobs, Job Opportunities API, and Jobvetta status lines, because those API secrets are only passed to the feed worker.


## Merged October 22 + current release

This package keeps the current V15.1-era crawler and its current repository data, restores broader but bounded discovery settings, and removes the RemoteOK feed integration. LinkedIn job URLs are not used as job results. The package does not overwrite GitHub Secrets.

### Discovery budgets
- Public SERP queries/run: 12
- Google discovery queries/provider: 6
- Job-board queries/run: 4
- Discovery candidates: 6,000
- ATS board budget: 1,800
- ATS validation per ATS: 900
- Career crawl budget: 750; daily career budget: 1,000
- Career browser calls: 20

These are bounded limits, not a promise of a specific job count. Search-provider billing/quotas depend on configured credentials and provider plans. The `--careers` stage does not report jobs found by separate ATS shards.

### Installation
Back up the current repository first. Replace `crawler.py`, `.github/workflows/daily.yml`, `.github/workflows/discover.yml`, and `README.md` from this package. Keep your existing `data/` files and GitHub Secrets if they are newer than the included snapshot. Run the daily workflow manually and inspect each ATS shard's summary.
