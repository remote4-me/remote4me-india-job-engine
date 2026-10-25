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
