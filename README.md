# Remote4.me V14 India crawler upgrade

## What changed
- Removed Jobvetta completely (no code, config, workflow secret, or feed call).
- India-first remote/hybrid support, customer success, product support and related role scope retained.
- Added a conservative direct-apply quality gate: known aggregators (including LinkedIn and Indeed) are discovery-only and cannot enter the direct jobs output.
- Feed records are direct only when they resolve to an exact supported ATS job route or an exact job route on the employer domain recorded for that company. A random non-aggregator hostname is no longer enough.
- Existing Google Sheets/manual review and sharded GitHub Actions flow are preserved.

## Install
1. Back up the current `crawler.py` and `.github/workflows/daily.yml`.
2. Replace repository `crawler.py` with `remote4me_v14_crawler.py` (rename to `crawler.py`).
3. Replace `.github/workflows/daily.yml` with `remote4me_v14_daily.yml` (rename to `daily.yml`).
4. Commit and run the workflow manually. Review `jobs-careers`, `jobs-feeds`, ATS artifacts, and the discovery sheet before publishing.

## Important limitations
- LinkedIn and Indeed are not directly scraped or bypassed. Their links are not treated as employer-direct application URLs. Use permitted discovery signals, then resolve to the employer's public career page or authorized ATS page.
- Himalayas remains a discovery source. A Himalayas page itself is not an employer application link; a separately verified employer/ATS link can qualify.
- This is not a promise of more jobs every run. The stricter direct-link gate may initially reduce the number of rows while improving accuracy.
- The live GitHub run still needs your existing repository secrets, dependencies, and manifests. This package was validated locally for Python syntax and workflow YAML only; no live external crawl was run.
