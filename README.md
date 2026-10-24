# Remote4.me V15.0 — ATS recovery and daily crawl rotation

This release builds on V14.9 and responds to the October 24 V14.8 run logs.

## High-impact fixes

- **Workday record normalization:** previous manifests typically store `key="tenant|wdN|site"` plus a URL, not separate `tenant`, `shard`, and `site` fields. The prior worker read those missing dictionary fields directly, causing every Workday board in the supplied status summary to fail. V15.0 reconstructs the fields from the key and normalizes the URL to the actual tenant/site landing page before calling Workday's public CXS endpoint.
- **Strict ATS slugs:** rejects archive artifacts such as `$12.1K`, `100vw`, `100%`, `...`, `.well-known`, and pure numeric tokens as board identifiers. This filters historical Common Crawl/Wayback noise when `data/boards.json` is rebuilt.
- **Daily coverage rotation:** the old `rotate_records()` used `day // V11_ROTATION_DAYS`, meaning a bounded ATS crawl could select the same slice for 7–10 days. V15.0 uses deterministic chunks that rotate daily, preferring boards with recent positive results and current directory/employer seeds over archive-only candidates.
- **Search API restraint:** one configured core SERP provider is used per run, with the keyless Bing RSS fallback as the supplement. Less reliable experimental endpoints that returned HTTP 429/TLS errors in your logs are opt-in via `SEARCH_EXPERIMENTAL_PROVIDERS_ENABLED=1`.
- **Aggregator resolution fallback:** when an aggregator page cannot be fetched, the resolver may use up to 12 keyless Bing RSS queries per run to look for an exact employer/ATS job URL. Every result still goes through destination-page verification; no snippet-only promotion.
- **ATS status diagnostics:** each ATS shard prints boards attempted, boards yielding target jobs, zero-result boards, and error counts/types.
- Keeps V14.9 fixes: persists homepage seeds, carries forward valid prior boards, crawls deeper into employer career pages, supports more nonstandard ATS routes, and keeps strict role/India/workplace/direct-apply validation.

## Install

1. Back up `crawler.py`, `.github/workflows/daily.yml`, `.github/workflows/discover.yml`, and the `data/` folder.
2. Replace those three code/workflow files with the V15.0 versions in this ZIP.
3. Keep `requirements.txt`, Google Sheets secrets, and existing repository data; the existing `data/boards.json` is cleaned during the next discovery refresh.
4. Commit and push, then run the **full daily workflow** first. Its prepare step runs discovery before the ATS, career, and feed shards. The separate weekly discovery workflow can remain on its existing schedule.
5. In the first run, check the `ATS BOARD SUMMARY Workday` line and ensure Workday's error count falls below the previous 136/136. Check that invalid archive slugs disappear from newly generated `data/boards.json`.

## Important limitations

This package has been syntax-checked and tested with local/mocked cases; it has not been run against your live GitHub Actions, spreadsheet, or all external sites. The public Bing RSS fallback may throttle or change. It is a discovery fallback, not a guaranteed Google replacement. Jobs still need employer/ATS page verification; more crawled URLs do not automatically mean more valid jobs.
