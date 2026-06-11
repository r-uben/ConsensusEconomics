# consensus-economics

Data pipeline: Consensus Economics xlsx surveys → tidy CSVs in `data/output/`.

## Environment (IMPORTANT — iCloud repo)

The venv lives OUTSIDE the repo (iCloud corrupts in-repo venvs). Every `uv` command
needs:

```bash
export UV_PROJECT_ENVIRONMENT=~/venvs/consensus-economics
```

If imports break (`ModuleNotFoundError: mains`), re-run
`uv sync --extra dev --extra aws` with that env var set — never recreate `.venv`
inside the repo.

## Commands

```bash
uv run get-country-forecasts [--year YYYY] [--reload]   # xlsx → forecasters CSVs
uv run get-forex-forecasts [--year YYYY] [--reload]     # xlsx → forex CSVs
uv run consolidate-output     # all CSVs → data/output/{forecasters,forex}.parquet
uv run decompress-files       # needs /Volumes/Main mounted
uv run clean-xlsx-folder
uv run save-to-bucket --year YYYY   # S3 upload (needs --extra aws)
uv run pytest tests/ -q
uvx ruff check src/ mains/ tests/   # keep at zero errors
```

## Gotchas

- `data/` and `sensitive_info/` are gitignored — licensed Consensus Economics data,
  never commit or publish it. S3 bucket: `consensus-economics`.
- Output schema (forecasters): `country,variable,source,statistic,year,value,unit,release_date`.
  Forex: `currency,reference,year,horizon,current_value,forecasted_value,release_date`.
  All 1990–2025 files must share one schema — if you change it, regenerate everything
  with `--reload`.
- `release_date` is YYYYMMDD; empty string means the survey date could not be parsed.
- Early vintages (1990s) cover ~12 of the 25 configured countries; a missing sheet is
  normal, not an error.
- "Euro Zone" sheets exist as header-only template stubs back to the early 1990s
  (anachronistic "Euros bn" units) — they parse to 0 rows with a WARNING until real
  coverage starts in Dec 2002. Expected noise, not a bug.
- `data/xlsx/201002.xlsx` (Feb 2010) is broken at the source: sheet names are
  misaligned with content (the "USA" sheet holds Japan's headers) and all cached
  values are stripped. Re-extract from the original zip on /Volumes/Main or
  re-download; no parser fix is possible. Its output CSV is near-empty until then.
- Raw zip/xlsx live on the external volume (`/Volumes/Main/Library/Databases/consensus_economics`);
  `data/xlsx/` holds renamed `YYYYMM.xlsx` working copies.
