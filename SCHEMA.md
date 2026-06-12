# Output data dictionary

All processed output lives in `data/output/` (gitignored — licensed Consensus
Economics data; mirrored to the `consensus-economics` S3 bucket).

```
data/output/
├── <YYYY>/forecasters/<YYYYMM>.csv   # one file per survey month
├── <YYYY>/forex/<YYYYMM>.csv
├── forecasters.parquet               # full consolidated panel (consolidate-output)
└── forex.parquet
```

Every CSV from 1990 to today shares one schema per kind. If a schema change is
ever needed, regenerate the whole corpus (`get-country-forecasts --reload`,
`get-forex-forecasts --reload`) so this stays true.

## Forecasters (country surveys)

One row = one statistic for one variable, target year, and source, from one
monthly survey. Long/tidy format.

| Column | Type | Description |
|---|---|---|
| `country` | str | Survey panel, as named in the workbook sheet (e.g. `USA`, `Euro Zone`). Coverage grows over time: early-1990s vintages have ~12 countries, modern ones 25. |
| `variable` | str | Forecast variable, raw from the worksheet header (e.g. `Gross Domestic Product`, `10 Year Govt Bond Yield`). **Not canonicalized** — names and definitions drift across decades; do not assume one label covers the same concept in 1991 and 2024 without checking `unit`. |
| `source` | str | `Consensus` for panel aggregates, otherwise the forecaster name (e.g. `Goldman Sachs`). |
| `statistic` | str | For `Consensus` rows: `mean`, `std_dev`, `high`, `low`, `count`. For individual forecasters: `forecast`. Monetary-policy probability rows use `Increase` / `No Change` / `Decrease` as `source` with `forecast` semantics. |
| `year` | int | **Target year** being forecast (current survey year or the next one). |
| `value` | float | The forecast/statistic value, rounded to 6 decimals. Never NaN. |
| `unit` | str | Raw unit string from the worksheet (e.g. `%`, `real, % change`, `US$ bn`). May be empty for some variables. |
| `release_date` | str | Survey date as `YYYYMMDD`. Empty when the workbook's date cell could not be parsed — fall back to the file's `YYYYMM`. |

## Forex

One row = one currency/horizon forecast against a reference currency.

| Column | Type | Description |
|---|---|---|
| `currency` | str | ISO code of the forecast currency (e.g. `CAD`, `JPY`). |
| `reference` | str | Reference currency: `USD` or `EUR`. Values are units of `currency` per 1 unit of `reference`. |
| `year` | int | Survey year (forecasts are horizon-based, not calendar-year). |
| `horizon` | int | Forecast horizon in months: `3`, `12`, or `24`. |
| `current_value` | float | Spot rate at survey time. |
| `forecasted_value` | float | Consensus forecast at the horizon. |
| `release_date` | str | As in forecasters. |

## Consolidated Parquet

`consolidate-output` adds one column the per-month CSVs don't carry:

| Column | Type | Description |
|---|---|---|
| `survey_date` | date | First of the survey month, derived from the filename. Always present, unlike `release_date` (which becomes a proper date, NaT when unparseable). |

String columns with repeated values are stored as categoricals.

## Concept layer (variable canonicalization)

The raw Parquet is vintage-faithful: `variable` is whatever the workbook said
that month. Cross-time concept identity lives in a separate, versioned
judgment layer (FRED/ALFRED-style separation of series identity from labels):

- `src/consensus_economics/mappings/variable_map.csv` — committed to git.
  Keyed `(country, raw_variable, valid_from..valid_to)` → `concept_id`
  (stable codes: `GDP`, `CPI`, `HICP`, `PPI`, `IP`, `RATE_3M`, ...), with
  `mapping_status` (`confirmed` / `needs_review` / `new`), `break_type`
  (e.g. `GERMAN_REUNIFICATION`), and an `evidence_note` per judgment.
  Regenerate the skeleton after new data with `build-variable-map`
  (existing judgments are preserved; only new pairs are appended).
- `data/output/forecasters_concepts.parquet` — convenience layer from
  `consolidate-output --concepts`: the raw panel pre-joined with
  `concept_id`, `concept_label`, `mapping_status`. Use this for
  cross-country panels; use the raw Parquet for vintage-faithful work.

Rows with `mapping_status == "needs_review"` are open research judgments
(UK CPI/RPI identity, Wholesale→Producer Prices renames, bare "Investment"
labels) — filter or resolve them before relying on those concepts.

## Known caveats

- `Number of Forecasts` rows carry the panel count as `value` (a float).
- Forecaster names also drift across decades (renames, mergers) — there is
  no canonicalization layer for `source` yet.
