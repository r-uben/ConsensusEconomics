# Consensus Economics Data Pipeline

Python package for processing economic forecast data from Consensus Economics Excel files.

## Data status (as of June 2026)

Corpus covers **Jan 1990 – Jun 2026**. Known gaps and open items:

- **Feb 2010 (`201002`) — missing.** The source workbook is corrupt (sheet
  names misaligned, all cached values stripped); the S3 backup is the same
  broken export. Fix: re-download the Feb 2010 *Consensus Forecasts (G7 &
  Western Europe)* issue, save as `data/xlsx/201002.xlsx`, re-run
  `get-country-forecasts --year 2010 --reload` (and forex), then
  `consolidate-output --concepts`.
- **Jan 2026 (`202601`) — missing.** Never downloaded. Fix: download
  `CFJan2026.xlsx`, same routine for year 2026.
- **30 `needs_review` rows** in
  `src/consensus_economics/mappings/variable_map.csv` — open concept-identity
  judgments (bare "Investment" labels, "Indust / Manuf Production" scope,
  Wholesale→Producer Prices continuity, US Employment Costs vs wages).
  Edit status/notes in the CSV, then rebuild with
  `consolidate-output --concepts`.
- The S3 bucket's per-month processed CSVs predate the 2026 schema
  regeneration (old schema, Jan 2025 vintage); refresh with `save-to-bucket`
  (`AWS_PROFILE=personal`) or sync `data/output/` wholesale.
- **PDF archive is partial.** `s3://consensus-economics/pdf/` holds only
  Apr 2025 onward (9 issues, named `YYYYMM.pdf`); the pipeline itself is
  xlsx-only, so this affects nothing downstream. Whether the 1990–2024 PDF
  back-catalog exists inside the original zips on `/Volumes/Main` is
  unverified — check next time the volume is mounted.

## Features

- Parse country-specific economic forecasts (GDP, inflation, etc.)
- Handle forex forecast data
- Automated data transformation pipeline
- Configurable country and currency lists

## Installation

```bash
cd consensus-economics

# Venv lives outside the repo (iCloud corrupts in-repo venvs)
uv venv ~/venvs/consensus-economics --python 3.12
export UV_PROJECT_ENVIRONMENT=~/venvs/consensus-economics
uv sync --extra dev --extra aws
```

Set `UV_PROJECT_ENVIRONMENT` before every `uv` command in this repo (or wire it
up with direnv).

## Storage Layout

```
External storage (raw downloads):
/Volumes/Main/Library/Databases/consensus_economics/
├── zip/          # Original zip downloads from Consensus Economics
└── xlsx/         # Extracted raw Excel files

Local (working files):
<repo>/data/
├── xlsx/         # Renamed YYYYMM.xlsx files (working copies)
└── output/       # Final processed CSVs
    ├── 2024/
    │   ├── forecasters/202401.csv, 202402.csv, ...
    │   └── forex/202401.csv, 202402.csv, ...
    └── ...
```

## Usage

### CLI Commands

```bash
# Decompress raw zip files (requires external volume mounted)
uv run decompress-files

# Clean up duplicate xlsx files
uv run clean-xlsx-folder

# Extract country forecasts to CSV
uv run get-country-forecasts --year 2024

# Extract forex forecasts to CSV
uv run get-forex-forecasts --year 2024

# Consolidate all CSVs into data/output/{forecasters,forex}.parquet
uv run consolidate-output

# Upload processed CSVs to S3 (requires the aws extra)
uv run save-to-bucket --year 2024
```

Output format: see [SCHEMA.md](SCHEMA.md) for the full data dictionary.

### Python API

```python
from consensus_economics import CountryWorksheet, ForexWorksheet, Paths

# Check paths
paths = Paths()
print(f"External storage: {paths.external}")
print(f"Available: {paths.external_available}")
print(f"Local xlsx: {paths.xlsx}")
print(f"Output: {paths.output}")

# Parse country forecast data
worksheet = CountryWorksheet(date="202409", country="Canada")
df = worksheet.forecasters_data

# Parse forex forecast data
forex = ForexWorksheet(date="202409")
forex_df = forex.forecasters_data
```

## Project Structure

```
consensus-economics/
├── src/consensus_economics/   # Core library
│   ├── config.py              # Configuration (countries, currencies, paths)
│   ├── constructor.py         # File processing
│   ├── paths.py               # Path management
│   ├── worksheets/            # Excel parsers
│   │   ├── base_worksheet.py
│   │   ├── country_worksheet.py
│   │   └── forex_worksheet.py
│   └── utils/                 # Utilities
├── mains/                     # CLI entry points
│   ├── getters/               # Data extraction
│   └── preprocessing/         # File cleanup
├── tests/                     # Test suite
└── data/                      # Local working data
    ├── xlsx/                  # Renamed xlsx files
    └── output/                # Final CSVs
```

## Configuration

Edit `src/consensus_economics/config.py` to customize:

- `EXTERNAL_STORAGE` - Path to external volume for raw files
- `COUNTRIES` - List of countries to process
- `CURRENCY_CODES` - Currency name to ISO code mappings
- `START_YEAR`, `END_YEAR` - Data collection range

## Testing

```bash
uv run pytest tests/ -v
```

## Dependencies

- Python 3.12+
- pandas
- openpyxl
- tqdm
- boto3 (optional, `aws` extra — only for S3 upload)

## License

Copyright © 2024 Rubén Fernández Fuertes. All Rights Reserved.
