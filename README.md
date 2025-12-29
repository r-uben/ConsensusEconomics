# Consensus Economics Data Pipeline

Python package for processing economic forecast data from Consensus Economics Excel files.

## Features

- Parse country-specific economic forecasts (GDP, inflation, etc.)
- Handle forex forecast data
- Automated data transformation pipeline
- Configurable country and currency lists

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/consensus-economics.git
cd consensus-economics

# Create virtual environment and install
uv venv
uv pip install -e ".[dev]"
```

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
```

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

## License

Copyright © 2024 Rubén Fernández Fuertes. All Rights Reserved.
