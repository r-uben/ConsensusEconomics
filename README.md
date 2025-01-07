# Consensus Economics Data Pipeline

A Python package for processing and analyzing economic forecast data from Consensus Economics. This tool streamlines the extraction, transformation, and storage of both country-specific and forex forecasts.

## Features

- 📊 Process country-specific economic forecasts
- 💱 Handle forex forecast data
- 🔄 Automated data transformation pipeline
- ☁️ AWS S3 integration for data storage
- 📈 Excel file processing and cleaning

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/ConsensusEconomics.git
cd ConsensusEconomics

# Install dependencies using Poetry
poetry install
```

## Usage

The package provides several command-line scripts for data processing:

```bash
# Process country forecasts
poetry run get_country_forecasts

# Process forex forecasts
poetry run get_forex_forecasts

# Clean Excel files
poetry run clean_xlsx_folder

# Upload data to S3
poetry run save_to_bucket
```

## Project Structure

```
ConsensusEconomics/
├── src/consensus_economics/    # Main package
│   ├── worksheets/            # Forecast data processors
│   ├── aws/                   # AWS integration
│   └── utils/                 # Utility functions
├── mains/                     # CLI scripts
└── data/                      # Data directory
```

## Dependencies

- Python 3.13+
- pandas
- boto3
- duckdb
- openpyxl

## License

Copyright © 2024 Rubén Fernández Fuertes. All Rights Reserved.

This project and its contents are proprietary and confidential. No part of this project may be reproduced, distributed, or modified without explicit written permission from the author. Any contributions or modifications must be requested and approved by the author.

## Author

Rubén Fernández-Fuertes