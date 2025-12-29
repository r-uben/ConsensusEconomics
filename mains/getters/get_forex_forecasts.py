"""Extract forex forecast data from Consensus Economics Excel files."""

import argparse
import os
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from consensus_economics.config import END_YEAR, START_YEAR
from consensus_economics.paths import Paths
from consensus_economics.utils.date_format import DateFormatUtils
from consensus_economics.worksheets.forex_worksheet import ForexWorksheet


def process_forex(date: str, reload: bool = False) -> pd.DataFrame:
    """Process forex data for a given date."""
    try:
        folder = Paths().output / date[:4] / "forex"
        filename = folder / f"{date}.csv"

        if filename.exists() and not reload:
            tqdm.write(f"File {filename} already exists, skipping...")
            return pd.DataFrame()

        forex_data = ForexWorksheet(date)
        result = forex_data.forecasters_data

        if not result.empty:
            os.makedirs(folder, exist_ok=True)
            result.drop_duplicates().to_csv(filename, index=False)
            tqdm.write(f"Saved forex data for {date}")

        return result
    except Exception as e:
        tqdm.write(f"Error processing forex data for {date}: {str(e)}")
        return pd.DataFrame()


def process_year(year: int, reload: bool = False) -> None:
    """Process all months for a given year."""
    paths = Paths()
    available_dates = []

    for month in range(1, 13):
        date = DateFormatUtils.get_date(year, month)
        if (paths.xlsx / f"{date}.xlsx").exists():
            available_dates.append(date)

    if not available_dates:
        print(f"No xlsx files found for year {year}")
        return

    print(f"Processing forex data for year {year} ({len(available_dates)} files found)")

    for date in tqdm(
        available_dates,
        desc=f"Processing {year}",
        ncols=100,
        bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
    ):
        process_forex(date, reload)


def main() -> None:
    """Main entry point for forex forecasts extraction."""
    parser = argparse.ArgumentParser(
        description="Process Consensus Economics forex data"
    )
    parser.add_argument(
        "--year",
        type=int,
        help="Year to process (e.g., 2024). If not provided, processes all years",
    )
    parser.add_argument(
        "--reload",
        action="store_true",
        help="Reload existing files",
    )
    args = parser.parse_args()

    if args.year:
        print(f"Processing forex data for year {args.year}")
        years = [args.year]
    else:
        print("Processing forex data for all years")
        years = range(START_YEAR, END_YEAR)

    print(f"Reload mode: {'ON' if args.reload else 'OFF'}")

    for year in years:
        process_year(year, args.reload)


if __name__ == "__main__":
    main()
