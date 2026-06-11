"""Extract country forecast data from Consensus Economics Excel files."""

import argparse
import os

import pandas as pd
from tqdm import tqdm

from consensus_economics.config import COUNTRIES, END_YEAR, START_YEAR
from consensus_economics.paths import Paths
from consensus_economics.utils.date_format import DateFormatUtils
from consensus_economics.worksheets.base_worksheet import clear_workbook_cache
from consensus_economics.worksheets.country_worksheet import CountryWorksheet


def process_country(date: str, country: str) -> tuple[str, pd.DataFrame]:
    """Process a single country's data for a given date."""
    try:
        data_consensus = CountryWorksheet(date, country)
    except KeyError:
        # Sheet absent in this vintage (coverage varies by year) — not an error
        return country, pd.DataFrame()

    try:
        df = data_consensus.forecasters_data
        if data_consensus.skipped_cells:
            tqdm.write(
                f"{date} {country}: skipped {data_consensus.skipped_cells} "
                "non-numeric cells"
            )
        if df.empty:
            # Sheet exists but yielded nothing — layout the parser can't read
            tqdm.write(f"WARNING {date} {country}: sheet present but parsed to 0 rows")
        return country, df
    except Exception as e:
        tqdm.write(f"Error processing {country}: {str(e)}")
        return country, pd.DataFrame()


def process_date(date: str, countries: list[str], reload: bool = False) -> None:
    """Process all countries for a given date."""
    try:
        year = date[:4]
        paths = Paths()
        folder = paths.output / year / "forecasters"
        filename = folder / f"{date}.csv"

        if not (paths.xlsx / f"{date}.xlsx").exists():
            tqdm.write(f"No xlsx file for {date}, skipping...")
            return

        if filename.exists() and not reload:
            tqdm.write(f"File {filename} already exists, skipping...")
            return

        all_data = []
        for country in countries:
            try:
                country, df = process_country(date, country)
                if not df.empty:
                    all_data.append(df)
            except Exception as e:
                tqdm.write(f"Error processing {country}: {str(e)}")

        # Clear cache for this date to free memory
        clear_workbook_cache(date)

        if all_data:
            final_df = pd.concat(all_data, ignore_index=True, copy=False)
            os.makedirs(folder, exist_ok=True)
            # Only a missing value invalidates a row; missing metadata (e.g.
            # unit) must not silently drop observations
            cleaned_df = final_df.dropna(subset=["value"])
            dropped = len(final_df) - len(cleaned_df)
            if dropped:
                tqdm.write(f"{date}: dropped {dropped} rows with missing value")
            cleaned_df.to_csv(filename, index=False)
            tqdm.write(f"Saved {len(all_data)} countries to {filename}")
        else:
            tqdm.write(f"No data to save for {date}")

    except Exception as e:
        tqdm.write(f"Error processing date {date}: {str(e)}")
        raise


def main() -> None:
    """Main entry point for country forecasts extraction."""
    parser = argparse.ArgumentParser(
        description="Process Consensus Economics country data"
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
        print(f"Processing country data for year {args.year}")
        years = [args.year]
    else:
        print("Processing country data for all years")
        years = range(START_YEAR, END_YEAR)

    print(f"Reload mode: {'ON' if args.reload else 'OFF'}")

    countries = list(COUNTRIES)
    print(f"Processing {len(countries)} countries")

    for year in years:
        dates = [DateFormatUtils.get_date(year, month) for month in range(1, 13)]

        for date in tqdm(
            dates,
            desc=f"Processing {year}",
            ncols=100,
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
        ):
            process_date(date, countries, args.reload)


if __name__ == "__main__":
    main()
