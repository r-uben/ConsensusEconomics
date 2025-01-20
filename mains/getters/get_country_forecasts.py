from consensus_economics.utils.countries import CountriesUtils
from consensus_economics.utils.date_format import DateFormatUtils
from consensus_economics.utils.path_format import PathFormatUtils
from consensus_economics.worksheets.country_worksheet import CountryWorksheet
from consensus_economics.paths import Paths

from tqdm import tqdm
import pandas as pd
import os
from itertools import product

def process_country(date: str, country: str) -> tuple[str, pd.DataFrame]:
    """Process a single country's data for a given date."""
    try:
        data_consensus = CountryWorksheet(date, country)
        return country, data_consensus.forecasters_data
    except Exception as e:
        
        return country, pd.DataFrame()

def process_date(date: str, countries: list, reload: bool = False) -> None:
    """Process all countries for a given date."""
    try:
        year = date[:4]
        folder = PathFormatUtils.get_file_path(Paths().processed, f"{year}/forecasters")
        filename = f"{folder}/{date}.csv"
        
        # Skip if file exists and not reloading
        if os.path.exists(filename) and not reload:
            tqdm.write(f"File {filename} already exists, skipping...")
            return

        # Process countries sequentially with progress bar
        all_data = []
        with tqdm(total=len(countries), 
                 desc=f"Processing {date}", 
                 ncols=100,
                 bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]') as pbar:
            
            for country in countries:
                try:
                    country, df = process_country(date, country)
                    if not df.empty:
                        all_data.append(df)
                        status = "✓"
                    else:
                        status = "✗"
                    pbar.set_description(f"Processing {date} - {country} {status}")
                    pbar.update(1)
                except Exception as e:
                    tqdm.write(f"Error processing {country}: {str(e)}")
                    pbar.update(1)
        
        if all_data:
            # Combine results efficiently
            final_df = pd.concat(all_data, ignore_index=True, copy=False)
            
            # Create directory and save
            os.makedirs(folder, exist_ok=True)
            final_df.dropna().to_csv(filename, index=False)
        else:
            tqdm.write(f"No data to save for {date}")
            
    except Exception as e:
        tqdm.write(f"Error processing date {date}: {str(e)}")
        raise

def main():
    RELOAD = input("Reload existing files? (y/n): ").lower() == 'y'
    
    # Generate all year-month combinations efficiently
    dates = [
        DateFormatUtils.get_date(year, month) 
        for year, month in product(range(1990, 2025), range(1, 13))
    ]
    
    countries = CountriesUtils().countries
    print(f"Processing {len(countries)} countries for {len(dates)} dates")
    
    # Process dates with progress bar
    for date in tqdm(dates, 
                    desc="Overall Progress", 
                    ncols=100,
                    bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]'):
        process_date(date, countries, RELOAD)

if __name__ == "__main__":
    main()