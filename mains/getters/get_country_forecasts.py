from consensus_economics.utils.countries import CountriesUtils
from consensus_economics.utils.date_format import DateFormatUtils
from consensus_economics.utils.path_format import PathFormatUtils
from consensus_economics.worksheets.country_worksheet import CountryWorksheet
from consensus_economics.paths import Paths
from consensus_economics.utils.parallel_processing import ParallelProcessor

from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed

import pandas as pd
import os

def process_country(args):
    date, country, error_log_path = args
    try:
        data_consensus = CountryWorksheet(date, country)
        return data_consensus.forecasters_data
    except (ValueError, KeyError) as e:
        return pd.DataFrame()

def get_forecasters_data_for_all_countries(date, countries, date_pbar):
    error_log_path = Paths().processed / "error_log.txt"
    processor = ParallelProcessor(error_log_path)
    
    # Create args list for parallel processing
    args_list = [(date, country, error_log_path) for country in countries]
    
    return processor.process_in_parallel(
        process_func=process_country,
        args_list=args_list,
        desc=f"Processing {date}",
        total=len(countries)
    )

def main():
    RELOAD = input("Reload existing files? (y/n): ").lower() == 'y'
    processed_path = Paths().processed 
    error_log_path = processed_path / "error_log.txt"
    
    # Create error log directory if it doesn't exist
    os.makedirs(os.path.dirname(error_log_path), exist_ok=True)
    
    # Clear the error log file at the start
    with open(error_log_path, "w") as error_log:
        error_log.write("")

    years = range(2024, 2025)
    months = range(9, 13)
    countries = CountriesUtils().countries
    total_tasks = len(years) * len(months) * len(countries)
    
    with tqdm(total=total_tasks, desc="Overall Progress") as overall_pbar:
        for year in years:
            for month in months:
                date = DateFormatUtils.get_date(year, month)
                folder = PathFormatUtils.get_file_path(processed_path, f"{year}/forecasters")

                os.makedirs(folder, exist_ok=True)
                filename = f"{folder}/{date}.csv"
                
                with tqdm(total=len(countries), desc=f"Processing {date}", leave=False) as date_pbar:
                    if not os.path.exists(filename) or RELOAD:
                        df = get_forecasters_data_for_all_countries(date, countries, date_pbar)
                        if not df.empty:
                            df.drop_duplicates().to_csv(filename, index=False)
                    else:
                        date_pbar.update(len(countries))
                    
                overall_pbar.update(len(countries))

if __name__ == "__main__":
    main()