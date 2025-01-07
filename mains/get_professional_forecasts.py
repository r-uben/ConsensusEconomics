from consensus_economics.utils.countries import CountriesUtils
from consensus_economics.utils.date_format import DateFormatUtils
from consensus_economics.utils.path_format import PathFormatUtils



from consensus_economics.worksheet import Worksheet

from consensus_economics.paths import Paths

from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed

import pandas as pd
import os


def get_date(year, month):
    return f"{year}{month:02d}"

def process_country(args):
    date, country, error_log_path = args
    try:
        data_consensus = Worksheet(date, country)
        return data_consensus.forecasters_data
    except (ValueError, KeyError):
        with open(error_log_path, "a") as error_log:
            error_log.write(f"Error processing {date} for {country}\n")
        return pd.DataFrame()

def get_forecasters_data_for_all_countries(date, countries, date_pbar):
    error_log_path = Paths().processed / "error_log.txt"
    
    # Create args list for parallel processing
    args_list = [(date, country, error_log_path) for country in countries]
    
    df = pd.DataFrame()
    
    # Use ProcessPoolExecutor for parallel processing
    with ProcessPoolExecutor() as executor:
        # Submit all tasks
        future_to_country = {executor.submit(process_country, args): args[1] for args in args_list}
        
        # Process completed tasks
        for future in as_completed(future_to_country):
            country = future_to_country[future]
            try:
                result = future.result()
                if not result.empty:
                    df = pd.concat([df, result], axis=0, ignore_index=True)
            except Exception as e:
                with open(error_log_path, "a") as error_log:
                    error_log.write(f"Error processing {date} for {country}: {str(e)}\n")
            date_pbar.update(1)
            
    return df

def main():
    RELOAD = input("Reload existing files? (y/n): ").lower() == 'y'
    processed_path = Paths().processed 
    error_log_path = processed_path / "error_log.txt"
    
    # Create error log directory if it doesn't exist
    os.makedirs(os.path.dirname(error_log_path), exist_ok=True)
    
    # Clear the error log file at the start of the script
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