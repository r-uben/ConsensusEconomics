from consensus_economics.utils.date_format import DateFormatUtils
from consensus_economics.utils.path_format import PathFormatUtils
from consensus_economics.worksheets.forex_worksheet import ForexWorksheet
from consensus_economics.paths import Paths
from consensus_economics.utils.parallel_processing import ParallelProcessor

from tqdm import tqdm
import pandas as pd
import os
import traceback

def process_forex(args):
    date, error_log_path = args
    try:
        print(f"\nProcessing date: {date}")  # Debug print
        forex_data = ForexWorksheet(date)
        result = forex_data.forecasters_data
        return result
    except Exception as e:
        error_msg = f"Error processing forex data for {date}:\n{str(e)}\n{traceback.format_exc()}"
        print(error_msg)  # Print error immediately
        with open(error_log_path, "a") as error_log:
            error_log.write(error_msg + "\n")
        return pd.DataFrame()

def get_forex_data_for_date(date):
    error_log_path = Paths().processed / "error_log.txt"
    args = (date, error_log_path)
    return process_forex(args)

def main():
    RELOAD = input("Reload existing files? (y/n): ").lower() == 'y'
    processed_path = Paths().processed 
    error_log_path = processed_path / "error_log.txt"
    
    # Create error log directory if it doesn't exist
    os.makedirs(os.path.dirname(error_log_path), exist_ok=True)
    
    # Clear the error log file at the start
    with open(error_log_path, "w") as error_log:
        error_log.write("")

    years = range(1990, 2025)
    months = range(1, 13)
    total_tasks = len(years) * len(months)
    
    with tqdm(total=total_tasks, desc="Processing Forex Data") as pbar:
        for year in years:
            for month in months:
                try:
                    date = DateFormatUtils.get_date(year, month)
                    folder = PathFormatUtils.get_file_path(processed_path, f"{year}/forex")

                    os.makedirs(folder, exist_ok=True)
                    filename = f"{folder}/{date}.csv"
                    
                    if not os.path.exists(filename) or RELOAD:
                        print(f"\nProcessing {filename}")  # Debug print
                        df = get_forex_data_for_date(date)
                        if not df.empty:
                            df.drop_duplicates().to_csv(filename, index=False)
                        else:
                            print(f"Empty DataFrame for {date}")
                    
                    pbar.update(1)
                except Exception as e:
                    error_msg = f"Error in main loop for {year}-{month}:\n{str(e)}\n{traceback.format_exc()}"
                    print(error_msg)
                    with open(error_log_path, "a") as error_log:
                        error_log.write(error_msg + "\n")
                    pbar.update(1)

if __name__ == "__main__":
    main()