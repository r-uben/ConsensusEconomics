from consensus_economics.utils.date_format import DateFormatUtils
from consensus_economics.utils.path_format import PathFormatUtils
from consensus_economics.worksheets.forex_worksheet import ForexWorksheet
from consensus_economics.paths import Paths
from consensus_economics.utils.parallel_processing import ParallelProcessor

from tqdm import tqdm
import pandas as pd
import os
import traceback
from itertools import product

def process_forex(args):
    date, error_log_path, reload = args
    try:
        folder = PathFormatUtils.get_file_path(Paths().processed, f"{date[:4]}/forex")
        filename = f"{folder}/{date}.csv"
        
        # Skip if file exists and not reloading
        if os.path.exists(filename) and not reload:
            return pd.DataFrame()
            
        forex_data = ForexWorksheet(date)
        result = forex_data.forecasters_data
        
        if not result.empty:
            os.makedirs(folder, exist_ok=True)
            result.drop_duplicates().to_csv(filename, index=False)
            
        return result
    except Exception as e:
        error_msg = f"Error processing forex data for {date}:\n{str(e)}\n{traceback.format_exc()}"
        print(error_msg)  # Print error immediately
        with open(error_log_path, "a") as error_log:
            error_log.write(error_msg + "\n")
        return pd.DataFrame()

def main():
    RELOAD = input("Reload existing files? (y/n): ").lower() == 'y'
    
    processed_path = Paths().processed 
    error_log_path = processed_path / "error_log.txt"
    
    # Create error log directory if it doesn't exist
    os.makedirs(os.path.dirname(error_log_path), exist_ok=True)
    
    # Clear the error log file at the start
    with open(error_log_path, "w") as error_log:
        error_log.write("")

    # Generate all year-month combinations
    years = range(1990, 2025)
    months = range(1, 13)
    dates = [
        DateFormatUtils.get_date(year, month) 
        for year, month in product(years, months)
    ]
    
    # Create args list for parallel processing, including RELOAD
    args_list = [(date, error_log_path, RELOAD) for date in dates]
    
    # Initialize parallel processor
    processor = ParallelProcessor(error_log_path)
    
    # Process in parallel
    results = processor.process_in_parallel(
        process_func=process_forex,
        args_list=args_list,
        desc="Processing Forex Data",
        total=len(args_list)
    )

if __name__ == "__main__":
    main()