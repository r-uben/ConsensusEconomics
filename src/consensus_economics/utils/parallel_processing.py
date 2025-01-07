from typing import List, Callable, Any, Tuple
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
import pandas as pd
import logging
from pathlib import Path

class ParallelProcessor:
    """
    Utility class for handling parallel processing tasks with error logging.
    """

    def __init__(self, error_log_path: Path):
        """
        Initialize the parallel processor.

        Args:
            error_log_path: Path to the error log file
        """
        self.error_log_path = error_log_path
        self._setup_error_logging()

    def _setup_error_logging(self) -> None:
        """Setup error logging configuration."""
        self.error_log_path.parent.mkdir(parents=True, exist_ok=True)
        if self.error_log_path.exists():
            self.error_log_path.unlink()  # Clear existing log
        self.error_log_path.touch()

    def _log_error(self, message: str) -> None:
        """
        Log an error message to the error log file.

        Args:
            message: Error message to log
        """
        with open(self.error_log_path, "a") as error_log:
            error_log.write(f"{message}\n")

    def process_in_parallel(
        self,
        process_func: Callable,
        args_list: List[Tuple],
        desc: str,
        total: int
    ) -> pd.DataFrame:
        """
        Process tasks in parallel using a ProcessPoolExecutor.

        Args:
            process_func: Function to process each task
            args_list: List of argument tuples for each task
            desc: Description for the progress bar
            total: Total number of tasks for the progress bar

        Returns:
            DataFrame containing combined results
        """
        df = pd.DataFrame()

        with ProcessPoolExecutor() as executor:
            # Submit all tasks
            future_to_args = {
                executor.submit(process_func, args): args 
                for args in args_list
            }
            
            # Process completed tasks with progress bar
            with tqdm(total=total, desc=desc, leave=False) as pbar:
                for future in as_completed(future_to_args):
                    args = future_to_args[future]
                    try:
                        result = future.result()
                        if isinstance(result, pd.DataFrame) and not result.empty:
                            df = pd.concat([df, result], axis=0, ignore_index=True)
                    except Exception as e:
                        self._log_error(f"Error processing {args}: {str(e)}")
                    pbar.update(1)

        return df 