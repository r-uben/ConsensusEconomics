from typing import List, Optional, Dict, Any
from pandas import DataFrame
import pandas as pd
from consensus_economics.utils.date_format import DateFormatUtils
from consensus_economics.worksheets.base_worksheet import BaseWorksheet
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import chain


class CountryWorksheet(BaseWorksheet):
    """
    Handles the processing of country-specific worksheet data.
    
    Args:
        date (str): Date in format 'yyyymm'
        country (str): Country name matching worksheet
    
    Example:
        >>> worksheet = CountryWorksheet(date='202409', country='Canada')
        >>> df = worksheet.forecasters_data
    """

    def __init__(self, date: str, country: str) -> None:
        super().__init__(date, sheet_name=country)
        # Initialize all properties at once to avoid multiple worksheet accesses
        self._initialize_properties()

    def _initialize_properties(self) -> None:
        """Initialize all properties at once to avoid multiple worksheet accesses."""
        # Get the worksheet data once and store it in a temporary variable
        worksheet_data = self.worksheet.copy()
        
        # Process column names
        column_headers = worksheet_data.iloc[1:4]
        self._column_names = column_headers.apply(
            lambda x: ' '.join(x.dropna().astype(str).str.strip()), 
            axis=0
        ).tolist()
        
        # Set column names on the copy
        worksheet_data.columns = self._column_names
        
        # Extract variables
        self._variables = worksheet_data.iloc[2:6].reset_index(drop=True)
        
        # Get release date
        try:
            date_str = str(worksheet_data.iloc[3, 0]).strip()
            self._release_date = self._parse_release_date(date_str)
        except Exception:
            self._release_date = "Survey"
        
        # Initialize forecasters data as None
        self._forecasters_data = None
        
        # Store the processed worksheet data in the parent class's _worksheet attribute
        self._worksheet = worksheet_data

    def _parse_release_date(self, date_str: str) -> str:
        """Parse release date string into YYYYMMDD format."""
        # First try the YYYY-MM-DD HH:MM:SS format
        if '-' in date_str:
            date_parts = date_str.split()[0].split('-')
            if len(date_parts) == 3:
                year, month, day = date_parts
                return f"{year}{month}{day}"
        
        # If that fails, try the text format (e.g., "March 5, 1990")
        import re
        pattern = r'(\w+)\s+(\d+),?\s+(\d{4})'
        match = re.match(pattern, date_str)
        if match:
            month, day, year = match.groups()
            month_num = DateFormatUtils.month_to_number(month)
            if month_num != "00":
                return f"{year}{month_num}{int(day):02d}"
        
        return "Survey"

    @property
    def forecasters_data(self) -> DataFrame:
        """DataFrame containing processed forecaster data."""
        if self._forecasters_data is None:
            self._forecasters_data = self.get_forecasters_data()
            self.clean_forecasters_dataframe()
        return self._forecasters_data
    
    @property
    def column_names(self) -> List[str]:
        """List of column names from the worksheet."""
        return self._column_names
    
    @property
    def release_date(self) -> str:
        """The formatted release date."""
        return self._release_date

    def get_forecasters_data(self) -> DataFrame:
        """
        Processes and extracts forecasters data from the worksheet.
        
        Returns:
            DataFrame: Processed forecaster data
        """
        # Pre-compute common values and constants
        SUMMARY_STATS = {'Consensus (Mean)', 'High', 'Low', 'Standard Deviation', 'Number of Forecasts'}
        
        # Extract data sections efficiently
        summary_data = self._worksheet.iloc[6:13].reset_index(drop=True)
        forecasters_data = self._worksheet.iloc[25:].reset_index(drop=True)
        
        # Pre-compute empty status for all columns at once
        first_row_empty = forecasters_data.iloc[0].astype(str).str.strip() == ''
        empty_cols = first_row_empty.values
        
        # Find variable sets more efficiently
        cols_to_keep = []
        col_idx = 0
        while col_idx < len(empty_cols):
            if not empty_cols[col_idx]:
                start_idx = col_idx
                while col_idx < len(empty_cols) and not empty_cols[col_idx]:
                    col_idx += 1
                cols_to_keep.extend(range(start_idx, col_idx))
            else:
                col_idx += 1
        
        # Extract and clean columns in one go
        summary_data = summary_data.iloc[:, cols_to_keep]
        forecasters_data = forecasters_data.iloc[:, cols_to_keep]
        
        # Clean column names efficiently
        column_names = [x if x != '' else summary_data.columns[i-1] for i, x in enumerate(summary_data.columns)]
        summary_data.columns = column_names
        forecasters_data.columns = column_names
        
        # Drop empty columns
        summary_data = summary_data.dropna(axis=1, how='all')
        forecasters_data = forecasters_data.dropna(axis=1, how='all')
        
        # Pre-filter summary rows once
        summary_rows = summary_data[summary_data.iloc[:, 0].str.strip().isin(SUMMARY_STATS)]
        
        # Initialize final data with estimated size
        final_data = []
        final_data_append = final_data.append  # Local reference for faster appends
        
        i = 1
        while i < len(summary_data.columns):
            # Skip empty columns efficiently
            if summary_data[summary_data.columns[i]].isna().all().iloc[0]:
                i += 1
                continue
            
            # Get column references
            current_col = summary_data.columns[i]
            next_col = summary_data.columns[i + 1] if i + 1 < len(summary_data.columns) else None
            next_next_col = summary_data.columns[i + 2] if i + 2 < len(summary_data.columns) else None
            
            # Efficient triple check
            is_triple = (current_col == next_col and 
                        ((next_col == next_next_col) or (next_next_col == '')))
            
            # Process summary statistics
            for _, row in summary_rows.iterrows():
                stat_type = row.iloc[0].strip()
                
                # Get all values at once
                current_value = row[current_col]
                if isinstance(current_value, pd.Series):
                    current_value = current_value.iloc[0]
                
                try:
                    if not is_triple:
                        if not (isinstance(current_value, str) and (current_value.strip().lower() == 'na' or not current_value.strip())):
                            value_float = float(current_value) if isinstance(current_value, str) else current_value
                            final_data_append({
                                'type': stat_type,
                                'variable': current_col,
                                'value': value_float,
                                'year': self.year
                            })
                    else:
                        # Process triple case (monetary policy)
                        if not (isinstance(current_value, str) and (current_value.strip().lower() == 'na' or not current_value.strip())):
                            next_value = row[next_col]
                            next_next_value = row[next_next_col] if next_next_col is not None else None
                            
                            # Process all three values at once
                            values = [
                                (current_value, 'Increase'),
                                (next_value, 'No Change'),
                                (next_next_value, 'Decrease')
                            ]
                            
                            for val, type_name in values:
                                if pd.notna(val) and str(val).strip().lower() != 'na':
                                    value_float = float(val) if isinstance(val, str) else val
                                    final_data_append({
                                        'type': type_name,
                                        'variable': current_col,
                                        'value': value_float,
                                        'year': self.year
                                    })
                except (ValueError, TypeError):
                    pass
            
                # Process next year value if it exists and not a triple
                if next_col is not None and not is_triple:
                    next_value = row[next_col]
                    if isinstance(next_value, pd.Series):
                        next_value = next_value.iloc[1]
                    
                    try:
                        if not (isinstance(next_value, str) and (next_value.strip().lower() == 'na' or not next_value.strip())):
                            value_float = float(next_value) if isinstance(next_value, str) else next_value
                            final_data_append({
                                'type': stat_type,
                                'variable': current_col,
                                'value': value_float,
                                'year': self.year + 1
                            })
                    except (ValueError, TypeError):
                        pass
            
            # Process forecasters efficiently
            if current_col in forecasters_data.columns and not is_triple:
                if not forecasters_data[current_col].isna().all().iloc[0]:
                    valid_forecasters = forecasters_data[forecasters_data.iloc[:, 0].astype(str).str.strip() != '']
                    
                    for _, row in valid_forecasters.iterrows():
                        forecaster = row.iloc[0]
                        
                        # Process current year value
                        current_value = row[current_col]
                        if isinstance(current_value, pd.Series):
                            current_value = current_value.iloc[0]
                        
                        try:
                            if not (isinstance(current_value, str) and (current_value.strip().lower() == 'na' or not current_value.strip())):
                                value_float = float(current_value) if isinstance(current_value, str) else current_value
                                final_data_append({
                                    'type': forecaster,
                                    'variable': current_col,
                                    'value': value_float,
                                    'year': self.year
                                })
                        except (ValueError, TypeError):
                            pass
                        
                        # Process next year value if it exists
                        if next_col is not None:
                            next_value = row[next_col]
                            if isinstance(next_value, pd.Series):
                                next_value = next_value.iloc[1]  # Use iloc[1] consistently for next year values
                            
                            try:
                                if not (isinstance(next_value, str) and (next_value.strip().lower() == 'na' or not next_value.strip())):
                                    value_float = float(next_value) if isinstance(next_value, str) else next_value
                                    final_data_append({
                                        'type': forecaster,
                                        'variable': current_col,
                                        'value': value_float,
                                        'year': self.year + 1
                                    })
                            except (ValueError, TypeError):
                                pass
            
            i += 3 if is_triple else 2
        
        # Convert to DataFrame only once at the end
        if not final_data:
            return pd.DataFrame()
        
        return pd.DataFrame(final_data)

    def _is_empty_column(self, df: DataFrame, col_idx: int) -> bool:
        """Check if a column is empty (used as a separator)."""
        return all(
            str(x).strip() == '' 
            for x in df.iloc[:, col_idx]
        )

    def clean_forecasters_dataframe(self) -> None:
        """
        Cleans and transforms the forecasters dataframe with additional metadata.
        
        This method:
        1. Extracts units from variables
        2. Gets the release date
        3. Adds metadata columns (units, release_date, month, country)
        """
        if self._forecasters_data.empty:
            return
            
        # Get units from variables DataFrame
        units = self._variables.iloc[2]
        
        # Add metadata columns
        self._forecasters_data["unit"] = self._forecasters_data["variable"].map(units.to_dict())
        self._forecasters_data["release_date"] = self._release_date
        self._forecasters_data["month"] = self.month
        self._forecasters_data["country"] = self.sheet_name
        
        # Sort the data to have summary stats first
        summary_order = ["Consensus (Mean)", "High", "Low", "Standard Deviation", "Number of Forecasts"]
        self._forecasters_data["sort_order"] = self._forecasters_data["type"].apply(
            lambda x: summary_order.index(x) if x in summary_order else len(summary_order)
        )
        self._forecasters_data = self._forecasters_data.sort_values(["variable", "sort_order"]).drop("sort_order", axis=1)

    def get_variable_metadata(self) -> DataFrame:
        """
        Returns the metadata for variables (rows 3-6 in the worksheet).
        
        Returns:
            DataFrame: Variable metadata including units and descriptions
        """
        if self._variables is None:
            self.get_forecasters_data()
        return self._variables