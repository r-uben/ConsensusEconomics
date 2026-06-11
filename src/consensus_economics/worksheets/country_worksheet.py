"""Country worksheet parser for Consensus Economics data."""

from typing import List, Optional

import pandas as pd
from pandas import DataFrame

from consensus_economics.config import SUMMARY_STATS
from consensus_economics.utils.date_format import DateFormatUtils
from consensus_economics.worksheets.base_worksheet import BaseWorksheet


class CountryWorksheet(BaseWorksheet):
    """
    Handles the processing of country-specific worksheet data.

    Args:
        date: Date in format 'yyyymm'
        country: Country name matching worksheet

    Example:
        >>> worksheet = CountryWorksheet(date='202409', country='Canada')
        >>> df = worksheet.forecasters_data
    """

    def __init__(self, date: str, country: str) -> None:
        super().__init__(date, sheet_name=country)
        self._initialize_properties()

    def _initialize_properties(self) -> None:
        """Initialize all properties at once to avoid multiple worksheet accesses."""
        worksheet_data = self.worksheet.copy()

        # Process column names
        column_headers = worksheet_data.iloc[1:4]
        self._column_names: List[str] = column_headers.apply(
            lambda x: " ".join(x.dropna().astype(str).str.strip()),
            axis=0,
        ).tolist()

        worksheet_data.columns = self._column_names

        # Extract variables
        self._variables = worksheet_data.iloc[2:6].reset_index(drop=True)

        # Get release date using shared utility
        try:
            date_str = str(worksheet_data.iloc[3, 0]).strip()
            self._release_date = DateFormatUtils.parse_release_date(date_str)
        except Exception:
            self._release_date = ""

        self._forecasters_data: Optional[DataFrame] = None
        self._skipped_cells = 0
        self._worksheet = worksheet_data

    @property
    def forecasters_data(self) -> DataFrame:
        """DataFrame containing processed forecaster data."""
        if self._forecasters_data is None:
            self._forecasters_data = self.get_forecasters_data()
            self._clean_forecasters_dataframe()
        return self._forecasters_data

    @property
    def column_names(self) -> List[str]:
        """List of column names from the worksheet."""
        return self._column_names

    @property
    def release_date(self) -> str:
        """The formatted release date."""
        return self._release_date

    @property
    def skipped_cells(self) -> int:
        """Number of non-empty cells that failed numeric conversion and were skipped."""
        return self._skipped_cells

    def get_forecasters_data(self) -> DataFrame:
        """
        Process and extract forecasters data from the worksheet.

        Returns:
            Processed forecaster data
        """
        # Extract data sections
        summary_data = self._worksheet.iloc[6:13].reset_index(drop=True)
        forecasters_data = self._worksheet.iloc[25:].reset_index(drop=True)

        # Find non-empty columns
        first_row_empty = forecasters_data.iloc[0].astype(str).str.strip() == ""
        empty_cols = first_row_empty.values

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

        summary_data = summary_data.iloc[:, cols_to_keep]
        forecasters_data = forecasters_data.iloc[:, cols_to_keep]

        # Clean column names
        column_names = [
            x if x != "" else summary_data.columns[i - 1]
            for i, x in enumerate(summary_data.columns)
        ]
        summary_data.columns = column_names
        forecasters_data.columns = column_names

        summary_data = summary_data.dropna(axis=1, how="all")
        forecasters_data = forecasters_data.dropna(axis=1, how="all")

        # Pre-filter summary rows
        summary_rows = summary_data[
            summary_data.iloc[:, 0].str.strip().isin(SUMMARY_STATS)
        ]

        final_data = []

        i = 1
        while i < len(summary_data.columns):
            if self._first_column(summary_data[summary_data.columns[i]]).isna().all():
                i += 1
                continue

            columns = summary_data.columns
            current_col = columns[i]
            next_col = columns[i + 1] if i + 1 < len(columns) else None
            next_next_col = columns[i + 2] if i + 2 < len(columns) else None

            is_triple = current_col == next_col and (
                (next_col == next_next_col) or (next_next_col == "")
            )

            # Process summary statistics
            for _, row in summary_rows.iterrows():
                stat_type = row.iloc[0].strip()
                current_value = row[current_col]
                if isinstance(current_value, pd.Series):
                    current_value = current_value.iloc[0]

                try:
                    if not is_triple:
                        if not self._is_na_value(current_value):
                            value_float = self._to_float(current_value)
                            final_data.append({
                                "type": stat_type,
                                "variable": current_col,
                                "value": value_float,
                                "year": self.year,
                            })
                    else:
                        # Process triple case (monetary policy)
                        if not self._is_na_value(current_value):
                            next_value = row[next_col]
                            next_next_value = row[next_next_col] if next_next_col else None

                            values = [
                                (current_value, "Increase"),
                                (next_value, "No Change"),
                                (next_next_value, "Decrease"),
                            ]

                            for val, type_name in values:
                                if pd.notna(val) and str(val).strip().lower() != "na":
                                    value_float = self._to_float(val)
                                    final_data.append({
                                        "type": type_name,
                                        "variable": current_col,
                                        "value": value_float,
                                        "year": self.year,
                                    })
                except (ValueError, TypeError):
                    self._skipped_cells += 1

                # Process next year value if exists and not triple
                if next_col is not None and not is_triple:
                    next_value = row[next_col]
                    if isinstance(next_value, pd.Series):
                        next_value = next_value.iloc[1]

                    try:
                        if not self._is_na_value(next_value):
                            value_float = self._to_float(next_value)
                            final_data.append({
                                "type": stat_type,
                                "variable": current_col,
                                "value": value_float,
                                "year": self.year + 1,
                            })
                    except (ValueError, TypeError):
                        self._skipped_cells += 1

            # Process forecasters
            if current_col in forecasters_data.columns and not is_triple:
                if not self._first_column(forecasters_data[current_col]).isna().all():
                    valid_forecasters = forecasters_data[
                        forecasters_data.iloc[:, 0].astype(str).str.strip() != ""
                    ]

                    for _, row in valid_forecasters.iterrows():
                        forecaster = row.iloc[0]
                        current_value = row[current_col]
                        if isinstance(current_value, pd.Series):
                            current_value = current_value.iloc[0]

                        try:
                            if not self._is_na_value(current_value):
                                value_float = self._to_float(current_value)
                                final_data.append({
                                    "type": forecaster,
                                    "variable": current_col,
                                    "value": value_float,
                                    "year": self.year,
                                })
                        except (ValueError, TypeError):
                            self._skipped_cells += 1

                        if next_col is not None:
                            next_value = row[next_col]
                            if isinstance(next_value, pd.Series):
                                next_value = next_value.iloc[1]

                            try:
                                if not self._is_na_value(next_value):
                                    value_float = self._to_float(next_value)
                                    final_data.append({
                                        "type": forecaster,
                                        "variable": current_col,
                                        "value": value_float,
                                        "year": self.year + 1,
                                    })
                            except (ValueError, TypeError):
                                self._skipped_cells += 1

            i += 3 if is_triple else 2

        if not final_data:
            return pd.DataFrame()

        return pd.DataFrame(final_data)

    @staticmethod
    def _to_float(value) -> float:
        return float(value) if isinstance(value, str) else value

    @staticmethod
    def _first_column(data):
        """First column of a duplicate-label selection; identity for a Series.

        df[col] returns a DataFrame when the label is duplicated (the usual
        current-year/next-year column pairs) but a Series when a sheet has a
        variable with a single year column.
        """
        return data.iloc[:, 0] if isinstance(data, pd.DataFrame) else data

    @staticmethod
    def _is_na_value(value) -> bool:
        """Check if a value represents NA/empty."""
        if pd.isna(value):
            return True
        if isinstance(value, str):
            stripped = value.strip().lower()
            return stripped == "na" or stripped == ""
        return False

    def _clean_forecasters_dataframe(self) -> None:
        """Clean and transform the forecasters dataframe with additional metadata."""
        if self._forecasters_data.empty:
            return

        units = self._variables.iloc[2]

        # Map type to source and statistic
        stat_mapping = {
            "Consensus (Mean)": ("Consensus", "mean"),
            "Standard Deviation": ("Consensus", "std_dev"),
            "High": ("Consensus", "high"),
            "Low": ("Consensus", "low"),
            "Number of Forecasts": ("Consensus", "count"),
        }

        def get_source(t: str) -> str:
            return stat_mapping.get(t, (t, "forecast"))[0]

        def get_statistic(t: str) -> str:
            return stat_mapping.get(t, (t, "forecast"))[1]

        self._forecasters_data["source"] = self._forecasters_data["type"].apply(get_source)
        self._forecasters_data["statistic"] = self._forecasters_data["type"].apply(get_statistic)
        self._forecasters_data.drop("type", axis=1, inplace=True)

        # Add metadata
        self._forecasters_data["unit"] = self._forecasters_data["variable"].map(units.to_dict())
        self._forecasters_data["release_date"] = self._release_date
        self._forecasters_data["country"] = self.sheet_name

        # Round values to 6 decimal places
        self._forecasters_data["value"] = self._forecasters_data["value"].round(6)

        # Reorder columns: country, variable, source, statistic, year, value, unit, release_date
        self._forecasters_data = self._forecasters_data[
            ["country", "variable", "source", "statistic", "year", "value", "unit", "release_date"]
        ]

        # Sort by variable, then source (Consensus first), then statistic
        stat_order = {"mean": 0, "std_dev": 1, "high": 2, "low": 3, "count": 4, "forecast": 5}
        self._forecasters_data["_sort"] = self._forecasters_data["statistic"].map(stat_order)
        self._forecasters_data["_source_sort"] = (
            self._forecasters_data["source"] != "Consensus"
        ).astype(int)
        self._forecasters_data = self._forecasters_data.sort_values(
            ["variable", "_source_sort", "_sort", "source"]
        ).drop(["_sort", "_source_sort"], axis=1)

    def get_variable_metadata(self) -> DataFrame:
        """
        Returns the metadata for variables (rows 3-6 in the worksheet).

        Returns:
            Variable metadata including units and descriptions
        """
        if self._variables is None:
            self.get_forecasters_data()
        return self._variables
