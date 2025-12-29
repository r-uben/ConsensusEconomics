"""Forex worksheet parser for Consensus Economics data."""

from typing import Optional

import pandas as pd
from pandas import DataFrame

from consensus_economics.config import CURRENCY_CODES
from consensus_economics.utils.date_format import DateFormatUtils
from consensus_economics.worksheets.base_worksheet import BaseWorksheet


class ForexWorksheet(BaseWorksheet):
    """Handles the processing of forex worksheet data."""

    def __init__(self, date: str) -> None:
        super().__init__(date, sheet_name="Forex")
        self._initialize_properties()

    def _initialize_properties(self) -> None:
        """Initialize all properties at once to avoid multiple worksheet accesses."""
        worksheet_data = self.worksheet.copy()

        # Get release date using shared utility
        try:
            date_str = str(worksheet_data.iloc[3, 0]).strip()
            self._release_date = DateFormatUtils.parse_release_date(date_str)
        except Exception:
            self._release_date = "Survey"

        self._forecasters_data: Optional[DataFrame] = None
        self._worksheet = worksheet_data

    @property
    def forecasters_data(self) -> DataFrame:
        """DataFrame containing processed forecaster data."""
        if self._forecasters_data is None:
            self._forecasters_data = self.get_forecasters_data()
        return self._forecasters_data

    @property
    def release_date(self) -> str:
        """The formatted release date."""
        return self._release_date

    def get_forecasters_data(self) -> DataFrame:
        """Process forex data into a structured DataFrame."""
        df = self._worksheet

        # Keep relevant columns: currency name and all forecast-related columns
        columns_to_keep = [0] + list(range(3, 11))

        # Extract USD section (rows 10-19, adjusting for 0-based index)
        usd_data = df.iloc[8:19, columns_to_keep].copy()

        # Extract EUR section (rows 21-25)
        eur_data = df.iloc[19:25, columns_to_keep].copy()

        # Process each section
        usd_processed = self._process_section(usd_data, reference="USD")
        eur_processed = self._process_section(eur_data, reference="EUR")

        # Combine both sections and add metadata
        result = pd.concat([usd_processed, eur_processed], ignore_index=True)
        result["year"] = self.year
        result["release_date"] = self.release_date

        # Round values to 6 decimal places
        result["current_value"] = result["current_value"].round(6)
        result["forecasted_value"] = result["forecasted_value"].round(6)

        # Reorder columns: currency, reference, year, horizon, current_value, forecasted_value, release_date
        result = result[
            ["currency", "reference", "year", "horizon", "current_value", "forecasted_value", "release_date"]
        ]

        return result

    def _process_section(self, df: DataFrame, reference: str) -> DataFrame:
        """Process a section (USD or EUR) of the forex data."""
        # Skip the header rows for data processing
        data = df.iloc[2:].copy()

        # Clean currency names and map to codes
        data = data.dropna(subset=[0])
        currencies = data[0].str.strip()
        currency_codes = currencies.map(CURRENCY_CODES)

        records = []

        # Current values are in column index 3
        current_values = pd.to_numeric(data[3], errors="coerce")

        # Process each currency
        for idx, (currency, current_value) in enumerate(zip(currency_codes, current_values)):
            if pd.isna(currency):
                continue

            # Process each forecast horizon
            # Columns are: [currency, ..., current(3), 3m(4), ..., 12m(6), ..., 24m(8)]
            for col_idx, horizon in [(3, 3), (5, 12), (7, 24)]:
                try:
                    forecasted_value = pd.to_numeric(data.iloc[idx, col_idx])

                    if not pd.isna(forecasted_value) and not pd.isna(current_value):
                        records.append({
                            "currency": currency,
                            "reference": reference,
                            "current_value": current_value,
                            "forecasted_value": forecasted_value,
                            "horizon": horizon,
                        })
                except (ValueError, IndexError):
                    pass

        return pd.DataFrame(records)
