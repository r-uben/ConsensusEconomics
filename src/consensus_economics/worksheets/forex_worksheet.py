from typing import List, Dict
from pandas import DataFrame
import pandas as pd
from .base_worksheet import BaseWorksheet

# Note: This code requires proper AWS credentials configuration to work
# See: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html

class ForexWorksheet(BaseWorksheet):
    """Handles the processing of forex worksheet data."""

    CURRENCY_CODES: Dict[str, str] = {
        'Canadian Dollar': 'CAD',
        'Egyptian Pound': 'EGP',
        'European Euro': 'EUR',
        'Israeli Shekel': 'ILS',
        'Japanese Yen': 'JPY',
        'Nigerian Naira': 'NGN',
        'Saudi Arabian Riyal': 'SAR',
        'South African Rand': 'ZAR',
        'United Kingdom Pound': 'GBP',
        'Danish Krone': 'DKK',
        'Norwegian Krone': 'NOK',
        'Swedish Krona': 'SEK',
        'Swiss Franc': 'CHF'
    }

    def __init__(self, date: str) -> None:
        super().__init__(date, sheet_name="Forex")
        self._forecasters_data = None

    @property
    def forecasters_data(self) -> DataFrame:
        """DataFrame containing processed forecaster data."""
        if self._forecasters_data is None:
            self._forecasters_data = self.get_forecasters_data()
        return self._forecasters_data

    def get_forecasters_data(self) -> DataFrame:
        """Process forex data into a structured DataFrame."""
        df = self.worksheet

        # Keep relevant columns: currency name and all forecast-related columns
        columns_to_keep = [0] + [x for x in range(3, 10+1)]
        
        # Extract USD section (rows 10-19, adjusting for 0-based index)
        usd_data = df.iloc[8:19, columns_to_keep].copy()
        
        # Extract EUR section (rows 21-25)
        eur_data = df.iloc[19:25, columns_to_keep].copy()
        
        # Process each section
        usd_processed = self._process_section(usd_data, reference="USD")
        eur_processed = self._process_section(eur_data, reference="EUR")
        
        # Combine both sections and add date
        result = pd.concat([usd_processed, eur_processed], ignore_index=True)
        result['date'] = self.date
        
        return result

    def _process_section(self, df: DataFrame, reference: str) -> DataFrame:
        """Process a section (USD or EUR) of the forex data."""
        # Skip the header rows for data processing
        data = df.iloc[2:].copy()
        
        # Clean currency names and map to codes
        data = data.dropna(subset=[0])
        currencies = data[0].str.strip()
        currency_codes = currencies.map(self.CURRENCY_CODES)
        
        records = []
        
        # Current values are in column index 3
        current_values = pd.to_numeric(data[3], errors='coerce')
        
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
                            'currency': currency,
                            'reference': reference,
                            'current_value': current_value,
                            'forecasted_value': forecasted_value,
                            'horizon': horizon
                        })
                except Exception as e:
                    print(f"Error processing {currency} for horizon {horizon}: {str(e)}")
        
        return pd.DataFrame(records)