"""
Configuration module for Consensus Economics data pipeline.

Contains all configurable values: countries, currencies, date ranges, etc.
"""

from pathlib import Path
from typing import Dict, List


# Storage paths
EXTERNAL_STORAGE: Path = Path("/Volumes/Main/Library/Databases/consensus_economics")



# Countries covered in Consensus Economics surveys
COUNTRIES: List[str] = [
    "USA",
    "Japan",
    "Germany",
    "France",
    "UK",
    "Italy",
    "Canada",
    "Euro Zone",
    "Netherlands",
    "Norway",
    "Spain",
    "Sweden",
    "Switzerland",
    "Austria",
    "Belgium",
    "Denmark",
    "Egypt",
    "Finland",
    "Greece",
    "Ireland",
    "Israel",
    "Nigeria",
    "Portugal",
    "Saudi Arabia",
    "South Africa",
]

# Currency code mappings for forex data
CURRENCY_CODES: Dict[str, str] = {
    "Canadian Dollar": "CAD",
    "Egyptian Pound": "EGP",
    "European Euro": "EUR",
    "Israeli Shekel": "ILS",
    "Japanese Yen": "JPY",
    "Nigerian Naira": "NGN",
    "Saudi Arabian Riyal": "SAR",
    "South African Rand": "ZAR",
    "United Kingdom Pound": "GBP",
    "Danish Krone": "DKK",
    "Norwegian Krone": "NOK",
    "Swedish Krona": "SEK",
    "Swiss Franc": "CHF",
}

# Month name to number mappings (both abbreviated and full)
MONTH_MAP: Dict[str, str] = {
    "Jan": "01", "January": "01",
    "Feb": "02", "February": "02",
    "Mar": "03", "March": "03",
    "Apr": "04", "April": "04",
    "May": "05",
    "Jun": "06", "June": "06",
    "Jul": "07", "July": "07",
    "Aug": "08", "August": "08",
    "Sep": "09", "September": "09",
    "Oct": "10", "October": "10",
    "Nov": "11", "November": "11",
    "Dec": "12", "December": "12",
}

# Data collection year range
START_YEAR: int = 1990
END_YEAR: int = 2026  # Exclusive upper bound

# Summary statistics labels in worksheets
SUMMARY_STATS: frozenset = frozenset({
    "Consensus (Mean)",
    "High",
    "Low",
    "Standard Deviation",
    "Number of Forecasts",
})
