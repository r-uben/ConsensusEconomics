"""Date formatting utilities for Consensus Economics data."""

import re

import pandas as pd

from consensus_economics.config import MONTH_MAP


class DateFormatUtils:
    """Utility class for date formatting operations."""

    @staticmethod
    def get_date(year: int, month: int) -> str:
        """
        Format year and month into YYYYMM string.

        Args:
            year: Four-digit year
            month: Month number (1-12)

        Returns:
            Date string in YYYYMM format
        """
        return f"{year}{month:02d}"

    @staticmethod
    def month_to_number(month: str) -> str:
        """
        Convert month name to two-digit number string.

        Args:
            month: Month name (e.g., "January", "Jan")

        Returns:
            Two-digit month string (e.g., "01") or "00" if not found
        """
        return MONTH_MAP.get(month.strip(), "00")

    @staticmethod
    def parse_release_date(date_str: str) -> str:
        """
        Parse a release date string into YYYYMMDD format.

        Handles two formats:
        - ISO format: "YYYY-MM-DD HH:MM:SS"
        - Text format: "March 5, 1990"

        Args:
            date_str: Date string to parse

        Returns:
            Date in YYYYMMDD format, or "" if parsing fails (empty keeps the
            column single-typed instead of mixing dates with a sentinel word)
        """
        date_str = str(date_str).strip()

        # Try ISO format first (YYYY-MM-DD HH:MM:SS)
        if "-" in date_str:
            date_parts = date_str.split()[0].split("-")
            if len(date_parts) == 3:
                year, month, day = date_parts
                return f"{year}{month}{day}"

        # Try text format (e.g., "March 5, 1990")
        pattern = r"(\w+)\s+(\d+),?\s+(\d{4})"
        match = re.match(pattern, date_str)
        if match:
            month, day, year = match.groups()
            month_num = DateFormatUtils.month_to_number(month)
            if month_num != "00":
                return f"{year}{month_num}{int(day):02d}"

        return ""

    @staticmethod
    def formatted_release_date(df: pd.DataFrame) -> str:
        """
        Extract formatted release date from a worksheet DataFrame.

        Args:
            df: DataFrame containing worksheet data (before cleaning)

        Returns:
            Date in YYYYMMDD format, or "" if extraction fails
        """
        try:
            if df.empty or df.shape[1] < 2:
                return ""

            # Check for "Survey Date:" in first column
            survey_mask = df.iloc[:, 0].astype(str).str.contains("Survey Date:", na=False)
            if survey_mask.any():
                row_idx = survey_mask.idxmax()
                if row_idx < df.shape[0] and df.shape[1] > 1:
                    date_str = str(df.iloc[row_idx, 1])
                    if pd.notna(date_str):
                        result = DateFormatUtils.parse_release_date(date_str)
                        if result:
                            return result

            # Fallback: try column header
            if len(df.columns) > 0:
                header = str(df.columns[0])
                parts = header.split()
                if len(parts) >= 3:
                    try:
                        day = parts[-2].replace(",", "").zfill(2)
                        month_num = DateFormatUtils.month_to_number(parts[-3])
                        year = parts[-1]
                        if month_num != "00":
                            return f"{year}{month_num}{day}"
                    except (IndexError, ValueError):
                        pass

            return ""

        except Exception:
            return ""
