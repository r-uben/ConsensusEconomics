"""Tests for date formatting utilities."""

import pandas as pd

from consensus_economics.utils.date_format import DateFormatUtils


class TestGetDate:
    """Tests for get_date function."""

    def test_basic_formatting(self):
        """Test basic year-month formatting."""
        assert DateFormatUtils.get_date(2024, 1) == "202401"
        assert DateFormatUtils.get_date(2024, 12) == "202412"

    def test_zero_padding(self):
        """Ensure months are zero-padded."""
        assert DateFormatUtils.get_date(1990, 1) == "199001"
        assert DateFormatUtils.get_date(2000, 9) == "200009"

    def test_various_years(self):
        """Test different year values."""
        assert DateFormatUtils.get_date(1990, 6) == "199006"
        assert DateFormatUtils.get_date(2025, 11) == "202511"


class TestMonthToNumber:
    """Tests for month_to_number function."""

    def test_abbreviated_months(self):
        """Test abbreviated month names."""
        assert DateFormatUtils.month_to_number("Jan") == "01"
        assert DateFormatUtils.month_to_number("Feb") == "02"
        assert DateFormatUtils.month_to_number("Dec") == "12"

    def test_full_month_names(self):
        """Test full month names."""
        assert DateFormatUtils.month_to_number("January") == "01"
        assert DateFormatUtils.month_to_number("February") == "02"
        assert DateFormatUtils.month_to_number("December") == "12"

    def test_invalid_month(self):
        """Test invalid month returns '00'."""
        assert DateFormatUtils.month_to_number("Invalid") == "00"
        assert DateFormatUtils.month_to_number("") == "00"

    def test_whitespace_handling(self):
        """Test whitespace is stripped."""
        assert DateFormatUtils.month_to_number("  Jan  ") == "01"
        assert DateFormatUtils.month_to_number("March ") == "03"


class TestParseReleaseDate:
    """Tests for parse_release_date function."""

    def test_iso_format(self):
        """Test ISO date format parsing."""
        assert DateFormatUtils.parse_release_date("2024-01-15") == "20240115"
        assert DateFormatUtils.parse_release_date("1990-12-31") == "19901231"

    def test_iso_format_with_time(self):
        """Test ISO format with time component."""
        assert DateFormatUtils.parse_release_date("2024-01-15 10:30:00") == "20240115"

    def test_text_format(self):
        """Test text date format parsing."""
        assert DateFormatUtils.parse_release_date("March 5, 1990") == "19900305"
        assert DateFormatUtils.parse_release_date("January 15, 2024") == "20240115"

    def test_text_format_without_comma(self):
        """Test text format without comma."""
        assert DateFormatUtils.parse_release_date("March 5 1990") == "19900305"

    def test_invalid_format_returns_empty(self):
        """Test invalid formats return empty string."""
        assert DateFormatUtils.parse_release_date("invalid") == ""
        assert DateFormatUtils.parse_release_date("") == ""
        assert DateFormatUtils.parse_release_date("2024") == ""

    def test_single_digit_day(self):
        """Test single digit days are zero-padded."""
        result = DateFormatUtils.parse_release_date("March 5, 1990")
        assert result == "19900305"


class TestFormattedReleaseDate:
    """Tests for formatted_release_date function."""

    def test_empty_dataframe(self):
        """Test empty DataFrame returns empty string."""
        df = pd.DataFrame()
        assert DateFormatUtils.formatted_release_date(df) == ""

    def test_single_column_dataframe(self):
        """Test single column DataFrame returns empty string."""
        df = pd.DataFrame({"col1": [1, 2, 3]})
        assert DateFormatUtils.formatted_release_date(df) == ""

    def test_survey_date_extraction(self):
        """Test extraction from 'Survey Date:' row."""
        df = pd.DataFrame({
            0: ["Header", "Survey Date:", "Other"],
            1: ["Value", "March 15, 2024", "Value"],
        })
        assert DateFormatUtils.formatted_release_date(df) == "20240315"

    def test_no_survey_date_returns_empty(self):
        """Test DataFrame without survey date returns empty string."""
        df = pd.DataFrame({
            0: ["Header", "Other", "Data"],
            1: ["Value", "Value", "Value"],
        })
        assert DateFormatUtils.formatted_release_date(df) == ""
