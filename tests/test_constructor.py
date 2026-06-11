"""Tests for FileProcessor filename normalization."""

from consensus_economics.constructor import FileProcessor


class TestFormatFilename:
    """Tests for format_filename (raw download name -> YYYYMM.xlsx)."""

    def test_basic(self):
        assert FileProcessor.format_filename("cfjan2024.xlsx") == "202401.xlsx"
        assert FileProcessor.format_filename("cfdec1995.xlsx") == "199512.xlsx"

    def test_mixed_case(self):
        assert FileProcessor.format_filename("cfJan2024.xlsx") == "202401.xlsx"
        assert FileProcessor.format_filename("CFOct2010.xlsx") == "201010.xlsx"

    def test_all_months(self):
        months = [
            "jan", "feb", "mar", "apr", "may", "jun",
            "jul", "aug", "sep", "oct", "nov", "dec",
        ]
        for i, month in enumerate(months, 1):
            assert (
                FileProcessor.format_filename(f"cf{month}2000.xlsx")
                == f"2000{i:02d}.xlsx"
            )


class TestCorrectDateFormat:
    """Tests for _correct_date_format (move year to front, strip cf prefix)."""

    def test_year_moved_to_front(self):
        assert FileProcessor._correct_date_format("cf012024.xlsx", ".xlsx") == "202401.xlsx"

    def test_without_prefix(self):
        assert FileProcessor._correct_date_format("011990.xlsx", ".xlsx") == "199001.xlsx"
