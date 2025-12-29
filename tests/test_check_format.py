"""Tests for file format checking utilities."""

import pytest

from consensus_economics.utils.check_format import CheckFormatUtils


class TestIsZip:
    """Tests for iszip function."""

    def test_lowercase_zip(self):
        """Test lowercase .zip extension."""
        assert CheckFormatUtils.iszip("file.zip") is True

    def test_uppercase_zip(self):
        """Test uppercase .ZIP extension."""
        assert CheckFormatUtils.iszip("file.ZIP") is True

    def test_mixed_case_zip(self):
        """Test mixed case .Zip extension."""
        assert CheckFormatUtils.iszip("file.Zip") is True

    def test_non_zip_file(self):
        """Test non-zip files return False."""
        assert CheckFormatUtils.iszip("file.xlsx") is False
        assert CheckFormatUtils.iszip("file.txt") is False
        assert CheckFormatUtils.iszip("file.csv") is False

    def test_zip_in_name(self):
        """Test 'zip' in filename but not extension."""
        assert CheckFormatUtils.iszip("zipfile.txt") is False

    def test_empty_string(self):
        """Test empty string returns False."""
        assert CheckFormatUtils.iszip("") is False


class TestIsXlsx:
    """Tests for isxlsx function."""

    def test_lowercase_xlsx(self):
        """Test lowercase .xlsx extension."""
        assert CheckFormatUtils.isxlsx("file.xlsx") is True

    def test_uppercase_xlsx(self):
        """Test uppercase .XLSX extension."""
        assert CheckFormatUtils.isxlsx("file.XLSX") is True

    def test_mixed_case_xlsx(self):
        """Test mixed case .Xlsx extension."""
        assert CheckFormatUtils.isxlsx("file.Xlsx") is True

    def test_non_xlsx_file(self):
        """Test non-xlsx files return False."""
        assert CheckFormatUtils.isxlsx("file.xls") is False
        assert CheckFormatUtils.isxlsx("file.csv") is False
        assert CheckFormatUtils.isxlsx("file.txt") is False

    def test_xlsx_in_name(self):
        """Test 'xlsx' in filename but not extension."""
        assert CheckFormatUtils.isxlsx("xlsxfile.txt") is False

    def test_empty_string(self):
        """Test empty string returns False."""
        assert CheckFormatUtils.isxlsx("") is False
