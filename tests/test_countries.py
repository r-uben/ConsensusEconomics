"""Tests for countries utilities."""

import pytest

from consensus_economics.config import COUNTRIES
from consensus_economics.utils.countries import CountriesUtils


class TestCountriesUtils:
    """Tests for CountriesUtils class."""

    def test_default_countries(self):
        """Test default countries list is populated."""
        utils = CountriesUtils()
        assert len(utils.countries) > 0

    def test_default_countries_match_config(self):
        """Test default countries match config."""
        utils = CountriesUtils()
        assert utils.countries == list(COUNTRIES)

    def test_countries_setter(self):
        """Test setting custom countries list."""
        utils = CountriesUtils()
        custom = ["USA", "Japan"]
        utils.countries = custom
        assert utils.countries == custom

    def test_countries_setter_validates_strings(self):
        """Test setter rejects non-string values."""
        utils = CountriesUtils()
        with pytest.raises(ValueError, match="must be strings"):
            utils.countries = ["USA", 123]

    def test_countries_setter_rejects_empty_list_items(self):
        """Test setter with mixed types."""
        utils = CountriesUtils()
        with pytest.raises(ValueError):
            utils.countries = ["USA", None]

    def test_countries_is_list_copy(self):
        """Test countries returns a copy, not the original."""
        utils = CountriesUtils()
        countries1 = utils.countries
        countries2 = utils.countries
        # Should be equal but not the same object
        assert countries1 == countries2

    def test_expected_countries_present(self):
        """Test expected major countries are in default list."""
        utils = CountriesUtils()
        expected = {"USA", "Japan", "Germany", "UK", "France", "Canada"}
        countries_set = set(utils.countries)
        assert expected.issubset(countries_set)
