"""Tests for config module."""


from consensus_economics.config import (
    COUNTRIES,
    CURRENCY_CODES,
    END_YEAR,
    MONTH_MAP,
    START_YEAR,
    SUMMARY_STATS,
)


class TestConfig:
    """Tests for configuration values."""

    def test_countries_not_empty(self):
        """Ensure countries list is populated."""
        assert len(COUNTRIES) > 0

    def test_countries_are_strings(self):
        """Ensure all countries are strings."""
        assert all(isinstance(c, str) for c in COUNTRIES)

    def test_expected_countries_present(self):
        """Check key countries are in the list."""
        expected = ["USA", "Japan", "Germany", "UK", "France"]
        for country in expected:
            assert country in COUNTRIES

    def test_currency_codes_not_empty(self):
        """Ensure currency codes dict is populated."""
        assert len(CURRENCY_CODES) > 0

    def test_currency_codes_valid(self):
        """Ensure currency codes are 3-letter strings."""
        for name, code in CURRENCY_CODES.items():
            assert isinstance(name, str)
            assert isinstance(code, str)
            assert len(code) == 3
            assert code.isupper()

    def test_expected_currencies_present(self):
        """Check key currencies are mapped."""
        expected_mappings = {
            "European Euro": "EUR",
            "Japanese Yen": "JPY",
            "United Kingdom Pound": "GBP",
        }
        for name, code in expected_mappings.items():
            assert CURRENCY_CODES.get(name) == code

    def test_month_map_complete(self):
        """Ensure all 12 months are mapped."""
        month_values = set(MONTH_MAP.values())
        expected = {f"{i:02d}" for i in range(1, 13)}
        assert expected == month_values

    def test_month_map_abbreviations(self):
        """Check month abbreviations are correct."""
        assert MONTH_MAP["Jan"] == "01"
        assert MONTH_MAP["Dec"] == "12"
        assert MONTH_MAP["June"] == "06"

    def test_year_range_valid(self):
        """Ensure year range is sensible."""
        assert START_YEAR < END_YEAR
        assert START_YEAR >= 1980
        assert END_YEAR <= 2050

    def test_summary_stats_immutable(self):
        """Ensure summary stats is a frozenset."""
        assert isinstance(SUMMARY_STATS, frozenset)

    def test_expected_summary_stats(self):
        """Check expected summary statistics are present."""
        expected = {"Consensus (Mean)", "High", "Low", "Standard Deviation"}
        assert expected.issubset(SUMMARY_STATS)
