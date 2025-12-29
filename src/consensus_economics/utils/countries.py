"""Country utilities for Consensus Economics data."""

from typing import List

from consensus_economics.config import COUNTRIES


class CountriesUtils:
    """Utility class for managing country data."""

    def __init__(self) -> None:
        self._countries: List[str] = list(COUNTRIES)

    @property
    def countries(self) -> List[str]:
        """Get the list of countries."""
        return self._countries

    @countries.setter
    def countries(self, countries: List[str]) -> None:
        """
        Set the list of countries.

        Args:
            countries: List of country names

        Raises:
            ValueError: If any item is not a string
        """
        if not all(isinstance(country, str) for country in countries):
            raise ValueError("All countries must be strings")
        self._countries = countries
