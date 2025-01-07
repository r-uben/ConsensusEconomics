


from typing import List

class CountriesUtils:
    def __init__(self):
        self.__countries = None

    @property
    def countries(self) -> List[str]:
        if self.__countries is None:
            self.__countries = [
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
                "South Africa"
            ]
        return self.__countries
    
    @countries.setter
    def countries(self, countries: List[str]) -> None:
        # Add any validation logic here
        if not all(isinstance(country, str) for country in countries):
            raise ValueError("All countries must be strings")
        self.__countries = countries