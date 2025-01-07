from typing import List
from pandas import DataFrame
from consensus_economics.utils.date_format import DateFormatUtils
from consensus_economics.worksheets.base_worksheet import BaseWorksheet


class CountryWorksheet(BaseWorksheet):
    """
    Handles the processing of country-specific worksheet data.
    
    Args:
        date (str): Date in format 'yyyymm'
        country (str): Country name matching worksheet
    
    Example:
        >>> worksheet = CountryWorksheet(date='202409', country='Canada')
        >>> df = worksheet.forecasters_data
    """

    def __init__(self, date: str, country: str) -> None:
        super().__init__(date, sheet_name=country)
        self._forecasters_data = None
        self._variables = None
        self._column_names = None
        self._release_date = None

    @property
    def forecasters_data(self) -> DataFrame:
        """DataFrame containing processed forecaster data."""
        if self._forecasters_data is None:
            self._forecasters_data = self.get_forecasters_data()
            self.clean_forecasters_dataframe()
        return self._forecasters_data
    
    @property
    def column_names(self) -> List[str]:
        """List of column names from the worksheet."""
        if self._column_names is None:
            self._column_names = self.get_column_names()
        return self._column_names
    
    @property
    def release_date(self) -> str:
        """The formatted release date."""
        if self._release_date is None:
            self._release_date = self.formatted_release_date()
        return self._release_date

    def get_column_names(self) -> List[str]:
        """
        Extracts and processes column names from the worksheet.
        
        Returns:
            List[str]: Processed column names
        """
        column_headers = self.worksheet.iloc[1:4]
        column_names = column_headers.apply(
            lambda x: ' '.join(x.dropna().astype(str).str.strip()), 
            axis=0
        )
        return column_names.tolist()

    def get_forecasters_data(self) -> DataFrame:
        """
        Processes and extracts forecasters data from the worksheet.
        
        Returns:
            DataFrame: Processed forecasters data
        """
        self.worksheet.columns = self.column_names
        # Extract rows 3-6 for common information
        self._variables = self.worksheet.iloc[2:6].reset_index(drop=True)

        # Extract rows 26-52 for individual forecasters' values
        forecasters_data = self.worksheet.iloc[25:].reset_index(drop=True)
        forecasters_data = forecasters_data.iloc[:, [0] + list(range(2, forecasters_data.shape[1]))]
        
        # Clean column names
        forecasters_data.columns = [
            x if x != '' else forecasters_data.columns[i-1] 
            for i, x in enumerate(forecasters_data.columns)
        ]
        forecasters_data.dropna(axis=1, how='all', inplace=True)

        # Add year row
        years = [self.year, self.year + 1] * ((forecasters_data.shape[1] - 1) // 2)
        if len(years) < forecasters_data.shape[1] - 1:
            years += [self.year]  # For odd number of columns
        
        forecasters_data.loc[-1] = ["year"] + years
        forecasters_data.index = forecasters_data.index + 1
        return forecasters_data.sort_index()

    def clean_forecasters_dataframe(self) -> None:
        """
        Cleans and transforms the forecasters dataframe with additional metadata.
        
        This method:
        1. Extracts units from variables
        2. Gets the release date
        3. Transforms the dataframe structure
        4. Adds metadata columns (units, year, release_date, month, country)
        """
        # Get units from variables DataFrame
        units = self._variables.iloc[2]
        release_date = self.formatted_release_date()
        
        # Transform the dataframe
        self._forecasters_data = (
            self._forecasters_data
            .set_index(self._forecasters_data.columns[0])
            .rename_axis(None)
            .T
            .rename_axis("variable")
            .reset_index()
            .melt(
                id_vars=["variable", "year"], 
                var_name="forecaster", 
                value_name="value"
            )
        )
        
        # Add metadata columns
        self._forecasters_data["unit"] = self._forecasters_data["variable"].map(units.to_dict())
        self._forecasters_data["year"] = self._forecasters_data["year"].astype(int)
        self._forecasters_data["release_date"] = release_date
        self._forecasters_data["month"] = self.month
        self._forecasters_data["country"] = self.sheet_name

    def formatted_release_date(self) -> str:
        """
        Gets the formatted release date from the forecasters data.
        
        Returns:
            str: Formatted release date
        """
        return DateFormatUtils.formatted_release_date(self._forecasters_data)

    def get_variable_metadata(self) -> DataFrame:
        """
        Returns the metadata for variables (rows 3-6 in the worksheet).
        
        Returns:
            DataFrame: Variable metadata including units and descriptions
        """
        if self._variables is None:
            self.get_forecasters_data()
        return self._variables