from typing import List
from pandas import DataFrame
from consensus_economics.utils.date_format import DateFormatUtils
from consensus_economics.paths import Paths
from openpyxl import load_workbook

class BaseWorksheet:
    """
    Base class for handling consensus economics worksheet operations.
    
    Args:
        date (str): Date in format 'yyyymm'
        sheet_name (str): Sheet name in the workbook
    """

    def __init__(self, date: str, sheet_name: str) -> None:
        # Validate date
        if not isinstance(date, str):
            raise ValueError("Date must be a string")
        if not len(date) == 6:
            raise ValueError("Date must be a 6-digit string: yyyymm.")
        
        # Validate sheet_name
        if not isinstance(sheet_name, str) or not sheet_name.strip():
            raise ValueError("Sheet name must be a non-empty string")

        self._date = date
        self._year = int(date[:4])
        self._month = int(date[4:])
        self._sheet_name = sheet_name.strip()
        
        # Initialize cached properties
        self._workbook = None
        self._sheets = None
        self._worksheet = None
        self._column_names = None

    @property
    def sheet_name(self) -> str:
        """The sheet name in the workbook."""
        return self._sheet_name

    @property
    def date(self) -> str:
        """The date string in yyyymm format."""
        return self._date
    
    @property
    def year(self) -> int:
        """The year component of the date."""
        return self._year
    
    @property
    def month(self) -> int:
        """The month component of the date."""
        return self._month
    
    @property
    def workbook(self):
        """The loaded Excel workbook."""
        if self._workbook is None:
            self._workbook = self.get_workbook()
        return self._workbook
    
    @property
    def sheets(self) -> DataFrame:
        """DataFrame containing sheet names."""
        if self._sheets is None:
            self._sheets = self.get_sheets()
        return self._sheets
    
    @property
    def worksheet(self) -> DataFrame:
        """DataFrame containing the worksheet's data."""
        if self._worksheet is None:
            self._worksheet = self.get_worksheet()
        return self._worksheet

    def get_workbook(self) -> object:
        """Loads the Excel workbook from the file system."""
        filepath = f"{Paths().processed}/xlsx/{self.date}.xlsx"
        return load_workbook(filepath, data_only=True)

    def get_sheets(self) -> DataFrame:
        """Returns DataFrame of sheet names."""
        sheet_names = self.workbook.sheetnames
        return DataFrame(sheet_names, columns=["Sheet Names"])

    def get_worksheet(self) -> DataFrame:
        """Returns DataFrame of the worksheet's data."""
        worksheet = self.workbook[self.sheet_name]
        return DataFrame(worksheet.values)