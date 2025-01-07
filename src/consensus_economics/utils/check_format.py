class CheckFormatUtils:

    @staticmethod
    def iszip(filename: str) -> bool:
        return filename.endswith('.zip') or filename.endswith('.ZIP')
    
    @staticmethod
    def isxlsx(filename: str) -> bool:
        return filename.endswith('.xlsx')
    
    @staticmethod
    def month_to_number(month):
        month_dict = {
            "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04",
            "May": "05", "Jun": "06", "Jul": "07", "Aug": "08",
            "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12"
        }
        return month_dict.get(month, "00")  # Return "00" if the month is not found
    
    