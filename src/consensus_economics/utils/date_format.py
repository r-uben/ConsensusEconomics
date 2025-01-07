import pandas as pd


class DateFormatUtils:


    @staticmethod
    def get_date(year, month):
        return f"{year}{month:02d}"

    @staticmethod
    def month_to_number(month):
        month_dict = {
            "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04",
            "May": "05", "Jun": "06", "Jul": "07", "Aug": "08",
            "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12"
        }
        return month_dict.get(month, "00")  # Return "00" if the month is not found
    

    @staticmethod
    def formatted_release_date(df: pd.DataFrame) -> str:
        
        ''' The input has to be forecasters_dataframe before cleaning'''
        release_date = df.columns[0].split()[2:]
        try:
            release_date = release_date[2] + DateFormatUtils.month_to_number(release_date[0]) + release_date[1]
            release_date = release_date.replace(",", "")
        except:
            release_date = release_date[0]  
        return release_date
