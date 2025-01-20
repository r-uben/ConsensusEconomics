import pandas as pd
import re


class DateFormatUtils:

    @staticmethod
    def get_date(year, month):
        return f"{year}{month:02d}"

    @staticmethod
    def month_to_number(month):
        month_dict = {
            "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04",
            "May": "05", "Jun": "06", "Jul": "07", "Aug": "08",
            "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12",
            "January": "01", "February": "02", "March": "03", "April": "04",
            "May": "05", "June": "06", "July": "07", "August": "08",
            "September": "09", "October": "10", "November": "11", "December": "12"
        }
        return month_dict.get(month.strip(), "00")  # Return "00" if the month is not found
    
    @staticmethod
    def formatted_release_date(df: pd.DataFrame) -> str:
        ''' The input has to be forecasters_dataframe before cleaning'''
        try:
            # Safety check for empty DataFrame
            if df.empty or df.shape[1] < 2:
                return "Survey"

            # For older files where "Survey Date:" is in first column
            survey_mask = df.iloc[:, 0].astype(str).str.contains('Survey Date:', na=False)
            if survey_mask.any():
                row_idx = survey_mask.idxmax()
                if row_idx < df.shape[0] and df.shape[1] > 1:
                    date_str = str(df.iloc[row_idx, 1])
                    if pd.notna(date_str):
                        # Extract components using regex
                        pattern = r'(\w+)\s+(\d+),?\s+(\d{4})'
                        match = re.match(pattern, date_str)
                        if match:
                            month, day, year = match.groups()
                            month_num = DateFormatUtils.month_to_number(month)
                            if month_num != "00":
                                return f"{year}{month_num}{int(day):02d}"
            
            # If we couldn't get the date from Survey Date, try the column header
            if len(df.columns) > 0:
                header = str(df.columns[0])
                parts = header.split()
                if len(parts) >= 3:
                    try:
                        day = parts[-2].replace(",", "").zfill(2)
                        month_num = DateFormatUtils.month_to_number(parts[-3])
                        year = parts[-1]
                        if month_num != "00":
                            return f"{year}{month_num}{day}"
                    except:
                        pass
            
            # If all else fails, return "Survey"
            return "Survey"
            
        except Exception as e:
            print(f"Error formatting release date: {str(e)}")
            return "Survey"
