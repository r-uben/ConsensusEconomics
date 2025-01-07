import calendar
import zipfile
import os 

from consensus_economics.utils.check_format import CheckFormatUtils
from consensus_economics.utils.path_format import PathFormatUtils

from tqdm import tqdm

from consensus_economics.paths import Paths



class Constructor():
    
    def __init__(self):
        super().__init__()
        self.raw_zip_path = Paths().raw / "zip"
        self.raw_xlsx_path = Paths().raw / "xlsx"

        self.processed_xlsx_path = Paths().processed / "xlsx"

    def extract_zip(self, filename: str):
        file_path = PathFormatUtils.get_file_path(self.raw_zip_path, filename)
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            zip_ref.extractall(self.raw_xlsx_path)
    
    def decompress_files(self):
        # Create xlsx directory if it doesn't exist
        os.makedirs(self.raw_xlsx_path, exist_ok=True)
        
        for filename in os.listdir(self.raw_zip_path):
            if CheckFormatUtils.iszip(filename): self.extract_zip(filename)
        print("All files have been decompressed.")
        self.rename_files()

    def correct_date_format(self, filename: str, extension: str) -> str: # this is completely ad hoc 
        # Move the year to the beginning of the filename
        parts = filename.split(extension)[0]  # Remove the extension for processing
        year = parts[-4:]  # Assuming the year is the last 4 characters before the extension
        new_filename = year + parts[:-4].replace('cf', '') + extension  # Reconstruct the filename with the year first and remove "cf"
        return new_filename

    def rename_files(self):
        for filename in tqdm(os.listdir(self.raw_xlsx_path)):
            if CheckFormatUtils.isxlsx(filename):
                old_file_path = PathFormatUtils.get_file_path(self.raw_xlsx_path, filename)
                new_filename = filename.lower()
                
                # Replace month names with numbers using calendar
                for i, month_name in enumerate(calendar.month_abbr[1:], 1):
                    new_filename = new_filename.replace(
                        month_name.lower(), 
                        f"{i:02d}"
                    )
   
                # Move the year to the beginning
                extension = '.xlsx'
                new_filename = self.correct_date_format(new_filename, extension)
                new_file_path = PathFormatUtils.get_file_path(self.processed_xlsx_path, new_filename)
                os.makedirs(os.path.dirname(new_file_path), exist_ok=True)
                os.replace(old_file_path, new_file_path)

        print("All files have been renamed.")



