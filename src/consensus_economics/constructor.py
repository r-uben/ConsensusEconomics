"""File processing utilities for Consensus Economics Excel files."""

import calendar
import os
import shutil
import zipfile

from tqdm import tqdm

from consensus_economics.paths import Paths
from consensus_economics.utils.check_format import CheckFormatUtils


class FileProcessor:
    """
    Handles decompression and renaming of Consensus Economics data files.

    Workflow:
        1. Read zip files from external storage (raw_zip)
        2. Extract xlsx to external storage (raw_xlsx)
        3. Rename and copy to local working directory (xlsx)
    """

    def __init__(self) -> None:
        self.paths = Paths()

    def extract_zip(self, filename: str) -> None:
        """Extract a single zip file to the raw xlsx directory."""
        file_path = self.paths.raw_zip / filename
        with zipfile.ZipFile(file_path, "r") as zip_ref:
            zip_ref.extractall(self.paths.raw_xlsx)

    def decompress_files(self) -> None:
        """
        Decompress all zip files from external storage.

        Requires external volume to be mounted.
        """
        if not self.paths.external_available:
            raise RuntimeError(
                f"External storage not available: {self.paths.external}\n"
                "Please mount the volume and try again."
            )

        os.makedirs(self.paths.raw_xlsx, exist_ok=True)

        zip_files = [f for f in os.listdir(self.paths.raw_zip) if CheckFormatUtils.iszip(f)]

        if not zip_files:
            print(f"No zip files found in {self.paths.raw_zip}")
            return

        for filename in tqdm(zip_files, desc="Extracting"):
            self.extract_zip(filename)

        print(f"Extracted {len(zip_files)} files.")
        self.rename_files()

    @staticmethod
    def _correct_date_format(filename: str, extension: str) -> str:
        """
        Reformat filename to have year at the beginning.

        Args:
            filename: Original filename (e.g., "cfjan2024.xlsx")
            extension: File extension (e.g., ".xlsx")

        Returns:
            Reformatted filename (e.g., "202401.xlsx")
        """
        parts = filename.split(extension)[0]
        year = parts[-4:]  # Year is last 4 characters
        new_filename = year + parts[:-4].replace("cf", "") + extension
        return new_filename

    @classmethod
    def format_filename(cls, filename: str) -> str:
        """
        Normalize a raw download name to YYYYMM.xlsx.

        Args:
            filename: Original filename (e.g., "cfJan2024.xlsx")

        Returns:
            Normalized filename (e.g., "202401.xlsx")
        """
        new_filename = filename.lower()
        for i, month_name in enumerate(calendar.month_abbr[1:], 1):
            new_filename = new_filename.replace(month_name.lower(), f"{i:02d}")
        return cls._correct_date_format(new_filename, ".xlsx")

    def rename_files(self) -> None:
        """
        Rename xlsx files to YYYYMM format and copy to local working directory.

        Reads from external raw_xlsx, writes to local xlsx directory.
        """
        os.makedirs(self.paths.xlsx, exist_ok=True)

        xlsx_files = [f for f in os.listdir(self.paths.raw_xlsx) if CheckFormatUtils.isxlsx(f)]

        if not xlsx_files:
            print(f"No xlsx files found in {self.paths.raw_xlsx}")
            return

        for filename in tqdm(xlsx_files, desc="Renaming"):
            old_file_path = self.paths.raw_xlsx / filename
            new_file_path = self.paths.xlsx / self.format_filename(filename)

            # Copy to local (don't delete from external)
            shutil.copy2(old_file_path, new_file_path)

        print(f"Renamed {len(xlsx_files)} files to {self.paths.xlsx}")


# Backwards compatibility alias
Constructor = FileProcessor
