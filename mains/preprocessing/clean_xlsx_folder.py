"""Clean duplicate xlsx files from processed directory."""

import os
from pathlib import Path

from consensus_economics.paths import Paths


def main() -> None:
    """
    Remove duplicate xlsx files with numbering (e.g., "file 2.xlsx").

    When files are downloaded multiple times, they may have numbers appended.
    This script removes those duplicates, keeping the original filename.
    """
    processed_xlsx_path = Paths().processed / "xlsx"

    for root, _, files in os.walk(processed_xlsx_path):
        for filename in files:
            if not filename.endswith(".xlsx"):
                continue

            # Check for " N" pattern where N is a digit
            for i in range(1, 10):
                if f" {i}" not in filename:
                    continue

                old_path = Path(root) / filename
                new_filename = filename.replace(f" {i}", "")
                new_path = Path(root) / new_filename

                # Remove target if exists, then rename
                if new_path.exists():
                    new_path.unlink()

                old_path.rename(new_path)
                print(f"Renamed: {filename} -> {new_filename}")
                break


if __name__ == "__main__":
    main()
