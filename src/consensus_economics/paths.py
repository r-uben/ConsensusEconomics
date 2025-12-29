"""Path management for Consensus Economics data pipeline."""

from pathlib import Path

from consensus_economics.config import EXTERNAL_STORAGE


class Paths:
    """
    Manages paths for data directories.

    Storage layout:
        External (raw downloads):
            /Volumes/Main/Library/Databases/consensus_economics/
            ├── zip/      # Original zip downloads
            └── xlsx/     # Extracted raw Excel files

        Local (working files):
            <repo>/data/
            ├── xlsx/     # Renamed YYYYMM.xlsx files
            └── output/   # Final CSVs
                ├── 2024/forecasters/
                └── 2024/forex/
    """

    def __init__(self) -> None:
        self._data: Path | None = None
        self._external: Path = EXTERNAL_STORAGE

    def _find_data_dir(self) -> Path:
        """Find the data directory by walking up from current directory."""
        path = Path("data")
        attempts = 0
        max_attempts = 10

        while not path.exists() and attempts < max_attempts:
            path = Path("..") / path
            attempts += 1

            if path.is_absolute():
                raise FileNotFoundError("Could not find 'data' directory")

        if not path.exists():
            raise FileNotFoundError("Could not find 'data' directory")

        return path.resolve()

    # -------------------------------------------------------------------------
    # Local paths (in repo)
    # -------------------------------------------------------------------------

    @property
    def data(self) -> Path:
        """Root data directory (local)."""
        if self._data is None:
            self._data = self._find_data_dir()
        return self._data

    @property
    def xlsx(self) -> Path:
        """Renamed xlsx files directory (local working files)."""
        return self.data / "xlsx"

    @property
    def output(self) -> Path:
        """Output directory for final CSVs."""
        return self.data / "output"

    # -------------------------------------------------------------------------
    # External paths (on mounted volume)
    # -------------------------------------------------------------------------

    @property
    def external(self) -> Path:
        """External storage root."""
        return self._external

    @property
    def external_available(self) -> bool:
        """Check if external storage is mounted."""
        return self._external.exists()

    @property
    def raw_zip(self) -> Path:
        """Raw zip downloads (external)."""
        return self._external / "zip"

    @property
    def raw_xlsx(self) -> Path:
        """Raw extracted xlsx files (external)."""
        return self._external / "xlsx"

    # -------------------------------------------------------------------------
    # Backwards compatibility
    # -------------------------------------------------------------------------

    @property
    def raw(self) -> Path:
        """Deprecated: use raw_zip or raw_xlsx instead."""
        return self._external

    @property
    def processed(self) -> Path:
        """Deprecated: use xlsx or output instead."""
        return self.data
