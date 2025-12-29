"""File format checking utilities."""


class CheckFormatUtils:
    """Utility class for checking file formats."""

    @staticmethod
    def iszip(filename: str) -> bool:
        """Check if filename has a zip extension."""
        return filename.lower().endswith(".zip")

    @staticmethod
    def isxlsx(filename: str) -> bool:
        """Check if filename has an xlsx extension."""
        return filename.lower().endswith(".xlsx")
