"""Decompress Consensus Economics zip files."""

from consensus_economics.constructor import FileProcessor


def main() -> None:
    """Decompress all zip files and rename to standardized format."""
    FileProcessor().decompress_files()


if __name__ == "__main__":
    main()
