

class PathFormatUtils:

    @staticmethod
    def get_file_path(path: str, filename: str) -> str:
        return path / filename
    