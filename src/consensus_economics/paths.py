from pathlib import Path


class Paths:
    def __init__(self):
        self.__data    = None

    def select_data(self) -> Path:
        path = Path("data")
        while not path.exists():
            path = Path("..") / path
            if path.is_absolute():
                raise FileNotFoundError("Could not find 'data' directory")
        return path
    def select_figures(self) -> Path:
        path = Path("figures")
        if path.exists():
            return path
        else:
            return Path("..") / path
        
    @property
    def raw(self):
        return self.data / "raw"

    @property
    def processed(self):
        return self.data / "processed"

    @property
    def data(self):
        if self.__data is None:
            self.__data = self.select_data()
        return self.__data
    
    @property
    def factset(self):
        return self.data / 'factset'

    @property
    def oecd(self):
        return self.data / 'oecd'
    
    @property
    def consensus(self):
        return self.data / 'consensus'
    