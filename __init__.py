import os


class StockRootDirectory:
    @staticmethod
    def root_dir():
        """
        Get root directory of the project
        """
        return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
