import os
from dotenv import load_dotenv


class StockRootDirectory:
    @staticmethod
    def root_dir() -> str:
        """
        Get root directory of the project
        """
        return os.path.dirname(os.path.abspath(__file__))

    @staticmethod
    def env() -> dict:
        """
        Get environment variables from .env file
        """
        root = StockRootDirectory.root_dir()
        env_path = os.path.join(root, ".env")
        if not os.path.exists(env_path):
            raise Exception("Please create .env file in the root directory.")
        load_dotenv(env_path)
        return os.environ
