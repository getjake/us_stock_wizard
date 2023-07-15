from typing import Optional
import logging
import pandas as pd
from database.db_utils import StockDbUtils, DbTable

logging.basicConfig(level=logging.INFO)


class TaAnalyzer:
    """ """

    def __init__(self, ticker: str) -> None:
        self.ticker = ticker
        self.db_utils = StockDbUtils()
        self.kline: Optional[pd.DataFrame] = None

    async def get_kline(self) -> None:
        """
        Get kline data from database
        """
        kline = await StockDbUtils.read(
            DbTable.DAILY_KLINE, where={"ticker": self.ticker}, output="df"
        )
        self.kline = kline
