from typing import Optional, List
import logging
import pandas as pd
import datetime
from prisma import Json
from us_stock_wizard.database.db_utils import DbTable, StockDbUtils
from us_stock_wizard.src.common import StockCommon
from us_stock_wizard.screener.ta_analyzer import TaAnalyzer, TaMeasurements
from us_stock_wizard.screener.fundamental_analyzer import (
    FundamentalAnalyzer,
    FundamentalMeasurements,
)


class DailyScreener:
    """
    Run this daily screener after market close

    """

    def __init__(self) -> None:
        self.stocks: Optional[List[str]] = None
        self.succ_tickers: Optional[List[str]] = None
        self.rs_dict: Optional[dict] = {}

    async def initialize(self) -> None:
        """
        Initialize the screener
        """
        today = pd.Timestamp.today().normalize()
        relative_strength = await StockDbUtils.read(
            DbTable.RELATIVE_STRENGTH, where={"date": today}, output="df"
        )
        self.rs_dict = dict(
            zip(relative_strength["ticker"], relative_strength["rscore"])
        )
        self.stocks = await StockCommon.get_stock_list()

    async def screen_stock(self, ticker: str) -> bool:
        """
        Will not consider any fundamental. Only consider technical analysis.

        Commented out the fundamental analysis part.
        # funda = FundamentalAnalyzer(ticker=ticker)
        """
        try:
            ta = TaAnalyzer(ticker=ticker, rs=self.rs_dict.get(ticker))
            await ta.get_kline()

            # Choose the measurements to run
            ta_succ = ta.get_result([TaMeasurements.STAGE2])
            if ta_succ:
                return True
            else:
                return False
        except Exception as e:
            logging.error(f"Failed to screen stock {ticker}: {e}")
            return False

    async def screen_all(self) -> List[str]:
        """
        Screen all stocks and get the succ. tickers.
        """
        if not self.stocks:
            await self.initialize()

        succ_tickers = []
        for ticker in self.stocks:
            succ = await self.screen_stock(ticker)
            print(f"{ticker}: {succ}")
            if succ:
                succ_tickers.append(ticker)

        self.succ_tickers = succ_tickers

    async def save(self) -> None:
        """
        Save the succ. tickers to csv file
        """
        if not self.succ_tickers:
            await self.screen_all()

        _ = {
            "date": pd.to_datetime(datetime.date.today()),
            "kind": "DailyScreen",
            "data": Json(
                {
                    "desc": "TA10",
                    "tickers": self.succ_tickers,
                }
            ),
        }
        await StockDbUtils.insert(table=DbTable.REPORT, data=[_])
        logging.warning("Saved the succ. tickers to database")
