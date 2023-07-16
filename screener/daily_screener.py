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

    async def initialize(self) -> None:
        """
        Initialize the screener
        """
        self.stocks = await StockCommon.get_stock_list()

    async def screen_stock(self, ticker: str) -> bool:
        try:
            ta = TaAnalyzer(ticker=ticker)
            funda = FundamentalAnalyzer(ticker=ticker)
            await ta.get_kline()
            await funda.get_fundamental()

            # Choose the measurements to run
            ta_succ = ta.get_result([TaMeasurements.STAGE2])
            funda_succ = funda.get_result([FundamentalMeasurements.CRITERIA_1])
            if ta_succ and funda_succ:
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
                    "desc": "Funda+TA",
                    "tickers": self.succ_tickers,
                }
            ),
        }
        await StockDbUtils.insert(table=DbTable.REPORT, data=[_])
        logging.warning("Saved the succ. tickers to database")
