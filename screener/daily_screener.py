from typing import Optional, List, Dict
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
        self.succ_tickers: Dict[str, List[str]] = {}  # 选股结果
        self.rs_dict: Optional[Dict[str, List[int]]] = {}

    async def initialize(self) -> None:
        """
        Initialize the screener
        """
        today = pd.Timestamp.today().normalize()
        relative_strength = await StockDbUtils.read(
            DbTable.RELATIVE_STRENGTH, where={"date": today}, output="df"
        )
        relative_strength["combined"] = relative_strength[
            ["rscore", "M1, M3", "M6"]
        ].values.tolist

        # Relative Strength

        if relative_strength.empty:
            raise ValueError("No relative strength data found!")
        self.rs_dict = dict(
            zip(relative_strength["ticker"], relative_strength["combined"])
        )
        self.stocks = await StockCommon.get_stock_list()

    async def screen_stock(self, ticker: str) -> Dict[str, bool]:
        """
        SS#1:
        Will not consider any fundamental. Only consider technical analysis.

        Commented out the fundamental analysis part.
        # funda = FundamentalAnalyzer(ticker=ticker)
        """
        try:
            ta = TaAnalyzer(ticker=ticker, rs=self.rs_dict.get(ticker))
            await ta.get_kline()

            # Choose the measurements to run
            succ_dict = ta.get_result()
            return succ_dict
        except Exception as e:
            logging.error(f"Failed to screen stock {ticker}: {e}")
            return {}

    async def screen_all(self) -> None:
        """
        Screen all stocks and get the succ. tickers.
        """
        if not self.stocks:
            await self.initialize()

        summary = pd.DataFrame()
        for ticker in self.stocks:
            _ = {"ticker": ticker}
            _result = await self.screen_stock(ticker)
            if not _result:
                continue
            _.update(_result)
            _df = pd.DataFrame([_])
            summary = pd.concat([summary, _df], ignore_index=True)

        # Handle summary
        summary.set_index("ticker", inplace=True)
        cret: List[str] = summary.columns.tolist()
        for kind in cret:
            ticker_list = summary[summary[kind] == True].index.tolist()
            self.succ_tickers[kind] = ticker_list

    async def save(self) -> None:
        """
        Save all the succ. tickers to csv file
        """
        if not self.succ_tickers:
            logging.warning("No succ. tickers found, skip saving")
            return

        for kind, tickers in self.succ_tickers.items():
            _ = {
                "date": pd.to_datetime(datetime.date.today()),
                "kind": kind,
                "data": Json(tickers),
            }
            await StockDbUtils.insert(table=DbTable.REPORT, data=[_])
        logging.warning("Saved the succ. tickers to database")
