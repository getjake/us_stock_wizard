"""
Screen the IPOs that meet the criteria
"""
import asyncio
from typing import List
import logging
import datetime
from prisma import Json
import pandas as pd
from us_stock_wizard.database.db_utils import StockDbUtils, DbTable


class IpoScreener:
    """
    IPO Screener


    Example:
        >>> screener = IpoScreener()
        >>> await screener.initialize()
        >>> await screener.screen_all()
        >>> await screener.save()
    """

    def __init__(self) -> None:
        self.stocks_df = pd.DataFrame()
        self.succ_tickers: List[str] = []

    async def initialize(self):
        await self.get_stocks()

    async def get_stocks(self) -> None:
        """
        Get all stocks that IPOed in the last _ year
        Choose tickers len < 5!
        """
        all_stocks = await StockDbUtils.read(table=DbTable.TICKERS, output="df")
        curr_year = datetime.datetime.now().year
        year_min = curr_year - 1
        all_stocks = all_stocks[all_stocks["ipoYear"] >= year_min]
        all_stocks = all_stocks[all_stocks["ticker"].str.len() < 5]
        self.stocks_df = all_stocks

    async def get_kline(self, ticker: str) -> pd.DataFrame:
        """
        Get the kline of a stock
        """
        kline = await StockDbUtils.read(
            DbTable.DAILY_KLINE, where={"ticker": ticker}, output="df"
        )
        if kline.empty:
            return pd.DataFrame()
        kline["date"] = pd.to_datetime(kline["date"]).dt.date
        return kline

    async def screen(self, ticker: str) -> bool:
        """
        Screen a spec stock
        0. IPOed less than 250 days.
        1. Latest Price >= 5
        2. SMA 5 >= SMA 20
        """
        # Check if exists
        _ = self.stocks_df[self.stocks_df["ticker"] == ticker]
        if _.empty:
            return False
        kline = await self.get_kline(ticker=ticker)
        if kline.empty:
            return False

        kline["ma5"] = kline["adjClose"].rolling(5).mean()
        kline["ma20"] = kline["adjClose"].rolling(20).mean()

        latest = kline.iloc[-1]
        days = kline.shape[0]
        c_0 = days <= 250

        # 1. Latest Price >= 5
        c_1 = latest["adjClose"] >= 5

        # 2. MA5 >= MA20
        c_2 = latest["ma5"] >= latest["ma20"]
        return c_0 and c_1 and c_2

    async def screen_all(self) -> List[str]:
        """
        Screen all stocks
        """
        succ_tickers = []
        for ticker in self.stocks_df["ticker"]:
            _ = await self.screen(ticker=ticker)
            logging.warning(f"Screening >>> {ticker} <<< Now!")
            if _:
                succ_tickers.append(ticker)
                logging.warning(f"{ticker} passed")
            else:
                logging.warning(f"{ticker} does not pass")
        self.succ_tickers = succ_tickers
        return succ_tickers

    async def save(self) -> None:
        """
        Save all the succ. tickers to csv file
        """
        if not self.succ_tickers:
            logging.warning("No succ. tickers found, skip saving")
            return
            
        _ = {
            "date": pd.to_datetime(datetime.date.today()),
            "kind": "ipo",
            "data": Json(self.succ_tickers),
        }
        await StockDbUtils.insert(table=DbTable.REPORT, data=[_])
        logging.warning("Saved the succ. tickers to database")


async def main():
    screener = IpoScreener()
    await screener.initialize()
    await screener.screen_all()
    await screener.save()


if __name__ == "__main__":
    asyncio.run(main())
