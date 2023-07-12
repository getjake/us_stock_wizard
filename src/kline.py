"""
Get the candlestick (kline) data  from yfinance API.
The API Key shall be stored in the .env file.
"""
import asyncio
import logging
from datetime import datetime
from time import sleep
from typing import List, Optional
import httpx
import pandas as pd
import yfinance as yf
from us_stock_wizard import StockRootDirectory
from us_stock_wizard.database.db_utils import StockDbUtils, DbTable
from us_stock_wizard.src.common import StockCommon


class KlineFetch:
    """
    Check stock split and dividend
    """

    def __init__(self) -> None:
        self.tickers = []

    async def initialize(self) -> None:
        await self.get_all_tickers()

    async def get_all_tickers(self) -> List[str]:
        """
        Only get the tickers that need to be updated
        """
        tickers_df = await StockCommon.get_stock_list({"delisted": False}, format="df")
        tickers_df["fundamentalsUpdatedAt"] = pd.to_datetime(
            tickers_df["fundamentalsUpdatedAt"]
        ).dt.date
        df_to_update = tickers_df[
            tickers_df["fundamentalsUpdatedAt"] < datetime.today().date()
        ]
        df_null = tickers_df[tickers_df["fundamentalsUpdatedAt"].isna()]
        df_joint = pd.concat([df_null, df_to_update])
        tickers = df_joint["ticker"].tolist()

        if not tickers:
            logging.info("No tickers found")
            return []
        self.tickers = tickers

    def get_ticker(self, ticker: str, start: Optional[datetime] = None) -> pd.DataFrame:
        """
        Get ticker data from yfinance API.
        Please get the daily data after market close.

        Args:
            ticker: The ticker symbol
            start: The start date of the data
        """
        tomorrow = (datetime.today() + pd.DateOffset(days=1)).strftime("%Y-%m-%d")
        default = (datetime.today() - pd.DateOffset(days=252 * 3)).strftime("%Y-%m-%d")
        if not start:
            start = default
        else:
            start = pd.to_datetime(start).strftime("%Y-%m-%d")
        result: pd.DataFrame = yf.download(ticker, start=start, end=tomorrow)
        return result

    def _check_split_dividend(self, ticker: str, start: str) -> bool:
        """
        Check if the ticker has split or dividend after the given date, no more than 90 days
        """
        _start = pd.to_datetime(start)
        if (datetime.today() - _start).days > 90:
            logging.info(f"More than 90 days for {ticker}")
            return False
        _start = pd.to_datetime(start).tz_localize("America/New_York")
        stock = yf.Ticker(ticker)
        _data = stock.history(period="3mo")
        _data.reset_index(inplace=True)
        # filter `Date` is larger than `start`
        _data["Date"] = pd.to_datetime(_data["Date"])
        _data = _data[_data["Date"] >= _start]
        total_split = _data["Stock Splits"].sum()
        total_dividend = _data["Dividends"].sum()
        if total_dividend != 0 or total_split != 0:
            logging.info(f"{ticker} has split or dividend")
            return True
        return False

    async def _check_last_update_date(self, ticker: str) -> Optional[datetime]:
        """
        Check the last update date of the given ticker
        """
        res = await StockDbUtils.read(DbTable.DAILY_KLINE, where={"ticker": ticker})
        if not res:
            return None
        data = StockCommon.convert_to_dataframe(res)
        data.sort_values(by=["date"], ascending=True, inplace=True)
        last_date = data.tail(1)["date"].values[0]
        return pd.to_datetime(last_date)

    async def handle_ticker(self, ticker: str) -> None:
        """
        Handle a single ticker

        1. Check the last update date in the database
        2. Get the data from yfinance API
        3. Insert the data to database
        """
        last_update_date = await self._check_last_update_date(ticker)
        _data = self.get_ticker(ticker, start=last_update_date)
        if _data.empty:
            logging.error(f"No data found for {ticker}")
            return
        if last_update_date:
            _is_dividend_split = self._check_split_dividend(ticker, last_update_date)
            if _is_dividend_split:
                # Remove all data for this stock and download all!
                await StockDbUtils.delete(DbTable.DAILY_KLINE, {"ticker": ticker})
                _data = self.get_ticker(ticker)

        _data.reset_index(inplace=True)
        _data = _data.rename(
            columns={
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Adj Close": "adjClose",
                "Volume": "volume",
                "Date": "date",
            }
        )
        _data["ticker"] = ticker
        _data["date"] = pd.to_datetime(_data["date"])

        # Check delisted
        last_date = _data.iloc[-1]["date"]
        today = datetime.today()
        if today - last_date > pd.Timedelta(days=15):  # delisted
            logging.info(f"{ticker} is delisted?! Please check.")
            await StockDbUtils.update(
                DbTable.TICKERS, {"ticker": ticker}, {"delisted": True}
            )
            return

        data_list = _data.to_dict("records")
        await StockDbUtils.insert(DbTable.DAILY_KLINE, data_list)

        # Update the ticker table
        await StockDbUtils.update(
            DbTable.TICKERS, {"ticker": ticker}, {"klineUpdatedAt": datetime.today()}
        )

        return

    async def handle_all_tickers(self) -> None:
        """
        Get all ticker data from yfinance API. And insert them to database.
        """
        for ticker in self.tickers:
            await self.handle_ticker(ticker)
            sleep(2)
