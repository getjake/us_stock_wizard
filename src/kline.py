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
import asyncify
import yfinance as yf
from us_stock_wizard import StockRootDirectory
from us_stock_wizard.database.db_utils import StockDbUtils, DbTable
from us_stock_wizard.src.common import StockCommon


class KlineFetch:
    """
    Check stock split and dividend

    parallel 2 => bug, do not use it!
    """

    def __init__(self, parallel: int = 1) -> None:
        self.parallel = parallel
        self.tickers = []
        self.cache = pd.DataFrame()

    async def initialize(self) -> None:
        self.tickers = await self.get_all_tickers()

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
        tickers = sorted(tickers)
        if not tickers:
            logging.info("No tickers found")
            return []
        return tickers

    async def get_ticker(
        self, ticker: str, start: Optional[datetime] = None, cache: bool = False
    ) -> pd.DataFrame:
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

        if cache and not self.cache.empty and ticker in self.cache:
            data = self.cache[ticker]
            data = data[start:tomorrow]
            return data
        else:
            result: pd.DataFrame = await self.download_kline(
                ticker, start=start, end=tomorrow
            )
            return result

    @asyncify
    def download_kline(self, ticker, start, end) -> pd.DataFrame:
        return yf.download(ticker, start=start, end=end)

    def _check_split_dividend(self, ticker: str, start: str) -> bool:
        """
        Check if the ticker has split or dividend after the given date, no more than 90 days
        """
        try:
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
        except Exception as e:
            # Default to False
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

    async def handle_spx(self) -> None:
        """
        Download the SPX data
        """
        ticker = "^GSPC"
        _data = await self.get_ticker(ticker)
        # Remove the data in the database
        await StockDbUtils.delete(DbTable.DAILY_KLINE, {"ticker": ticker})

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
        _data["ticker"] = "SPX"  # Change to SPX
        _data["date"] = pd.to_datetime(_data["date"])
        data_list = _data.to_dict("records")
        await StockDbUtils.insert(DbTable.DAILY_KLINE, data_list)

    async def handle_ticker(self, ticker: str) -> None:
        """
        Handle a single ticker

        1. Check the last update date in the database
        2. Get the data from yfinance API
        3. Insert the data to database
        """
        today = datetime.today().strftime("%Y%m%d")
        last_update_date = await self._check_last_update_date(ticker)
        if last_update_date and today == last_update_date.strftime("%Y%m%d"):
            logging.warning(f"Ticker {ticker} alreadly updated kline. skipped.")
            return
        _data = await self.get_ticker(ticker, start=last_update_date, cache=True)
        if _data.empty:
            logging.error(f"No data found for {ticker}")
            return
        if last_update_date:
            _is_dividend_split = self._check_split_dividend(ticker, last_update_date)
            if _is_dividend_split:
                # Remove all data for this stock and download all!
                await StockDbUtils.delete(DbTable.DAILY_KLINE, {"ticker": ticker})
                _data = await self.get_ticker(ticker)

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

    async def update_all_tickers(self) -> None:
        """
        Update the tickers for all only after the trading day.
        """
        cal = await StockDbUtils.read(DbTable.TRADING_CALENDAR, output="df")
        cal["date"] = pd.to_datetime(cal["date"]).dt.date
        # check today in cal["date"]
        today = pd.Timestamp.today().date()
        is_today_trading = today in cal["date"].tolist()
        if not is_today_trading:
            logging.info("Today is not a trading day, skip")

        await self.handle_spx()
        await self.handle_all_tickers()

    async def get_updated_ticker(self, date: Optional[datetime] = None) -> List[str]:
        """
        Get the tickers that has been updated on a date
        """
        if not date:
            date = pd.Timestamp.today().normalize()
        res: pd.DataFrame = await StockDbUtils.read(
            DbTable.DAILY_KLINE, where={"date": date}, output="df"
        )
        if res.empty:
            return []
        return res["ticker"].tolist()

    def download_cache(self, tickers: List[str]) -> None:
        """
        Download the data for the given tickers and store them in cache
        """
        self.cache = yf.download(tickers, period="3mo", group_by="ticker", threads=True)

    async def handle_all_tickers(self) -> None:
        """
        Get all ticker data from yfinance API. And insert them to database.
        """
        count = 0
        updated_tickers = await self.get_updated_ticker()
        to_update_tickers = list(set(self.tickers) - set(updated_tickers))
        total = len(to_update_tickers)

        # Old way
        if self.parallel < 2:
            for ticker in to_update_tickers:
                count += 1
                logging.warning(f"Downloading {count} of {total} ticker: {ticker}")
                await self.handle_ticker(ticker)
        else:
            # New way Concurrently
            ticker_pairs = [
                to_update_tickers[n : n + self.parallel]
                for n in range(0, len(to_update_tickers), 2)
            ]  # Group tickers into pairs
            count = 0
            for pair in ticker_pairs:
                count += 1
                logging.warning(
                    f"Downloading pair {pair}: {count} / {len(ticker_pairs)}"
                )
                self.download_cache(pair)
                for _ticker in pair:
                    await self.handle_ticker(_ticker)
                # await asyncio.gather(
                #     *(self.handle_ticker(ticker) for ticker in pair)
                # )  # Run for each pair concurrently
                logging.warning(f"Done pair {pair}")
                await asyncio.sleep(
                    1
                )  # Sleep for 1 second to avoid being blocked by yfinance
