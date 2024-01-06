"""
Get the candlestick (kline) data  from yfinance API.
The API Key shall be stored in the .env file.
"""
import json
import asyncio
import logging
from datetime import datetime
from time import sleep
from typing import List, Optional, Set, Tuple
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
    """

    def __init__(self, parallel: int = 5) -> None:
        self.parallel = parallel
        self.tickers: List[str] = []
        self.simple_tickers: Set[str] = ()  # No dividend or split
        self.delisted_tickers: Set[str] = set()
        self.cache = pd.DataFrame()
        self.error_tickers = set()

    async def initialize(self) -> None:
        self.tickers = await self.get_all_tickers()
        self.simple_tickers = await self.get_simple_tickers()

    async def get_all_tickers(self) -> List[str]:
        """
        Only get the tickers that need to be updated
        """
        tickers_df = await StockCommon.get_stock_list({"delisted": False}, format="df")
        tickers = tickers_df["ticker"].tolist()
        tickers = sorted(tickers)
        tickers = [x for x in tickers if "/" not in x]
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
        default = (datetime.today() - pd.DateOffset(days=365 * 6)).strftime("%Y-%m-%d")
        if not start:
            start = default
        if not isinstance(start, str):
            start = pd.to_datetime(start).strftime("%Y-%m-%d")

        if cache and not self.cache.empty and ticker in self.cache:
            data: pd.DataFrame = self.cache[ticker]
            data = data[start:tomorrow]
            print(f"Ticker {ticker} using cache.")
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
        _data["volume"] = _data["volume"].astype(int)
        _data["ticker"] = "SPX"  # Change to SPX
        _data["date"] = pd.to_datetime(_data["date"])
        data_list = _data.to_dict("records")
        await StockDbUtils.insert(DbTable.DAILY_KLINE, data_list)

    async def handle_simple_ticker(self, ticker: str) -> bool:
        """
        Handle Simpel ticker that has no dividend or split, recently.
        """
        try:
            today = datetime.today().strftime("%Y-%m-%d")
            _data = await self.get_ticker(ticker, start=today, cache=True)
            if _data.empty or _data.shape[0] > 1:
                return False
            _ = {
                "ticker": ticker,
                "date": _data.index[0],
                "open": _data.iloc[0]["Open"],
                "high": _data.iloc[0]["High"],
                "low": _data.iloc[0]["Low"],
                "close": _data.iloc[0]["Close"],
                "adjClose": _data.iloc[0]["Adj Close"],
                "volume": int(_data.iloc[0]["Volume"]),
            }
            await StockDbUtils.insert(DbTable.DAILY_KLINE, [_])
            return True
        except Exception as e:
            logging.error(f"Error for {ticker}: {e}")
            return False

    async def handle_ticker(self, ticker: str) -> None:
        """
        Handle a single ticker

        1. Check the last update date in the database
        2. Get the data from yfinance API
        3. Insert the data to database
        """
        if ticker in self.delisted_tickers:
            logging.warning(f"{ticker} is delisted. skipped.")
            return
        today = datetime.today().strftime("%Y%m%d")
        # Handle Simple Tickers
        if ticker in self.simple_tickers:
            # Only update today's data
            succ = await self.handle_simple_ticker(ticker)
            if succ:
                return
        last_update_date = await self._check_last_update_date(ticker)
        if last_update_date and today == last_update_date.strftime("%Y%m%d"):
            logging.warning(f"Ticker {ticker} alreadly updated kline. skipped.")
            return
        try:
            _data = await self.get_ticker(ticker, start=last_update_date, cache=True)
        except Exception as e:
            logging.error(f"Ticker {ticker} Download Error.")
            self.error_tickers.add(ticker)
            return

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
        _data["volume"] = _data["volume"].astype(int)
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
        result = res["ticker"].tolist()
        lst = [x for x in result if "/" not in x]
        return lst

    def download_cache(self, tickers: List[str], retries: int = 3) -> None:
        """
        Download the data for the given tickers and store them in cache
        """
        while retries > 0:
            self.cache = yf.download(
                tickers, period="1mo", group_by="ticker", threads=True
            )
            if not self.cache.empty:
                break
            retries -= 1
            sleep(1)
        if self.cache.empty:
            logging.error(f"Download failed for {tickers}, retries: {retries}")
            return

        # Check NaN value
        for ticker in tickers:
            if self.cache[ticker].isnull().values.any():
                logging.warning(f"NaN value found for {ticker}")
                self.delisted_tickers.add(ticker)

    async def get_simple_tickers(self) -> List[str]:
        """
        Get tickers that has no split or dividend in the last two trading days.
        """
        res: pd.DataFrame = await StockDbUtils.read(
            DbTable.TICKERS, where={"recentSplitDivend": False}, output="df"
        )
        _ = res.ticker.tolist()
        # Remove tickers that has / in it
        _ = [x for x in _ if "/" not in x]
        return set(_)

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
            ticker_pairs = StockCommon.split_list(to_update_tickers, self.parallel)
            count = 0
            print("All tickers:", len(to_update_tickers))
            print("All Groups:", len(ticker_pairs))
            for pair in ticker_pairs:
                count += 1
                try:
                    logging.warning(
                        f"Downloading pair {pair}: {count} / {len(ticker_pairs)}"
                    )
                    self.download_cache(pair)
                    await asyncio.gather(
                        *(self.handle_ticker(ticker) for ticker in pair)
                    )
                    logging.warning(f"Done for {pair}")

                    await asyncio.sleep(0.1)
                except Exception as e:
                    logging.error(f"Error in {pair}: {e}")
                    continue

    async def get_abnormal_tickers(self, days_ago: int = 10) -> List[str]:
        """
        Get those tickers which has the missing values of OHLC on any dates.

        Args:
        - days_ago: The number of trading days to check.
        """

        all_tickers = await self.get_all_tickers()
        all_tickers = set(all_tickers)
        today = pd.Timestamp.today()
        _ = await StockDbUtils.read(
            DbTable.TRADING_CALENDAR, where={"date": {"lte": today}}, output="df"
        )
        days_to_check: List[pd.Timestamp] = (
            _.sort_values("date", ascending=False).head(days_ago)["date"].to_list()
        )

        abnormal_tickers: Set[str] = set()
        for day in days_to_check:
            _data = await StockDbUtils.read(
                DbTable.DAILY_KLINE, where={"date": day}, output="df"
            )
            existed_tickers: Set[str] = set(_data["ticker"].unique().tolist())
            abnormal_tickers = abnormal_tickers.union(all_tickers - existed_tickers)

        return sorted(list(abnormal_tickers))

    async def refetch_ticker(self, ticker: str) -> bool:
        """
        Refetch Ticker and insert to database
        """
        _data = await self.get_ticker(ticker, cache=False)
        if _data.empty:
            logging.error(f"No data found for {ticker}")
            return False
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
        _data["volume"] = _data["volume"].astype(int)
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
            return False

        data_list = _data.to_dict("records")
        # Delete old records first
        await StockDbUtils.delete(DbTable.DAILY_KLINE, {"ticker": ticker})
        await StockDbUtils.insert(DbTable.DAILY_KLINE, data_list)
        return True

    async def refetch_abnormal_tickers(self) -> Tuple[int, int]:
        """
        Refetch the abnormal tickers
        """

        abnormal_tickers = await self.get_abnormal_tickers()
        if not abnormal_tickers:
            logging.warning("No abnormal tickers found. So skip.")
            return True

        # Refetch the abnormal tickers
        abnormal_tickers = sorted(list(abnormal_tickers))
        logging.warning(
            f"Abnormal tickers: {len(abnormal_tickers)}, Going to re-fetch them."
        )
        count = 0
        succ_count = 0
        fail_count = 0
        for ticker in abnormal_tickers:
            count += 1
            logging.warning(
                f"Refetching the {count} / {len(abnormal_tickers)} - {ticker}"
            )
            _ = await self.refetch_ticker(ticker)
            if _:  # Success
                succ_count += 1
                logging.warning(f"Refetching {ticker} Succ!.")
            else:
                fail_count += 1
                logging.warning(f"Refetching {ticker} Fail!.")

        logging.warning(f"Refetching done. Success: {succ_count}, Fail: {fail_count}")
        return succ_count, fail_count
