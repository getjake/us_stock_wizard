import logging
import asyncio
from enum import Enum
from typing import List, Optional
from dotenv import load_dotenv
import httpx
import yfinance as yf
import pandas as pd
import pandas_market_calendars as mcal
from us_stock_wizard.database.db_utils import StockDbUtils, DbTable


class StockMarket:
    NASDAQ = "NASDAQ"
    NYSE = "NYSE"


class StockCommon:
    """
    Stock common utils
    """

    @staticmethod
    async def get_stock_list(conditions: dict = {}) -> List[str]:
        """
        Get list of stocks from the database, based on the given conditions
        """
        stocks = await StockDbUtils.read(table="Tickers", where=conditions)
        _stocks = [stock.dict() for stock in stocks]
        stock_df = pd.DataFrame(_stocks)
        stock_df = stock_df[~stock_df["ticker"].str.contains("\^")]
        return stock_df["ticker"].tolist()


class StockTickers:
    """
    Get the list of tickers from Nasdaq and NYSE

    Usage:
        from us_stock_wizard.stocks import StockTickers
        st = StockTickers()
        await st.handle_all_tickers()
    """

    NASDAQ_URL = "https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=25&exchange=NASDAQ&download=true"
    NYSE_URL = "https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=25&exchange=NYSE&download=true"
    HEADERS = {
        "authority": "api.nasdaq.com",
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "accept-language": "en-US,en;q=0.9",
        "cache-control": "max-age=0",
        "dnt": "1",
        "sec-ch-ua": '"Not.A/Brand";v="8", "Chromium";v="114", "Google Chrome";v="114"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "none",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    }

    def __init__(self) -> None:
        self._nasdaq_tickers: Optional[pd.DataFrame] = None
        self._nyse_tickers: Optional[pd.DataFrame] = None

    def get_tickers(self, market: StockMarket) -> pd.DataFrame:
        if market == StockMarket.NASDAQ:
            url = self.NASDAQ_URL
        elif market == StockMarket.NYSE:
            url = self.NYSE_URL
        else:
            raise Exception("Invalid market")

        resp = httpx.get(url, headers=self.HEADERS)
        if resp.status_code != 200:
            raise Exception(f"Error getting tickers for market: {market}")
        data_headers = resp.json()["data"]["headers"]
        data_rows = resp.json()["data"]["rows"]
        df = pd.DataFrame(data_rows, columns=data_headers)
        return df

    def get_all_tickers(self) -> None:
        # Nasdaq
        self._nasdaq_tickers = self.get_tickers(market=StockMarket.NASDAQ)
        # NYSE
        self._nyse_tickers = self.get_tickers(market=StockMarket.NYSE)

    async def _handle_tickers(self, market: StockMarket) -> None:
        """
        Handle tickers for a given market, save it to database
        """
        if market == StockMarket.NASDAQ:
            data = self._nasdaq_tickers
        elif market == StockMarket.NYSE:
            data = self._nyse_tickers
        if data == None:
            raise Exception(f"{market} tickers not loaded")
        _data = data.copy()
        columns = {
            "symbol": "ticker",
            "name": "name",
            "market": "market",
            "ipoyear": "ipoYear",
            "sector": "sector",
            "industry": "industry",
        }

        _data.rename(columns=columns, inplace=True)
        _data["ipoYear"] = _data["ipoYear"].apply(lambda x: int(x) if x else -1)
        _data["market"] = market

        _data = _data[columns.values()]
        all_results = _data.to_dict(orient="records")
        # insert into database
        await StockDbUtils.insert(table=DbTable.TICKERS, data=all_results)
        logging.info(f"Done for {market}")

    async def handle_all_tickers(self) -> None:
        self.get_all_tickers()
        await self._handle_tickers(market=StockMarket.NASDAQ)
        await self._handle_tickers(market=StockMarket.NYSE)


class TradingCalendar:
    """
    Get the trading calendar for the next 90 days

    Usage:

        from us_stock_wizard.stocks import TradingCalendar
        tc = TradingCalendar()
        await tc.handle_calendar()
    """

    def __init__(self) -> None:
        self.trading_calendar: Optional[List[pd.Timestamp]] = None
        self.nyse = mcal.get_calendar(StockMarket.NYSE)

    def get_calendar(self, days_forward: int = 90) -> List[pd.Timestamp]:
        today = pd.Timestamp.today().strftime("%Y-%m-%d")
        later = (pd.Timestamp.today() + pd.Timedelta(days=days_forward)).strftime(
            "%Y-%m-%d"
        )

        result: List[pd.Timestamp] = self.nyse.schedule(
            start_date=today, end_date=later
        ).index.tolist()
        return result

    async def handle_calendar(self) -> None:
        """
        Handle trading calendar, save it to database
        """
        self.trading_calendar = self.get_calendar()
        data = [{"date": date} for date in self.trading_calendar]
        await StockDbUtils.insert(table=DbTable.TRADING_CALENDAR, data=data)
        logging.info("Done")
