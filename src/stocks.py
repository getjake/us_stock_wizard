import logging
import asyncio
from typing import List, Optional
import httpx
import yfinance as yf
import pandas as pd
import pandas_market_calendars as mcal
from us_stock_wizard.database.db_utils import StockDbUtils, DbTable


class StockMarket:
    NASDAQ = "NASDAQ"
    NYSE = "NYSE"
    AMEX = "AMEX"  # aka. NYSE ARCA


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
    AMEX_URL = "https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=25&exchange=AMEX&download=true"

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
        self._amex_tickers: Optional[pd.DataFrame] = None

    def get_tickers(self, market: StockMarket) -> pd.DataFrame:
        if market == StockMarket.NASDAQ:
            url = self.NASDAQ_URL
        elif market == StockMarket.NYSE:
            url = self.NYSE_URL
        elif market == StockMarket.AMEX:
            url = self.AMEX_URL
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
        # AMEX
        self._amex_tickers = self.get_tickers(market=StockMarket.AMEX)

    async def _handle_tickers(self, market: StockMarket) -> None:
        """
        Handle tickers for a given market, save it to database
        """
        if market == StockMarket.NASDAQ:
            data = self._nasdaq_tickers
        elif market == StockMarket.NYSE:
            data = self._nyse_tickers
        elif market == StockMarket.AMEX:
            data = self._amex_tickers
        if data is None:
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

        # Filer out those tickers with /, ^, %, acquisition, blank checks, 5 digits, W
        _data = _data[
            ~_data["ticker"].str.contains("/|\^", regex=True)
            & ~_data["name"].str.contains(
                "Acquisition|%|acquisition", case=False, regex=True
            )
            & ~_data["industry"].str.contains("Blank Checks", case=False)
            & ~(_data["ticker"].str.len() == 5)
            & ~_data["ticker"].str.endswith("W")
        ]

        _data = _data[columns.values()]
        all_results = _data.to_dict(orient="records")
        # Save to database
        await StockDbUtils.insert(table=DbTable.TICKERS, data=all_results)
        logging.info(f"Done for {market}")

    async def handle_all_tickers(self) -> None:
        self.get_all_tickers()
        # await self._handle_tickers(market=StockMarket.NASDAQ)
        # await self._handle_tickers(market=StockMarket.NYSE)
        await self._handle_tickers(market=StockMarket.AMEX)

    async def update_blank_fields(self) -> None:
        """
        Update those blank `sector` and `industry` fields from yfinance
        """
        data = await StockDbUtils.read(table=DbTable.TICKERS, output="df")
        data = data[(data["sector"] == "") | (data["industry"] == "")]
        if data.empty:
            logging.info("No blank fields found")
            return

        succ_count = 0
        fail_count = 0
        for index, row in data.iterrows():
            ticker = row["ticker"]
            print(f"Processing {ticker}")
            try:
                ticker_data = yf.Ticker(ticker).info
                sector = ticker_data.get("sector", "")
                industry = ticker_data.get("industry", "")
                if not sector and not industry:
                    continue
                logging.info(
                    f"Updating {ticker} with sector {sector} and industry {industry}"
                )
                await StockDbUtils.update(
                    DbTable.TICKERS,
                    {"ticker": ticker},
                    {"sector": sector, "industry": industry},
                )
                succ_count += 1
                logging.info(
                    f"Updated {ticker} with sector {sector} and industry {industry}"
                )
            except Exception as e:
                logging.error(e)
                fail_count += 1
                continue

            asyncio.sleep(2)

        await StockDbUtils.create_logging(
            "YFinanceSectorIndUpdate",
            True,
            f"Updated {succ_count} tickers, failed {fail_count} tickers",
        )


class StockDividends:
    """
    Handle Stock Split / Dividend on recent 2 trading days.
    Should be run before market opens to accelerate the candlestick chart downloading process.

    Example:
        from us_stock_wizard.stocks import StockDividends
        sd = StockDividends()
        await sd.initialize()
        sd.handle_all()
        await sd.save_to_db()

    """

    def __init__(self) -> None:
        self.tickers_df: pd.DataFrame = pd.DataFrame()

    async def initialize(self) -> None:
        await self.get_tickers()

    async def get_tickers(self) -> None:
        """
        Get tickers from database
        """
        self.tickers_df = await StockDbUtils.read(
            table=DbTable.TICKERS, where={"delisted": False}, output="df"
        )
        self.diff_df = pd.DataFrame()

    def handle_all(self) -> None:
        """
        Handle all tickers, and save the result to dataframe
        """
        # iterate through all tickers
        if self.tickers_df.empty:
            raise Exception("Tickers not loaded")
        modified_tickers_df = self.tickers_df.copy()
        for _, row in modified_tickers_df.iterrows():
            ticker = row["ticker"]
            is_dividend = self.check_dividend_and_split(ticker=ticker)
            modified_tickers_df.at[_, "recentSplitDivend"] = is_dividend
        # Check diff between two dataframes
        self.diff_df = modified_tickers_df[
            modified_tickers_df["recentSplitDivend"]
            != self.tickers_df["recentSplitDivend"]
        ]

    async def save_to_db(self) -> None:
        """
        Save the result to database
        """
        if self.diff_df.empty:
            logging.warning("No diff found, nothing to save")
            return None
        for _, row in self.diff_df.iterrows():
            id = row["id"]
            recentSplitDivend = row["recentSplitDivend"]
            # save to db
            await StockDbUtils.update(
                table=DbTable.TICKERS,
                where={"id": id},
                data={"recentSplitDivend": recentSplitDivend},
            )
        logging.info(f"Done inserted {self.diff_df.shape[0]} rows into database")

    @staticmethod
    def check_dividend_and_split(ticker: str, days_ago: int = 2) -> bool:
        """
        Checks if a stock has had a dividend or split in the last n trading days
        """
        if days_ago > 5:
            raise ValueError("Days ago must be less than 5")
        try:
            # Get the data of the stock
            stock = yf.Ticker(ticker)

            # Get stock info
            hist = stock.history(period="5d").tail(days_ago)

            has_dividend = hist["Dividends"].sum() != 0
            has_split = hist["Stock Splits"].sum() != 0

            # Check flags and return result
            return has_dividend or has_split
        except Exception as e:
            logging.error(e)
            return False


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

    def get_calendar(
        self, days_before: int = 300, days_forward: int = 90
    ) -> List[pd.Timestamp]:
        _days_before = (pd.Timestamp.today() - pd.Timedelta(days=days_before)).strftime(
            "%Y-%m-%d"
        )
        later = (pd.Timestamp.today() + pd.Timedelta(days=days_forward)).strftime(
            "%Y-%m-%d"
        )

        result: List[pd.Timestamp] = self.nyse.schedule(
            start_date=_days_before, end_date=later
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
