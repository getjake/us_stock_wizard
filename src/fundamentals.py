"""
Get the fundamental data from Alpha Vantage API.
The API Key shall be stored in the .env file.
"""

import asyncio
from datetime import datetime
from io import StringIO
import logging

logging.basicConfig(level=logging.INFO)
from time import sleep
from typing import List
import httpx
import numpy as np
import pandas as pd
import yfinance as yf
from us_stock_wizard import StockRootDirectory
from us_stock_wizard.database.db_utils import StockDbUtils, DbTable
from us_stock_wizard.src.common import StockCommon, retry_decorator


class DataSource:
    ALPHA_VANTAGE = "alphavantage"
    YFINANCE = "yfinance"


class ReportType:
    ANNUAL = "ANNUAL"
    QUARTERLY = "QUARTERLY"


class Fundamentals:
    """
    Fetch the recent fundamental data from Alpha Vantage API.
    """

    BASE_URL = "https://www.alphavantage.co/query"

    def __init__(self) -> None:
        self.env = StockRootDirectory.env()
        self.keys = self.get_keys()
        self.key_index = 0

    def get_keys(self) -> List[str]:
        keys = self.env.get("ALPHA_VANTAGE_API_KEYS")
        if not keys:
            logging.error("No API keys found.")
            return []
        return keys.split(",")

    @retry_decorator(delay=5, failure_return_types=[])
    def _get_data_alphavantage(self, base_url: str, params: dict = {}) -> dict:
        """
        Get data from the given url
        """
        count = 0
        while count < len(self.keys):
            params["apikey"] = self.keys[self.key_index]
            result = httpx.get(base_url, params=params, timeout=10)
            if result.status_code == 200 and result.json().get("Note") is None:
                return result.json()
            self.key_index += 1
            if self.key_index >= len(self.keys):
                self.key_index = 0
            count += 1
            sleep(2)  # Avoid hitting the API limit
        raise Exception(f"Failed to fetch data from API, params: {params}")

    @staticmethod
    def _process_yfinance_data(data: pd.DataFrame) -> List[dict]:
        """
        Process yfinance data to the format of alphavantage
        """
        _data = data.T
        _data = _data[["Net Income", "Total Revenue", "Gross Profit"]]
        # Convert `"Net Income"` to int
        _data["Net Income"] = _data["Net Income"].astype(float).astype(int)
        _data["Total Revenue"] = _data["Total Revenue"].astype(float).astype(int)
        _data["Gross Profit"] = _data["Gross Profit"].astype(float).astype(int)
        _data.reset_index(inplace=True)
        _data = _data.rename(
            columns={
                "index": "fiscalDateEnding",
                "Net Income": "netIncome",
                "Gross Profit": "grossProfit",
                "Total Revenue": "totalRevenue",
            }
        )
        _data = _data.astype(str)
        _data = _data.replace("", "None")
        return _data.to_dict(orient="records")

    def _get_data_yfinance(self, stock: str) -> dict:
        """
        Get data from yfinance
        """
        ticker = yf.Ticker(stock)
        quarterly_data = ticker.quarterly_income_stmt
        annual_data = ticker.income_stmt

        _ = {
            "symbol": stock,
            "annualReports": self._process_yfinance_data(annual_data),
            "quarterlyReports": self._process_yfinance_data(quarterly_data),
        }

        return _

    def get_is_data(self, stock: str, source: str = DataSource.YFINANCE) -> dict:
        """
        Get data from income statement

        Args:
            stock: The stock ticker
            source: The source of the data, either DataSource.YFINANCE or DataSource.ALPHA_VANTAGE
        """
        if source == DataSource.YFINANCE:
            return self._get_data_yfinance(stock)
        if source == DataSource.ALPHA_VANTAGE:
            params = {
                "function": "INCOME_STATEMENT",
                "symbol": stock,
            }
            result = self._get_data_alphavantage(self.BASE_URL, params=params)
            return result
        raise Exception(
            f"Unknown source: {source}, only support `yfinance` or `alphavantage`"
        )

    def get_earning_call_data(self) -> dict:
        """
        Get incoming earning call data
        """
        params = {
            "function": "EARNINGS_CALENDAR",
            "horizon": "3month",
            "apikey": self.keys[0],
        }
        response = httpx.get(self.BASE_URL, params=params)
        assert response.status_code == 200, "Failed to fetch data from API"
        data = StringIO(response.text)
        df = pd.read_csv(data)
        return df

    @staticmethod
    def _process_earning_call_data(data: pd.DataFrame) -> List[dict]:
        """
        Process the earning call data
        """
        if data.empty:
            logging.warning("No earning call data found")
            return []
        data = data[data.currency == "USD"]
        data = data.rename(columns={"symbol": "ticker"})
        data["reportDate"] = pd.to_datetime(data["reportDate"])
        data["fiscalDateEnding"] = pd.to_datetime(data["fiscalDateEnding"])
        data = data[["ticker", "reportDate", "fiscalDateEnding"]]
        return data.to_dict(orient="records")

    async def handle_earning_call_data(self) -> None:
        """
        Process the earning call data
        """
        data = self.get_earning_call_data()
        data = self._process_earning_call_data(data)
        await StockDbUtils.insert(table=DbTable.EARNING_CALL, data=data)
        logging.info("Earning call data updated.")

    @staticmethod
    def _process_report(ticker: str, data: dict, report_type: ReportType) -> List[dict]:
        """
        Process the Income Statement Report

        Only keep the following columns:
            - fiscalDateEnding
            - netIncome
            - totalRevenue
            - grossMaginRatio
        """
        _data = pd.DataFrame(data)
        _data = _data.replace("None", 0)
        _data["grossProfit"] = _data["grossProfit"].astype(float).astype(int)
        _data["netIncome"] = _data["netIncome"].astype(float).astype(int)
        _data["totalRevenue"] = _data["totalRevenue"].astype(float).astype(int)
        _data["grossMaginRatio"] = _data["grossProfit"] / _data["totalRevenue"]
        _data = _data[
            ["fiscalDateEnding", "netIncome", "totalRevenue", "grossMaginRatio"]
        ]
        _data = _data.replace(-np.inf, 0)
        _data = _data.replace(np.inf, 0)
        _data = _data.replace(np.nan, 0)

        # Rename

        _data = _data.rename(
            columns={
                "fiscalDateEnding": "reportDate",
                "netIncome": "netIncome",
                "totalRevenue": "sales",
                "grossMaginRatio": "grossMarginRatio",
            }
        )
        _data["ticker"] = ticker
        _data["reportType"] = report_type
        return _data.to_dict(orient="records")

    async def handle_is_data(
        self, stock: str, source: str = DataSource.YFINANCE
    ) -> bool:
        """
        Process the Income Statement data, and save it into the database
        """
        data = self.get_is_data(stock, source=source)
        if not data:  # Maybe info not exist for the ticker.
            logging.error("No data found for %s", stock)
            await StockDbUtils.update(
                DbTable.TICKERS,
                {"ticker": stock},
                {"fundamentalsUpdatedAt": None},  # Remove the updated time
            )
            return False
        quarterly_reports: dict = data.get("quarterlyReports")
        annaul_reports: dict = data.get("annualReports")
        if not quarterly_reports and not annaul_reports:
            logging.error("No data found for %s", stock)
            await StockDbUtils.update(
                DbTable.TICKERS,
                {"ticker": stock},
                {"fundamentalsUpdatedAt": None},  # Remove the updated time
            )
            return False

        if quarterly_reports:
            _quarterly_reports = self._process_report(
                ticker=stock, data=quarterly_reports, report_type=ReportType.QUARTERLY
            )
            await StockDbUtils.insert(
                table=DbTable.FUNDAMENTALS, data=_quarterly_reports
            )
        if annaul_reports:
            _annual_report = self._process_report(
                ticker=stock, data=annaul_reports, report_type=ReportType.ANNUAL
            )
            await StockDbUtils.insert(table=DbTable.FUNDAMENTALS, data=_annual_report)

        logging.warning(f"Fundamental Successfully updated for {stock}")
        await StockDbUtils.update(
            DbTable.TICKERS,
            {"ticker": stock},
            {"fundamentalsUpdatedAt": datetime.now()},
        )
        return True

    async def handle_all_is_data(self, source: str = DataSource.YFINANCE) -> None:
        """
        Process the Income Statement data for all tickers, including those non-updated and updated.
        """
        all_tickers: pd.DataFrame = await StockCommon.get_stock_list(format="df")

        # Null, those tickers have no fundamental data and never fetched before
        all_tickers_null = all_tickers[all_tickers["fundamentalsUpdatedAt"].isnull()]

        # Not null, updated before
        all_tickers_not_null = all_tickers[
            all_tickers["fundamentalsUpdatedAt"].notnull()
        ]
        all_tickers_not_null.loc[:, "fundamentalsUpdatedAt"] = pd.to_datetime(
            all_tickers_not_null["fundamentalsUpdatedAt"]
        )

        # Proritize those tickers which have not been updated before
        null_list = all_tickers_null["ticker"].tolist()
        not_null_list = all_tickers_not_null["ticker"].tolist()
        np.random.shuffle(null_list)

        all_list = null_list + not_null_list
        return await self.update_tickers_data(all_list, source=source)

    async def update_expired_data(
        self, days_ago: int = 5, source: str = DataSource.YFINANCE
    ) -> None:
        """
        Update the expired data.
        """
        _days_ago = pd.Timestamp.today() - pd.Timedelta(days=days_ago)
        _ = await StockDbUtils.read(
            table=DbTable.TICKERS, where={"fundamentalsUpdatedAt": {"lte": _days_ago}}
        )
        expired_tickers: List[str] = [r.dict()["ticker"] for r in _]
        _ = await StockDbUtils.read(
            table=DbTable.TICKERS, where={"fundamentalsUpdatedAt": None}
        )
        wrong_tickers: List[str] = [r.dict()["ticker"] for r in _]

        tickers = expired_tickers + wrong_tickers
        return await self.update_tickers_data(tickers, source=source)

    async def update_tickers_data(
        self, tickers: List[str], source: str = DataSource.YFINANCE
    ) -> None:
        """
        Update IS data for the given tickers
        """
        succ_count = 0
        fail_count = 0
        failed_tickers = []
        for ticker in tickers:
            logging.info(f"Fundamental Start for {ticker}")
            succ = await self.handle_is_data(ticker, source=source)
            if succ:
                logging.info(f"Fundamental Done for {ticker}")
                succ_count += 1
            else:
                logging.error(f"Fundamental Failed for {ticker}")
                failed_tickers.append(ticker)
                fail_count += 1
            if source == DataSource.YFINANCE:
                await asyncio.sleep(2)
            else:
                await asyncio.sleep(10)
        # Log
        await StockDbUtils.create_logging(
            DbTable.FUNDAMENTALS,
            success=True,
            msg=f"Fundamental Success: {succ_count}, Failed: {fail_count}, failed_tickers: {failed_tickers}",
        )

    async def update_is_data(
        self, days_ago: int = 7, source: str = DataSource.YFINANCE
    ) -> None:
        """
        Update the Income Statement data for tickers which has the earning call less than __ days.
        """
        # Get the earning call data
        data = await StockDbUtils.read(table=DbTable.EARNING_CALL, output="df")
        # Filter data >= days_ago and <= today
        data["reportDate"] = pd.to_datetime(data["reportDate"]).dt.date
        data = data[
            (
                data["reportDate"]
                >= (pd.Timestamp.today() - pd.Timedelta(days=days_ago)).date()
            )
            & (data["reportDate"] <= pd.Timestamp.today().date())
        ]
        # Get the tickers
        tickers: List[str] = data["ticker"].unique().tolist()
        if not tickers:
            logging.info("No tickers found, do not need to update")
            return None

        # Update the tickers
        for ticker in tickers:
            logging.info(f"Start for {ticker}")
            await self.handle_is_data(ticker, source=source)
            logging.info(f"Done for {ticker}")
            if source == DataSource.YFINANCE:
                await asyncio.sleep(2)
            else:
                await asyncio.sleep(10)
