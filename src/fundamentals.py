"""
Get the fundamental data from Alpha Vantage API.
The API Key shall be stored in the .env file.
"""
import asyncio
from datetime import datetime
from io import StringIO
import logging
from time import sleep
from typing import List
import httpx
import numpy as np
import pandas as pd
from us_stock_wizard import StockRootDirectory
from us_stock_wizard.database.db_utils import StockDbUtils, DbTable
from us_stock_wizard.src.common import StockCommon


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

    def _get_data(self, base_url: str, params: dict = {}) -> dict:
        """
        Get data from the given url
        """
        count = 0
        while count < len(self.keys):
            params["apikey"] = self.keys[self.key_index]
            result = httpx.get(base_url, params=params)
            if result.status_code == 200 and result.json().get("Note") is None:
                return result.json()
            self.key_index += 1
            if self.key_index >= len(self.keys):
                self.key_index = 0
            count += 1
            sleep(2)  # Avoid hitting the API limit
        raise Exception(f"Failed to fetch data from API, params: {params}")

    def get_is_data(self, stock: str) -> dict:
        """
        Get data from income statement
        """
        params = {
            "function": "INCOME_STATEMENT",
            "symbol": stock,
        }
        result = self._get_data(self.BASE_URL, params=params)
        return result

    def get_earning_call_data(self) -> dict:
        """
        Get incoming earning call data
        """
        params = {
            "function": "EARNINGS_CALENDAR",
            "horizon": "3month",
            "apikey": "demo",
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
        logging.info("Done")

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
        _data["grossProfit"] = _data["grossProfit"].astype(int)
        _data["netIncome"] = _data["netIncome"].astype(int)
        _data["totalRevenue"] = _data["totalRevenue"].astype(int)
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

        # save to pickle
        _data.to_pickle(f"{ticker}_{report_type}.pkl")

        return _data.to_dict(orient="records")

    async def handle_is_data(self, stock: str) -> bool:
        """
        Process the Income Statement data, and save it into the database
        """
        data = self.get_is_data(stock)
        print(data)
        if not data:  # Maybe info not exist for the ticker.
            logging.error("No data found for %s", stock)
            await StockDbUtils.update(
                DbTable.TICKERS,
                {"ticker": stock},
                {"fundamentalsUpdatedAt": datetime.now()},
            )
            return False
        quarterly_reports: dict = data.get("quarterlyReports")
        annaul_reports: dict = data.get("annualReports")
        if not quarterly_reports and not annaul_reports:
            logging.error("No data found for %s", stock)
            await StockDbUtils.update(
                DbTable.TICKERS,
                {"ticker": stock},
                {"fundamentalsUpdatedAt": datetime.now()},
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
            _annaul_reports = self._process_report(
                ticker=stock, data=annaul_reports, report_type=ReportType.ANNUAL
            )
            await StockDbUtils.insert(table=DbTable.FUNDAMENTALS, data=_annaul_reports)
        logging.info(f"Done for {stock}")
        await StockDbUtils.update(
            DbTable.TICKERS,
            {"ticker": stock},
            {"fundamentalsUpdatedAt": datetime.now()},
        )
        return True

    async def handle_all_is_data(self) -> None:
        """
        Process the Income Statement data for all tickers
        """
        all_tickers: pd.DataFrame = await StockCommon.get_stock_list(format="df")

        earning_call_data = await StockDbUtils.read(table=DbTable.EARNING_CALL)
        earning_call_data = StockCommon.convert_to_dataframe(earning_call_data)

        # Null, those tickers have no fundamental data and never fetched before
        all_tickers_null = all_tickers[all_tickers["fundamentalsUpdatedAt"].isnull()]

        # Not null, updated before
        all_tickers_not_null = all_tickers[
            all_tickers["fundamentalsUpdatedAt"].notnull()
        ]
        all_tickers_not_null["fundamentalsUpdatedAt"] = pd.to_datetime(
            all_tickers_not_null["fundamentalsUpdatedAt"]
        )

        # Handle null
        for ticker in all_tickers_null["ticker"].tolist():
            res = await self.handle_is_data(ticker)
            # Always mark as succeed, even if failed
            logging.info(f"Done for {ticker}")
            asyncio.sleep(10)

        # Handle not null
