"""
Get the fundamental data from Alpha Vantage API.
The API Key shall be stored in the .env file.
"""
import asyncio
import logging
from time import sleep
from typing import List
import httpx
import pandas as pd
from us_stock_wizard import StockRootDirectory
from us_stock_wizard.database.db_utils import StockDbUtils


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
        return {}

    def get_is_data(self, stock: str) -> dict:
        """
        Get data from income statement
        """
        params = {
            "function": "INCOME_STATEMENT",
            "symbol": stock,
            "apikey": self.keys[0],
        }
        result = self._get_data(self.BASE_URL, params=params)
        return result

    @staticmethod
    def _process_report(ticker: str, data: dict, report_type: ReportType) -> List[dict]:
        _data = pd.DataFrame(data)
        # set as int
        _data["grossProfit"] = _data["grossProfit"].astype(int)
        _data["netIncome"] = _data["netIncome"].astype(int)
        _data["totalRevenue"] = _data["totalRevenue"].astype(int)
        _data["grossMaginRatio"] = _data["grossProfit"] / _data["totalRevenue"]
        _data = _data[
            ["fiscalDateEnding", "netIncome", "totalRevenue", "grossMaginRatio"]
        ]
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

    async def process_is_data(self, stock: str) -> bool:
        """
        Process the Income Statement data, and save it into the database
        """
        data = self.get_is_data(stock)
        if not data:
            logging.error("No data found for %s", stock)
            return False
        quarterly_reports: dict = data.get("quarterlyReports")
        annaul_reports: dict = data.get("annualReports")
        if not quarterly_reports and not annaul_reports:
            logging.error("No data found for %s", stock)
            return False

        if quarterly_reports:
            _quarterly_reports = self._process_report(
                ticker=stock, data=quarterly_reports, report_type=ReportType.QUARTERLY
            )
            await StockDbUtils.insert(table="Fundamentals", data=_quarterly_reports)
        if annaul_reports:
            _annaul_reports = self._process_report(
                ticker=stock, data=annaul_reports, report_type=ReportType.ANNUAL
            )
            await StockDbUtils.insert(table="Fundamentals", data=_annaul_reports)
        logging.info(f"Done for {stock}")

        return True
