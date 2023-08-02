"""
Macro Analysis

1. NAA200R
"""
import logging
import asyncio
import datetime
from enum import Enum
from typing import List, Optional
from dotenv import load_dotenv
import httpx
import yfinance as yf
import numpy as np
import pandas as pd
from us_stock_wizard.database.db_utils import StockDbUtils, DbTable
from us_stock_wizard.src.common import StockCommon


import os
import pandas as pd
import matplotlib.pyplot as plt
from us_stock_wizard import StockRootDirectory
from us_stock_wizard.database.db_utils import StockDbUtils, DbTable


class Naa200R:
    """
    NAA200R

    Example:
    >>> naa200r = Naa200R()
    >>> await naa200r.initialize()
    >>> await naa200r.analyze_all()
    >>> await naa200r.save()
    """

    def __init__(self):
        self.stocks_df = pd.DataFrame()
        self.cache = pd.DataFrame()
        self.history_summary = pd.DataFrame()

    async def initialize(self):
        """Initialize"""
        await self.get_stocks()

    async def get_stocks(self) -> None:
        """
        Get all Nasdaq stocks that IPOed more than 1 year ago
        Choose tickers len < 5!
        """
        all_stocks = await StockDbUtils.read(table=DbTable.TICKERS, output="df")
        all_stocks = all_stocks[all_stocks["market"] == "NASDAQ"]
        curr_year = datetime.datetime.now().year
        year_min = curr_year - 1
        all_stocks = all_stocks[all_stocks["ipoYear"] <= year_min]
        all_stocks = all_stocks[all_stocks["ticker"].str.len() < 5]
        self.stocks_df = all_stocks

    async def analyze_one(self, ticker: str) -> pd.DataFrame:
        """
        Analyze one stock of its whole history

        MA200
        """
        kline = await StockDbUtils.read(
            table=DbTable.DAILY_KLINE, where={"ticker": ticker}, output="df"
        )
        if kline.empty:
            return pd.DataFrame()
        kline["date"] = pd.to_datetime(kline["date"]).dt.date
        kline = kline[["ticker", "date", "adjClose"]]
        kline["ma200"] = kline["adjClose"].rolling(200).mean()
        # check if the last ma200 is nan
        if kline[["ma200"]].tail(1).isna().values[0][0]:
            return pd.DataFrame()
        kline[ticker] = (kline["adjClose"] >= kline["ma200"]).astype(int)
        kline.set_index("date", inplace=True)
        return kline[[ticker]]

    async def analyze_all(self) -> None:
        """
        Analyse all Nasdaq Stocks in their whole history (recent 3 yrs)
        """
        if self.stocks_df.empty:
            raise ValueError("Please run get_stocks() first!")

        all_tickers: List[str] = self.stocks_df["ticker"].tolist()
        # Group into 10 tickers
        splited: List[List[str]] = StockCommon.split_list(all_tickers, 10)
        total_group = len(splited)
        for tickers in splited:
            curr_group = splited.index(tickers) + 1
            logging.warning(f"Analyzing group {curr_group}/{total_group}")
            tasks = [self.analyze_one(ticker) for ticker in tickers]
            results = await asyncio.gather(*tasks)
            self.cache = pd.concat(results, axis=1)
            self.cache.fillna(method="ffill", inplace=True)
            self.cache.fillna(value=0, inplace=True)
            if self.cache.empty:
                continue
            self.cache["above_200ma"] = self.cache.sum(axis=1)
            self.cache["below_200ma"] = self.cache.shape[1] - self.cache["above_200ma"]
            if self.history_summary.empty:
                self.history_summary = self.cache[["above_200ma", "below_200ma"]]
            else:
                self.history_summary = (
                    self.history_summary + self.cache[["above_200ma", "below_200ma"]]
                )
            self.cache = pd.DataFrame()  # Clear cache
        # Calculate the percentage
        self.history_summary["total"] = self.history_summary.sum(axis=1)
        # value => above_pct
        self.history_summary["value"] = (
            self.history_summary["above_200ma"] / self.history_summary["total"]
        )
        self.history_summary = self.history_summary[["value"]]
        logging.warning("Analysis completed!")

    async def save(self):
        """
        Save all to database
        """
        if self.history_summary.empty:
            raise ValueError("Please run analyze_all() first!")
        self.history_summary.reset_index(inplace=True)
        self.history_summary = self.history_summary.tail(300)  # Last 300 days
        self.history_summary["date"] = pd.to_datetime(self.history_summary["date"])
        data: List[dict] = self.history_summary.to_dict("records")
        await StockDbUtils.insert(table=DbTable.NAA200R, data=data)
        logging.warning("Saved Naa200R to database!")

    @staticmethod
    async def export_image(output_path: Optional[str] = None, days: int = 300) -> None:
        """
        Export recent 300 days to image
        """
        if not output_path:
            output_path = os.path.join(
                StockRootDirectory.root_dir(), "export", "naa200r.png"
            )

        df = await StockDbUtils.read(table=DbTable.NAA200R, output="df")
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values(by="date")  # ensure the data is sorted by date
        recent_df = df.tail(days)

        plt.figure(figsize=(12, 6))
        plt.plot(recent_df["date"], recent_df["value"])

        plt.title("NAA200R the recent 300 days")
        plt.xlabel("Date")
        plt.ylabel("Value")
        plt.grid(True)

        plt.tight_layout()  # Adjust the layout to make it fit well

        # save the plot as a PNG file
        plt.savefig(output_path, dpi=300)
