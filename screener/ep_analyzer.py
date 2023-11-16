"""
Epsodic Pivot Analyzer
"""
from typing import Optional, List, Dict
from enum import Enum
import logging
import datetime
from datetime import timedelta
import numpy as np
import pandas as pd

from us_stock_wizard.database.db_utils import StockDbUtils, DbTable


class EpAnalyzer:
    """
    Analyze the EP.

    Example:
        ep = EpAnalyzer()
        await ep.get_calendar()
        await ep.initialize_data(start_date="2023-02-16")
    """

    def __init__(self) -> None:
        self.calendar: Optional[pd.DataFrame] = None
        self.kline_start: Optional[pd.DataFrame] = None
        self.kline_end: Optional[pd.DataFrame] = None
        self.common_tickers: Optional[List[str]] = None
        self.results = pd.DataFrame()  # Store the results

    async def get_calendar(self) -> pd.DataFrame:
        """
        Get Trading Calendar
        """
        self.calendar = await StockDbUtils.read(
            table=DbTable.TRADING_CALENDAR, output="df"
        )
        self.calendar.sort_values(by=["date"], ascending=True, inplace=True)
        self.calendar.drop_duplicates(subset=["date"], inplace=True)
        self.calendar["date"] = self.calendar["date"].dt.tz_convert(None)

    def get_dates(self, start_date: pd.Timestamp | str) -> List[pd.Timestamp]:
        """
        Get several trading dates in the list. Now, 1, 2, 7days

        Args:
            start_date (datetime.datetime): The Start Date
        """
        start_date = pd.to_datetime(start_date)
        dates: List[pd.Timestmap] = (
            self.calendar[self.calendar["date"] >= start_date].head(7)["date"].to_list()
        )
        assert len(dates) == 7, "Not enough trading dates"
        _ = [dates[0], dates[1], dates[-1]]
        return _

    async def get_klines(self, on_date: datetime.datetime) -> pd.DataFrame:
        """
        Get Kline
        """
        kline = await StockDbUtils.read(
            table=DbTable.DAILY_KLINE, where={"date": on_date}, output="df"
        )
        return kline

    async def initialize_data(self, start_date: pd.Timestamp | str) -> bool:
        """
        Test Run.
        """
        try:
            start, end, reference = self.get_dates(start_date=start_date)
            self.kline_start = await self.get_klines(on_date=start)
            self.kline_end = await self.get_klines(on_date=end)
            self.kline_reference = await self.get_klines(on_date=reference)

            self.common_tickers: List[str] = list(
                set(self.kline_start["ticker"]).intersection(
                    set(self.kline_end["ticker"])
                )
            )
            self.kline_start.set_index("ticker", inplace=True)
            self.kline_end.set_index("ticker", inplace=True)
            self.kline_reference.set_index("ticker", inplace=True)
            assert self.common_tickers, "No Common Tickers"
            return True
        except Exception as e:
            logging.error(f"Error in initialize_data: {e}")
            return False

    async def screen(
        self, start_date: pd.Timestamp | str, gap_up: float = 0.1
    ) -> pd.DataFrame:
        """
        Screen the EP.

        Args:
            start_date (pd.Timestamp | str): The Start Date
        """
        init_succ = await self.initialize_data(start_date=start_date)
        if not init_succ:
            return pd.DataFrame()
        klines_merged = pd.merge(
            self.kline_start,
            self.kline_end,
            left_index=True,
            right_index=True,
            how="inner",
        )

        # New column:
        klines_merged["gap"] = (
            klines_merged["open_y"] - klines_merged["high_x"]
        ) / klines_merged["high_x"]
        # Sort descending by gap
        klines_merged.sort_values(by=["gap"], ascending=False, inplace=True)
        # filter out gap < 0
        klines_merged = klines_merged[klines_merged["gap"] > gap_up]

        # Merge with reference
        klines_merged = pd.merge(
            klines_merged,
            self.kline_reference,
            left_index=True,
            right_index=True,
            how="inner",
        )

        # Check 7-day price change
        klines_merged["7d_price_chg"] = (
            klines_merged["close"] / klines_merged["close_y"] - 1
        )

        # Filter Columns
        klines_merged = klines_merged[
            ["date_x", "gap", "close_y", "close", "7d_price_chg"]
        ]
        # Rename Columns
        klines_merged.columns = [
            "date",
            "gap",
            "next_day_close",
            "7d_close",
            "7d_pct_change",
        ]
        klines_merged["date"] = klines_merged["date"].dt.date
        return klines_merged

    async def screen_all(
        self, start_date: pd.Timestamp | str, end_date: pd.Timestamp | str
    ) -> pd.DataFrame:
        """
        Screen all in the date range.
        """
        dates = self.calendar[
            (self.calendar["date"] >= start_date) & (self.calendar["date"] <= end_date)
        ]["date"].to_list()
        assert dates, "No dates in the range"
        for _date in dates:
            res = await self.screen(start_date=_date)
            logging.warning(f"There are {res.shape[0]} stocks gap up on {_date}")
            if res.empty:
                continue
            self.results = pd.concat([self.results, res])

        # Save to file
        self.results.to_csv("ep_results.csv", index=False)
        logging.warning(f"Saved to ep_results.csv")
