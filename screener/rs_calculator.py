from typing import Optional, List
import logging
import pandas as pd
import numpy as np
import datetime
from prisma import Json
from us_stock_wizard.database.db_utils import DbTable, StockDbUtils
from us_stock_wizard.src.stocks import TradingCalendar
from us_stock_wizard.src.common import StockCommon


class RelativeStrengthCalculator:
    """
    IBD-style RS calculator

    Rationle:
    RS Score = 40% * P3 + 20% * P6 + 20% * P9 + 20% * P12
    P3, P6, P9 and P12 are the stock's 3 Months, 6 Months, 9 Months and 12 Months performance respectively. Giving 40% weightage on the last quarter's performance and 20% on the other three.

    >>> rs = RelativeStrengthCalculator()
    >>> await rs.initialize()

    """

    def __init__(self) -> None:
        self.stocks: Optional[List[str]] = None
        self.calendar: List[datetime.date] = []

    async def initialize(self) -> None:
        """
        Initialize the screener
        """
        self.stocks = await StockCommon.get_stock_list()
        cal: pd.DataFrame = await StockDbUtils.read(
            DbTable.TRADING_CALENDAR, output="df"
        )
        cal["date"] = pd.to_datetime(cal["date"]).dt.date
        self.calendar = cal["date"].tolist()

    async def get_kline(
        self, ticker: str, deadline: Optional[datetime.date] = None
    ) -> pd.DataFrame:
        """
        Get the kline for the ticker
        """
        if not deadline:
            deadline = pd.Timestamp.today().date()
        kline = await StockDbUtils.read(
            DbTable.DAILY_KLINE, where={"ticker": ticker}, output="df"
        )
        if kline.empty:
            logging.warning(f"No kline data for {ticker}, skip")
            return kline
        kline = kline[["date", "adjClose"]]
        kline["date"] = pd.to_datetime(kline["date"]).dt.date
        kline = kline[kline["date"] <= deadline]
        return kline

    async def batch_get_rs(
        self, ticker: str, start: datetime.date, end: datetime.date
    ) -> Optional[pd.DataFrame]:
        """
        Calc the RS of a spec stock in a date range.
        """
        kline = await self.get_kline(ticker)
        if kline.shape[0] < 252:
            logging.warning(f"Insufficient data for {ticker}, skip")
            return None
        kline["date"] = pd.to_datetime(kline["date"])
        kline.set_index("date", inplace=True)
        kline = kline.resample("D").ffill()
        kline["90d_ago"] = kline["adjClose"].shift(90)
        kline["180d_ago"] = kline["adjClose"].shift(180)
        kline["270d_ago"] = kline["adjClose"].shift(270)
        kline["360d_ago"] = kline["adjClose"].shift(360)
        kline[ticker] = (
            kline["adjClose"] / kline["90d_ago"] * 0.4
            + kline["adjClose"] / kline["180d_ago"] * 0.2
            + kline["adjClose"] / kline["270d_ago"] * 0.2
            + kline["adjClose"] / kline["360d_ago"] * 0.2
        )

        # filter start and end date
        _start = pd.to_datetime(start)
        _end = pd.to_datetime(end)
        kline = kline[kline.index >= _start]
        kline = kline[kline.index <= _end]
        result = kline[[ticker]]
        return result

    async def batch_get_all_rs(
        self, start: datetime.date, end: datetime.date
    ) -> pd.DataFrame:
        """
        Get RS of all stocks in a date range
        """
        combined = pd.DataFrame()
        for ticker in self.stocks:
            logging.warning(f"Batch RS Calc: {ticker} - {start} - {end}")
            rs_df = await self.batch_get_rs(ticker, start, end)
            if rs_df is None:
                continue
            combined = pd.concat([combined, rs_df], axis=1)

        succ_count = 0
        fail_count = 0
        for index, row in combined.iterrows():
            date = index
            _df = pd.DataFrame(row)
            _df.dropna(inplace=True)
            _df["rank"] = _df[_df.columns[0]].rank(ascending=True)
            _df["rscore"] = _df["rank"] / len(_df["rank"]) * 100
            _df["rscore"] = _df["rscore"].astype(int)
            _df.reset_index(inplace=True)
            _df.rename(columns={"index": "ticker"}, inplace=True)
            _df["date"] = pd.Timestamp(date)
            _df = _df[["date", "rscore", "ticker"]]
            if not _df.empty:
                await StockDbUtils.insert(
                    DbTable.RELATIVE_STRENGTH, _df.to_dict(orient="records")
                )
                logging.warning(f"Batch RS Calc Done for {date}")
                succ_count += 1
            else:
                logging.warning(f"Batch RS Calc: No data for {date}")
                fail_count += 1
        logging.warning(f"Batch RS Calc: {succ_count} succ, {fail_count} fail")

    async def get_rs(
        self,
        ticker: str,
        date: Optional[datetime.date] = None,
    ) -> Optional[float]:
        """
        Will not consider stocks with less than 252 trading days of data
        """
        if not date:
            date = pd.Timestamp.today().date()
        if date not in self.calendar:
            logging.warning(f"{date} is not a trading day, skip")
            return None
        kline = await self.get_kline(ticker, date)
        if kline.shape[0] < 252:
            logging.warning(f"Insufficient data for {ticker}, skip")
            return None

        # Calculate the RS
        price_now = kline.iloc[-1]["adjClose"]
        three_months_ago = date - datetime.timedelta(days=90)
        _ = kline[kline["date"] >= three_months_ago]
        price_3m_ago = _["adjClose"].iloc[0]
        six_months_ago = date - datetime.timedelta(days=180)
        _ = kline[kline["date"] >= six_months_ago]
        price_6m_ago = _["adjClose"].iloc[0]
        nine_months_ago = date - datetime.timedelta(days=270)
        _ = kline[kline["date"] >= nine_months_ago]
        price_9m_ago = _["adjClose"].iloc[0]
        twelve_months_ago = date - datetime.timedelta(days=360)
        _ = kline[kline["date"] >= twelve_months_ago]
        price_12m_ago = _["adjClose"].iloc[0]

        p3 = price_now / price_3m_ago
        p6 = price_now / price_6m_ago
        p9 = price_now / price_9m_ago
        p12 = price_now / price_12m_ago
        rs = 0.4 * p3 + 0.2 * p6 + 0.2 * p9 + 0.2 * p12
        logging.warning(f"{ticker} RS: {rs} on date {date}")
        return rs

    async def update_all_rs(self, date: Optional[datetime.date] = None) -> bool:
        """ """
        if not date:
            date = pd.Timestamp.today().date()
        rs_list = []
        for ticker in self.stocks:
            rs = await self.get_rs(ticker, date)
            if rs:
                rs_list.append({"ticker": ticker, "rs": rs})
        rs_df = pd.DataFrame(rs_list)
        rs_df["rank"] = rs_df["rs"].rank(ascending=True)
        rs_df["rscore"] = rs_df["rank"] / len(rs_df["rank"]) * 100
        rs_df["rscore"] = rs_df["rscore"].astype(int)
        rs_df["date"] = pd.Timestamp(date)
        rs_df = rs_df[["date", "rscore", "ticker"]]
        if not rs_df.empty:
            await StockDbUtils.insert(
                DbTable.RELATIVE_STRENGTH, rs_df.to_dict(orient="records")
            )
            logging.warning(f"RS Calc Done for {date}")
            return True

        logging.warning(f"No data for {date}")
        return False
