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
    ) -> Optional[List[pd.DataFrame]]:
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
        kline["30d_ago"] = kline["adjClose"].shift(30)
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

        # Handle M1, M3 and M6 only
        kline[f"{ticker}_M1"] = kline["adjClose"] / kline["30d_ago"]
        kline[f"{ticker}_M3"] = kline["adjClose"] / kline["90d_ago"]
        kline[f"{ticker}_M6"] = kline["adjClose"] / kline["180d_ago"]

        # filter start and end date
        _start = pd.to_datetime(start)
        _end = pd.to_datetime(end)
        kline = kline[kline.index >= _start]
        kline = kline[kline.index <= _end]

        _rs = kline[[ticker]]
        _m1 = kline[[f"{ticker}_M1"]]
        _m1 = _m1.rename(columns={f"{ticker}_M1": ticker})
        _m3 = kline[[f"{ticker}_M3"]]
        _m3 = _m3.rename(columns={f"{ticker}_M3": ticker})
        _m6 = kline[[f"{ticker}_M6"]]
        _m6 = _m6.rename(columns={f"{ticker}_M6": ticker})
        return _rs, _m1, _m3, _m6

    async def batch_get_all_rs(self, start: datetime.date, end: datetime.date) -> None:
        """
        Get RS of all stocks in a date range
        """
        combined_rs = pd.DataFrame()
        combined_m1 = pd.DataFrame()
        combined_m3 = pd.DataFrame()
        combined_m6 = pd.DataFrame()
        for ticker in self.stocks:
            logging.warning(f"Batch RS Calc: {ticker} - {start} - {end}")
            packed = await self.batch_get_rs(ticker, start, end)
            if packed is None:
                continue
            rs_df, m1_df, m3_df, m6_df = packed
            combined_rs = pd.concat([combined_rs, rs_df], axis=1)
            combined_m1 = pd.concat([combined_m1, m1_df], axis=1)
            combined_m3 = pd.concat([combined_m3, m3_df], axis=1)
            combined_m6 = pd.concat([combined_m6, m6_df], axis=1)

        succ_count = 0
        fail_count = 0

        for index, row in combined_rs.iterrows():
            date = index
            # Basic RS
            _df = pd.DataFrame(row)
            _df.dropna(inplace=True)
            # Relative Strength
            _df["rank"] = _df[_df.columns[0]].rank(ascending=True)
            _df["rscore"] = _df["rank"] / len(_df["rank"]) * 100
            _df["rscore"] = _df["rscore"].astype(int)

            # M1
            _m1 = pd.DataFrame(combined_m1.loc[date])
            _m1.dropna(inplace=True)
            _m1["rank"] = _m1[_m1.columns[0]].rank(ascending=True)
            _m1["M1"] = _m1["rank"] / len(_m1["rank"]) * 100
            _m1["M1"] = _m1["M1"].astype(int)

            # M3
            _m3 = pd.DataFrame(combined_m3.loc[date])
            _m3.dropna(inplace=True)
            _m3["rank"] = _m3[_m3.columns[0]].rank(ascending=True)
            _m3["M3"] = _m3["rank"] / len(_m3["rank"]) * 100
            _m3["M3"] = _m3["M3"].astype(int)

            # M6
            _m6 = pd.DataFrame(combined_m6.loc[date])
            _m6.dropna(inplace=True)
            _m6["rank"] = _m6[_m6.columns[0]].rank(ascending=True)
            _m6["M6"] = _m6["rank"] / len(_m6["rank"]) * 100
            _m6["M6"] = _m6["M6"].astype(int)

            # Merge
            merged = pd.concat([_df, _m1, _m3, _m6], axis=1)
            merged.reset_index(inplace=True)
            merged.rename(columns={"index": "ticker"}, inplace=True)
            merged["date"] = pd.Timestamp(date)
            merged = merged[["date", "ticker", "rscore", "M1", "M3", "M6"]]

            if not merged.empty:
                await StockDbUtils.insert(
                    DbTable.RELATIVE_STRENGTH, merged.to_dict(orient="records")
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
        await self.batch_get_all_rs(date, date)
        return True

    async def export_high_rs(
        self, days_ago: int = 90, threshold: int = 80, lasting: int = 10
    ) -> None:
        """
        Export high RS stocks

        Args:
        - days_ago: how many days ago to start the search
        - threshold: the RS threshold
        - lasting: how many days the RS should last
        """
        assert days_ago >= lasting, "days_ago should be greater than lasting"

        results: List[str] = []
        for ticker in self.stocks:
            rs_data = await StockDbUtils.read(
                DbTable.RELATIVE_STRENGTH, where={"ticker": ticker}, output="df"
            )
            if rs_data.shape[0] <= days_ago:
                continue

            rs_data.sort_values(by=["date"], inplace=True)
            data = rs_data.tail(days_ago)["rscore"] >= threshold
            # lasting days
            data = data.rolling(lasting).sum() >= lasting
            _ = True in data.unique()
            if _:
                results.append(ticker)

        # Export to database

        _ = {
            "date": pd.to_datetime(datetime.date.today()),
            "kind": "high_rs",
            "data": Json(results),
        }
        await StockDbUtils.insert(table=DbTable.REPORT, data=[_])
        logging.warning(f"High RS stocks count: {len(results)}")

    async def export_new_born(self) -> None:
        """
        Export new-born high RS stocks

        The Fixed Crets:
        - The recent 5 trading days has mean RS > 90
        - The recent 100-20 trading days has mean RS < 85
        """
        # Params
        _most_recent_days = 5
        _min_days = 20
        _max_days = 100

        results: List[str] = []
        for ticker in self.stocks:
            rs_data = await StockDbUtils.read(
                DbTable.RELATIVE_STRENGTH, where={"ticker": ticker}, output="df"
            )
            if rs_data.shape[0] <= _max_days:
                continue

            rs_data.sort_values(by=["date"], inplace=True)
            most_recent_rs: float = rs_data[-_most_recent_days:]["rscore"].mean()
            if most_recent_rs < 90:
                continue
            faraway_rs: float = rs_data[-_max_days:-_min_days]["rscore"].mean()
            if faraway_rs < 85:
                continue
            results.append(ticker)

        # Export to database

        _ = {
            "date": pd.to_datetime(datetime.date.today()),
            "kind": "newborn_rs",
            "data": Json(results),
        }
        await StockDbUtils.insert(table=DbTable.REPORT, data=[_])
        logging.warning(f"Newborn RS stocks count: {len(results)}")
