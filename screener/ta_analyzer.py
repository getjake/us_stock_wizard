from typing import Optional, List
import logging
import datetime
import pandas as pd
from us_stock_wizard.database.db_utils import StockDbUtils, DbTable

logging.basicConfig(level=logging.INFO)


class TaMeasurements:
    STAGE2 = "stage2"


class TaAnalyzer:
    """

    Usage:
        >>> ta = TaAnalyzer("AAPL")
        >>> await ta.get_kline()
        >>> res: bool = ta.get_result([TaMeasurements.STAGE2])
    """

    def __init__(self, ticker: str, rs: Optional[float]) -> None:
        self.ticker = ticker
        self.rs = rs or 0  # on the day's rs
        self.db_utils = StockDbUtils()
        self.kline: Optional[pd.DataFrame] = None

    async def get_kline(self) -> None:
        """
        Get kline data from database
        """
        kline = await StockDbUtils.read(
            DbTable.DAILY_KLINE, where={"ticker": self.ticker}, output="df"
        )
        kline["date"] = pd.to_datetime(kline["date"]).dt.date
        self.kline = kline

    def get_result(
        self, criteria: List[TaMeasurements], date: Optional[str] = None
    ) -> bool:
        """
        Get result of technical analysis

        Args:
            criteria (List[TaMeasurements]): List of criteria to be used
            date (Optional[str], optional): Date to be used. Defaults to None => today.
        """
        if self.kline is None:
            raise ValueError("kline is not loaded")
        if date is None:
            date = datetime.date.today().strftime("%Y-%m-%d")
        _date = pd.to_datetime(date).date()
        kline = self.kline.copy()

        date_included = kline[kline["date"] == _date]
        if date_included.empty:
            logging.warning(f"{_date} is not included in kline for {self.ticker}")
        kline = kline[kline["date"] <= _date]  # only use data before date
        kline = kline.sort_values(by="date", ascending=True)

        combined_bool = []
        for cret in criteria:
            if cret == TaMeasurements.STAGE2:
                _ = self._cret_stage_2(kline)
                combined_bool.append(_)
            else:
                raise ValueError(f"Unknown criteria: {cret}")
        return all(combined_bool)

    def _cret_stage_2(self, _kline: pd.DataFrame) -> bool:
        """
        Mark's Trend Template for Stage 2
        Docs: https://docs.google.com/document/d/1sS7uMXzG1j626b1BYXUhr8lY2B-YlQkF9e7skFPlmig/edit#bookmark=id.7uewkhhvqexh

        Mod: 满足 7 of 8 个条件就可以
        1. Stock price is above MA150 and MA200
        2. MA150 is above MA200
        3. MA200 is trending up for at least 1 month
        4. MA50 is above MA150
        5. Stock price is at least 25% above 52-week low.
        6. Stock price is within 25% of 52-week high.
        7. RS (relative strength) is above 70.
        8. Current price is above MA50.
        """
        # ma50, 150, 200
        kline = _kline.copy()
        kline["ma50"] = kline["adjClose"].rolling(50).mean()
        kline["ma150"] = kline["adjClose"].rolling(150).mean()
        kline["ma200"] = kline["adjClose"].rolling(200).mean()
        # 52 week high, low
        kline["rolling_high"] = kline["adjClose"].rolling(250).max()
        kline["rolling_low"] = kline["adjClose"].rolling(250).min()

        # Conditions
        latest = kline.iloc[-1]
        c_1 = latest["adjClose"] > latest["ma150"]
        c_2 = latest["ma150"] > latest["ma200"]

        # c_3
        ma200 = kline["ma200"]
        ma200 = ma200[-20:]  # last 20 days
        c_3 = ma200.iloc[0] < ma200.iloc[-1]

        c_4 = latest["ma50"] > latest["ma150"]

        c_5 = latest["adjClose"] > latest["rolling_low"] * 1.25
        c_6 = latest["adjClose"] > latest["rolling_high"] * 0.75
        c_7 = self.rs > 70
        c_8 = latest["adjClose"] > latest["ma50"]

        # Result
        result = sum([c_1, c_2, c_3, c_4, c_5, c_6, c_7, c_8]) >= 7
        return result
