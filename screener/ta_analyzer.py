from typing import Optional, List, Dict
from enum import Enum
from dateutil.relativedelta import relativedelta
import logging
import datetime
import pandas as pd
from us_stock_wizard.database.db_utils import StockDbUtils, DbTable

logging.basicConfig(level=logging.INFO)


class TaMeasurements(Enum):
    """
    Stage 2: Mark's Trend Template for Stage 2
    MINERVINI_5M: MA200 is trending up for at least 5 month
    MINERVINI_1M: MA200 is trending up for at least 1 month
    SEVEN_DAY_LOW_VOLATILITY: In the recent 7 trading days, the max price - min price range is less than 10%.
    RECENT_LOW_VOLUME: Today's volume is lower than the average volume of the last 20 / .. / .. days.
    """

    STAGE2 = "stage2"
    MINERVINI_5M = "minervini_5m"
    MINERVINI_1M = "minervini_1m"
    SEVEN_DAY_LOW_VOLATILITY = "seven_day_low_volatility"
    RECENT_LOW_VOLUME = "recent_low_volume"
    QULL_M1 = "qull_m1"
    QULL_M3 = "qull_m3"
    QULL_M6 = "qull_m6"

    @classmethod
    def list(cls):
        return [member.value for member in cls.__members__.values()]


class TaAnalyzer:
    """

    Usage:
        >>> ta = TaAnalyzer("AAPL")
        >>> await ta.get_kline()
        >>> res: dict[] = ta.get_result()
    """

    def __init__(
        self,
        ticker: str,
        rs: Optional[List[int]] = None,
        cret: List[TaMeasurements] = [],
    ) -> None:
        """
        Args:
            ticker (str): Ticker
            rs (Optional[float]): Today's Relative strength score - List [ Rscore, M1, M3, M6 ]
            cret: List of criteria to be used
        """
        self.ticker = ticker
        self.rs = rs or [0, 0, 0, 0]
        self.db_utils = StockDbUtils()
        self.kline: Optional[pd.DataFrame] = None
        # Default to all criteria
        self.cret: List[TaMeasurements] = cret or TaMeasurements.list()

    async def get_kline(self) -> None:
        """
        Get kline data from database
        """
        kline = await StockDbUtils.read(
            DbTable.DAILY_KLINE, where={"ticker": self.ticker}, output="df"
        )
        kline["date"] = pd.to_datetime(kline["date"]).dt.date
        self.kline = kline

    def get_result(self, date: Optional[str] = None) -> Dict[TaMeasurements, bool]:
        """
        Get result of technical analysis

        Args:
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

        result = {}
        for cret in self.cret:
            if cret == TaMeasurements.STAGE2.value:
                _ = self._cret_stage_2(kline)
                result[cret] = _
            elif cret == TaMeasurements.MINERVINI_5M.value:
                _ = self._cret_minervini(kline, months=5)
                result[cret] = _
            elif cret == TaMeasurements.MINERVINI_1M.value:
                _ = self._cret_minervini(kline, months=1)
                result[cret] = _
            elif cret == TaMeasurements.SEVEN_DAY_LOW_VOLATILITY.value:
                _ = self._cret_days_vol(kline, days=7, vol=0.12)
                result[cret] = _
            elif cret == TaMeasurements.RECENT_LOW_VOLUME.value:
                _ = self._cret_recent_low_volume(kline, days=1)
                result[cret] = _
            elif cret == TaMeasurements.QULL_M1.value:
                _ = self._cret_qull(kline, months=1)
                result[cret] = _
            elif cret == TaMeasurements.QULL_M3.value:
                _ = self._cret_qull(kline, months=3)
                result[cret] = _
            elif cret == TaMeasurements.QULL_M6.value:
                _ = self._cret_qull(kline, months=6)
                result[cret] = _
            else:
                raise ValueError(f"Unknown criteria: {cret}")
        return result

    def _cret_stage_2(self, _kline: pd.DataFrame) -> bool:
        """
        Mark's Trend Template for Stage 2
        Docs: https://docs.google.com/document/d/1sS7uMXzG1j626b1BYXUhr8lY2B-YlQkF9e7skFPlmig/edit#bookmark=id.7uewkhhvqexh

        Must statisfy all conditions, it is a AND relationship.
        - We would like the stock to be in a Stage 2 uptrend.
        - Also, don't want the stock to be too extended from the 50-day moving average.

        1. Stock price is above MA150 and MA200
        2. MA150 is above MA200
        3. MA200 is trending up for at least 1 month
        4. MA50 is above MA150
        5. Stock price is at least 25% above 52-week low.
        6. Stock price is within 25% of 52-week high.
        7. RS (relative strength) is above 70.
        8. Current price is above MA50.
        9. Stock Price > 5 USD
        10. Daily Volume USD > 200k USD
        11. Current close no more than 30% of the MA50
        12. Max Drawdown no more than 35% in the recent 90 trading days.
        """
        # ma50, 150, 200
        kline = _kline.copy()
        kline["ma50"] = kline["adjClose"].rolling(50).mean()
        kline["ma150"] = kline["adjClose"].rolling(150).mean()
        kline["ma200"] = kline["adjClose"].rolling(200).mean()
        # 52 week high, low
        kline["rolling_high"] = kline["adjClose"].rolling(250).max()
        kline["rolling_low"] = kline["adjClose"].rolling(250).min()
        # Average volume
        _avg_vol = int(kline["volume"].tail(20).mean())
        # Conditions
        latest = kline.iloc[-1]
        c_1 = latest["adjClose"] > latest["ma150"]
        # Slightly modified the criteria, making it available for the stock that is pumped recently.
        c_2 = latest["ma150"] > latest["ma200"] if self.rs[0] < 85 else True

        # c_3
        ma200 = kline["ma200"]
        ma200 = ma200[-20:]  # last 20 days
        c_3 = ma200.iloc[0] < ma200.iloc[-1]
        c_4 = latest["ma50"] > latest["ma150"]
        c_5 = latest["adjClose"] > latest["rolling_low"] * 1.25
        c_6 = latest["adjClose"] > latest["rolling_high"] * 0.75
        c_7 = self.rs[0] > 70
        c_8 = latest["adjClose"] > latest["ma50"]
        c_9 = latest["adjClose"] >= 5  # 5 USD
        c_10 = _avg_vol >= 25000  # 25000 Share Volume at least

        # c_11 - No more than 30% of the MA50 - Miss the powerplay
        # c_11 = latest["adjClose"] <= latest["ma50"] * 1.3
        c_11 = True  # Disable for now

        # Max Drawdown
        c_12 = True
        stock = _kline.copy()
        stock.set_index("date", inplace=True)
        stock = stock.iloc[-90:]  # Last 90 days
        stock["cummax"] = stock["adjClose"].cummax()
        stock["drawdown"] = stock["close"] / stock["cummax"] - 1
        max_drawdown = stock["drawdown"].min()
        if max_drawdown < -0.35:  # 35% Max drawdown
            c_12 = False

        result = (
            sum([c_1, c_2, c_3, c_4, c_5, c_6, c_7, c_8, c_9, c_10, c_11, c_12]) >= 12
        )
        logging.warning(f"Stage 2 - {self.ticker} Result: {result}")
        return result

    def _cret_minervini(self, _kline: pd.DataFrame, months: int) -> bool:
        """
        MINERVINI_xM: MA200 is trending up for at least x month
        """
        kline = _kline.copy()
        kline["ma200"] = kline["adjClose"].rolling(200).mean()
        end_date = pd.to_datetime("today").date()
        start_date = pd.to_datetime(end_date - relativedelta(months=months)).date()
        df_last_n_months = kline[
            (kline["date"] >= start_date) & (kline["date"] <= end_date)
        ]
        df_last_n_months = df_last_n_months[["ma200"]].iloc[::10, :]  # 10 days interval
        res: bool = df_last_n_months["ma200"].is_monotonic_increasing
        return res

    def _cret_days_vol(self, _kline: pd.DataFrame, days: int, vol: float) -> bool:
        """
        检查最近 n 天的波动率

        We did not use the hihn and lows, instead, use the close price.
        """
        kline = _kline.copy()
        # Get the last `days` row
        kline = kline.iloc[-days:, :]
        high = kline.close.max()
        low = kline.close.min()
        res = high / low <= 1 + vol
        return res

    def _cret_recent_low_volume(self, _kline: pd.DataFrame, days: int = 1) -> bool:
        """
        Check recent low volume
        """
        kline = _kline.copy().tail(20)
        if not kline.shape[0] < 20:  # Must have at least 20 days
            return False
        # Recent 1 days volume
        latest_avg_volume = kline["volume"].iloc[-days:].mean()
        last_5d_volume = kline["volume"].iloc[-5:].mean()
        last_10d_volume = kline["volume"].iloc[-10:].mean()
        last_20d_volume = kline["volume"].iloc[-20:].mean()

        # Cret 1 - Less than 50% of the average volume
        _c1 = latest_avg_volume < min(last_5d_volume, last_10d_volume, last_20d_volume)
        # Cret 2 - At least 200,000 Outstanding Shares - Cannot process, default True
        _c2 = True

        res = _c1 and _c2
        return res

    def _cret_qull(self, _kline: pd.DataFrame, months: int) -> bool:
        """
        Qull's screening cret.

        Args:
        - months: 1 / 3 / 6
        """
        if months not in [1, 3, 6]:
            logging.warning("Only support 1 / 3 / 6 months")
            return False
        # Get corresponding RS
        rs = 0
        if months == 1:
            rs = self.rs[1]
        elif months == 3:
            rs = self.rs[2]
        elif months == 6:
            rs = self.rs[3]

        if rs is None:
            logging.warning("RS is not available")
            return False

        if rs <= 90:
            logging.warning("RS is not good enough")
            return False

        _kline = _kline.copy()

        # Calc ADR
        _kline = _kline.iloc[-20:, :]  # Last 20 days only
        _kline.loc[:, "high_low"] = _kline["high"] / _kline["low"]
        adr = 100 * (_kline["high_low"].mean() - 1)
        # ADR Must >= 4
        if adr < 4:
            logging.warning("ADR is too low")
            return False

        # Daily Volume > 8M USD on average
        _kline.loc[:, "volume_usd"] = _kline["volume"] * _kline["close"]
        mean_volume_usd = _kline.volume_usd.mean()
        if mean_volume_usd < 8000000:
            logging.warning("Volume is too low")
            return False

        return True
