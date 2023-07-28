from typing import Optional, List
import logging
import datetime
import pandas as pd
from us_stock_wizard.database.db_utils import StockDbUtils, DbTable

logging.basicConfig(level=logging.INFO)


class FundamentalMeasurements:
    CRITERIA_1 = "cret_1"


class FundamentalAnalyzer:
    """

    Usage:
    >>> analyzer = FundamentalAnalyzer(ticker="AAPL")
    >>> await analyzer.get_fundamental()
    >>> res: bool = analyzer.get_result([FundamentalMeasurements.CRITERIA_1])

    """

    def __init__(self, ticker: str) -> None:
        self.ticker = ticker
        self.db_utils = StockDbUtils()
        self.data: Optional[pd.DataFrame] = None

    async def get_fundamental(self) -> None:
        """
        Get fundamental data from database
        """
        fundamental = await StockDbUtils.read(
            DbTable.FUNDAMENTALS, where={"ticker": self.ticker}, output="df"
        )
        self.data = fundamental

    def analyze(self) -> pd.DataFrame:
        """
        Analyze fundamental data
        """
        if self.data is None:
            logging.info("No fundamental data found")
            return
        fundamental = self.data.copy()
        # Handle Quarterly Report only
        fundamental["reportDate"] = pd.to_datetime(fundamental["reportDate"])
        fundamental = fundamental[fundamental["reportType"] == "QUARTERLY"]
        fundamental = fundamental.sort_values(by="reportDate", ascending=True)

        # Calculate the YoY increasement
        fundamental["sales_YoY"] = fundamental["sales"].pct_change(periods=4)
        fundamental["netIncome_YoY"] = fundamental["netIncome"].pct_change(periods=4)
        fundamental["grossMarginRatio_YoY"] = fundamental[
            "grossMarginRatio"
        ].pct_change(periods=4)

        # Calculate the QoQ increasement
        fundamental["sales_QoQ"] = fundamental["sales"].pct_change(periods=1)
        fundamental["netIncome_QoQ"] = fundamental["netIncome"].pct_change(periods=1)
        fundamental["grossMarginRatio_QoQ"] = fundamental[
            "grossMarginRatio"
        ].pct_change(periods=1)

        return fundamental

    def get_result(self, criteria: List[FundamentalMeasurements]) -> bool:
        """
        Get result of fundamental analysis
        """
        if self.data is None:
            return False
        combined_bool = []
        for cret in criteria:
            if cret == FundamentalMeasurements.CRITERIA_1:
                combined_bool.append(self.cret_1())
            else:
                raise ValueError(f"Unknown criteria: {cret}")
        return all(combined_bool)

    def cret_1(self) -> bool:
        """
        Criteria
        1. Sales YoY growth rate > 20%
        2. netIncome YoY growth rate > 20%
        """
        data = self.analyze()
        if data.empty:
            logging.warning("No fundamental data found")
            return False

        sales_yoy = data.iloc[-1]["sales_YoY"] > 0.2
        netIncome_yoy = data.iloc[-1]["netIncome_YoY"] > 0.2

        _ = sales_yoy and netIncome_yoy
        return _


class FundamentalScreener:
    """
    Screen a list of stocks based on fundamental analysis

    Example:
        >>> screener = FundamentalScreener()
        >>> await screener.initialize()
    """

    def __init__(self) -> None:
        self.stocks_df = pd.DataFrame()

        self.preferred_keywords = [
            "tech",
            "bio",
            "health",
            "pharm",
            "software",
            "medic",
            "consumer",
        ]
        self.blacklist_keywords = [
            "bank",
            "insurance",
            "estate",
            "oil",
            "energy",
            "gas",
            "power",
        ]

    async def initialize(self):
        await self.get_stocks()

    async def get_stocks(self) -> None:
        """
        Get stocks!
        """
        all_stocks = await StockDbUtils.read(table=DbTable.TICKERS, output="df")
        all_stocks = all_stocks[all_stocks["ticker"].str.len() < 5]
        self.stocks_df = all_stocks

    def filter_preferred(self) -> List[str]:
        """
        Return the preferred stocks
        """
        if self.stocks_df.empty:
            raise ValueError("No stocks in the dataframe!")
        df = self.stocks_df.copy()
        df["joint_kw"] = df["sector"] + " " + df["industry"]
        df["joint_kw"] = df["joint_kw"].str.lower()
        pattern = "|".join(self.preferred_keywords)
        df["contains_preferred"] = df["joint_kw"].str.contains(pattern)
        df = df[df["contains_preferred"] == True]
        return df["ticker"].tolist()

    def filter_blacklist(self) -> List[str]:
        """
        Return the blacklist stocks
        """
        if self.stocks_df.empty:
            raise ValueError("No stocks in the dataframe!")
        df = self.stocks_df.copy()
        df["joint_kw"] = df["sector"] + " " + df["industry"]
        df["joint_kw"] = df["joint_kw"].str.lower()
        pattern = "|".join(self.blacklist_keywords)
        df["contains_blacklist"] = df["joint_kw"].str.contains(pattern)
        df = df[df["contains_blacklist"] == True]
        return df["ticker"].tolist()
