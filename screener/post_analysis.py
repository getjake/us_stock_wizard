"""
Post Analysis to get what we really want!
"""
import logging
import asyncio
import tempfile
from collections import defaultdict
from prisma import Json
from typing import Optional, List, Dict
import datetime
import pandas as pd
from us_stock_wizard.database.db_utils import StockDbUtils, DbTable
from us_stock_wizard.screener.fundamental_analyzer import FundamentalScreener
from us_stock_wizard.src.common import create_xlsx_file
from us_stock_wizard.src.gdrive_utils import GoogleDriveUtils


class PostAnalysis:
    """
    Post Analysis and generate a report, and upload it into Google Drive

    Example:
        pa = PostAnalysis()
        await pa.analyze_all()
    """

    def __init__(self, date: Optional[datetime.date] = None) -> None:
        self.gdrive = GoogleDriveUtils()
        self.date = date or datetime.date.today()
        self.date_str = self.date.strftime("%Y-%m-%d")
        self.date_ts = pd.Timestamp(self.date)
        self.to_save: Dict[str, pd.DataFrame] = defaultdict(lambda: pd.DataFrame())
        self.fs = FundamentalScreener()
        self.preferred_stocks = []
        self.blacklist_stocks = []

    async def analyze_all(self) -> Dict[str, pd.DataFrame]:
        """
        Analyze all and save to Google Drive

        Customize it!
        """
        # Sector Filter
        await self.fs.initialize()
        self.preferred_stocks = self.fs.filter_preferred()
        self.blacklist_stocks = self.fs.filter_blacklist()

        await self.analyze_stage2()
        await self.analyze_stage2_diff()

        # Disable them for now. bc. useless somehow.
        # await self.analyze_stage2_high_rs()
        # await self.analyze_stage2_rs_newborn()
        await self.analyze_stage2_good_fundamentals()
        await self.analyze_ipo()

        # Create excel file to tempdir
        with tempfile.TemporaryDirectory() as tempdir:
            file_path = f"{tempdir}/report.xlsx"
            create_xlsx_file(self.to_save, file_path)
            self.gdrive.upload(file_path, f"PostAnalysis_{self.date_str}.xlsx")

    async def analyze_stage2(self) -> None:
        """
        - Stage 2 Analysis
        - Low volatility in recent 7 days.
        """
        tickers = await StockDbUtils.read(DbTable.TICKERS, output="df")
        tickers = tickers[["ticker", "sector", "industry"]]

        rs = await StockDbUtils.read(
            DbTable.RELATIVE_STRENGTH, where={"date": self.date_ts}, output="df"
        )
        rs["date"] = pd.to_datetime(rs["date"]).dt.date

        # fundamental
        fundamentals = await StockDbUtils.read(DbTable.FUNDAMENTALS, output="df")

        report = await StockDbUtils.read(
            DbTable.REPORT, where={"date": self.date_ts}, output="df"
        )
        # report stage2 data
        stage2_stocks: List[str] = report.loc[
            report["kind"] == "stage2", "data"
        ].values[0]
        # low volatility data
        low_volatility_stocks: List[str] = report.loc[
            report["kind"] == "seven_day_low_volatility", "data"
        ].values[0]

        # intersec
        stage2_low_volatility_stocks = list(
            set(stage2_stocks) & set(low_volatility_stocks)
        )

        # left join stage2_low_volatility_stocks - tickers
        stage2_low_volatility_stocks = pd.DataFrame(
            stage2_low_volatility_stocks, columns=["ticker"]
        )

        # Not filtered by Low Volatility
        _stage2_stocks = pd.DataFrame(stage2_stocks, columns=["ticker"])

        # stage2_low_volatility_stocks -> Low
        # _stage2_stocks -> Original
        stage2_overview = _stage2_stocks.merge(tickers, on="ticker", how="left")
        # join rs
        stage2_overview = stage2_overview.merge(rs, on="ticker", how="left")
        # sort by rs desc
        stage2_overview = stage2_overview.sort_values(by="rscore", ascending=False)
        stage2_overview = stage2_overview.drop(columns=["id", "date"])  # @DEBUG
        quarterly_data = fundamentals[fundamentals["reportType"] == "QUARTERLY"]
        latest_quarterly_data = (
            quarterly_data.sort_values(["ticker", "reportDate"])
            .groupby("ticker")
            .last()
            .reset_index()
        )

        # latest_quarterly_data
        # only preserve `ticker` , `sales`, `netIncome`, `grossMarginRatio` columns
        latest_quarterly_data = latest_quarterly_data[
            ["ticker", "sales", "netIncome", "grossMarginRatio"]
        ]

        stage2_overview = stage2_overview.merge(
            latest_quarterly_data, on="ticker", how="left"
        )

        # Filter by preferred_stocks
        stage2_overview_preferred = stage2_overview[
            stage2_overview["ticker"].isin(self.preferred_stocks)
        ]

        # Filter by blacklist_stocks (Remove them from tickers)
        stage2_overview_non_blacklist = stage2_overview[
            ~stage2_overview["ticker"].isin(self.blacklist_stocks)
        ]

        # Also save result to database
        tickers = stage2_overview["ticker"].values.tolist()
        tickers_preferred = stage2_overview_preferred["ticker"].values.tolist()
        tickers_non_blacklist = stage2_overview_non_blacklist["ticker"].values.tolist()

        _ = {
            "date": pd.to_datetime(self.date),
            "kind": "PostAnalysis_stage2",
            "data": Json(tickers),
        }
        await StockDbUtils.insert(table=DbTable.REPORT, data=[_])

        _ = {
            "date": pd.to_datetime(self.date),
            "kind": "PostAnalysis_stage2_preferred",
            "data": Json(tickers_preferred),
        }
        await StockDbUtils.insert(table=DbTable.REPORT, data=[_])

        _ = {
            "date": pd.to_datetime(self.date),
            "kind": "PostAnalysis_stage2_non_blacklist",
            "data": Json(tickers_non_blacklist),
        }

        await StockDbUtils.insert(table=DbTable.REPORT, data=[_])

        # Save to self.to_save
        self.to_save["stage2"] = stage2_overview
        self.to_save["stage2_preferred"] = stage2_overview_preferred
        self.to_save["stage2_non_blacklist"] = stage2_overview_non_blacklist
        return

    async def analyze_stage2_good_fundamentals(self) -> None:
        """
        Filter the stage2 stocks with good fundamentals
        """
        # Good fundamentals
        good_fundamental_stocks = await StockDbUtils.read_first(
            DbTable.REPORT,
            where={"kind": "GoodFundamentals"},
        )
        if not good_fundamental_stocks:
            # Not available, do thing.
            logging.warning(f"No GoodFundamentals data. Skip.")
            return
        _stocks: List[str] = good_fundamental_stocks["data"]
        if not _stocks:
            logging.warning(f"No GoodFundamentals data. Skip.")
            return

        # Stage2 Good Fundamentals
        _stage2 = self.to_save["stage2"].copy()
        _stage2 = _stage2[_stage2["ticker"].isin(_stocks)]

        # Stage2GF tickers
        _stage2_tickers = _stage2["ticker"].tolist()

        # Insert database
        _ = {
            "date": pd.to_datetime(self.date),
            "kind": "PostAnalysis_stage2_GoodFundamentals",
            "data": Json(_stage2_tickers),
        }

        await StockDbUtils.insert(table=DbTable.REPORT, data=[_])

        self.to_save["stage2_good_fundamentals"] = _stage2

    async def analyze_stage2_diff(self) -> None:
        """
        把stage2的数据和上一次的数据对比, 看看哪些股票是新的
        """
        # Get stage2 data
        _date = self.date_ts - pd.Timedelta(days=15)  # Less data, faster
        stage2s: pd.DataFrame = await StockDbUtils.read(
            table=DbTable.REPORT,
            where={"kind": "PostAnalysis_stage2", "date": {"gt": _date}},
            output="df",
        )
        stage2s = stage2s.sort_values(by="date", ascending=True)
        stage2s = stage2s.tail(2)
        if stage2s.shape[0] < 2:
            print("Not enough data to compare. Skip.")
            return pd.DataFrame()

        latest: List[str] = stage2s.iloc[-1]["data"]
        previous: List[str] = stage2s.iloc[-2]["data"]
        diff = list(set(latest) - set(previous))
        if len(diff) == 0:
            print("No diff. Skip.")

        # Save to databse
        _ = {
            "date": pd.to_datetime(self.date),
            "kind": "PostAnalysis_stage2_diff",
            "data": Json(diff),
        }
        await StockDbUtils.insert(table=DbTable.REPORT, data=[_])

        if self.to_save["stage2"].empty:
            print("No stage2 data. Skip.")
            return pd.DataFrame()

        _df = self.to_save["stage2"].copy()
        _df = _df[_df["ticker"].isin(diff)]
        self.to_save["stage2_diff"] = _df
        return

    async def analyze_stage2_high_rs(self) -> None:
        """
        Analyze stage2 high RS stocks
        """
        # High RS stock list
        _ = await StockDbUtils.read_first(
            DbTable.REPORT,
            where={"kind": "high_rs"},
        )
        if not _:
            logging.warning("No high_rs data. Skip.")
            return
        high_rs: List[str] = _["data"]
        if not high_rs:
            logging.warning("No high_rs data in list. Skip.")
            return

        stage2 = self.to_save["stage2"]
        stage2_high_rs = stage2[stage2["ticker"].isin(high_rs)]

        _high_rs = stage2_high_rs["ticker"].tolist()
        _ = {
            "date": pd.to_datetime(self.date),
            "kind": "PostAnalysis_stage2_high_rs",
            "data": Json(_high_rs),
        }
        await StockDbUtils.insert(table=DbTable.REPORT, data=[_])

        self.to_save["stage2_high_rs"] = stage2_high_rs
        return

    async def analyze_stage2_rs_newborn(self) -> None:
        """
        Analyze stage2 new-born high RS stocks
        """
        # New-born High RS stock list
        _ = await StockDbUtils.read_first(
            DbTable.REPORT,
            where={"kind": "newborn_rs"},
        )
        if not _:
            logging.warning("No newborn_rs data. Skip.")
            return
        high_rs: List[str] = _["data"]
        if not high_rs:
            logging.warning("No new-born high_rs data in list. Skip.")
            return

        stage2 = self.to_save["stage2"]
        stage2_nb_highrs = stage2[stage2["ticker"].isin(high_rs)]

        _high_rs = stage2_nb_highrs["ticker"].tolist()
        _ = {
            "date": pd.to_datetime(self.date),
            "kind": "PostAnalysis_stage2_nb_high_rs",
            "data": Json(_high_rs),
        }
        await StockDbUtils.insert(table=DbTable.REPORT, data=[_])

        self.to_save["stage2_newborn_high_rs"] = stage2_nb_highrs
        return

    async def analyze_ipo(self) -> None:
        """
        Analyze IPO stocks
        """
        tickers = await StockDbUtils.read(DbTable.TICKERS, output="df")
        tickers = tickers[["ticker", "sector", "industry"]]

        _date = self.date_ts - pd.Timedelta(days=15)  # Less data, faster
        ipos: pd.DataFrame = await StockDbUtils.read(
            table=DbTable.REPORT,
            where={"kind": "ipo", "date": {"gt": _date}},
            output="df",
        )
        ipos = ipos.sort_values(by="date", ascending=True)
        ipo_stocks = ipos.iloc[-1]["data"]
        result = tickers[tickers["ticker"].isin(ipo_stocks)]
        # Save to self.to_save
        self.to_save["ipo"] = result
        return


async def main():
    today = datetime.date.today()
    pa = PostAnalysis(today)
    await pa.analyze_all()
    print("Done!")


if __name__ == "__main__":
    asyncio.run(main())
