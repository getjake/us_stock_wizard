"""
Post Analysis to get what we really want!
"""
import asyncio
import tempfile
from prisma import Json
from typing import Optional, List, Dict
import datetime
import pandas as pd
from us_stock_wizard.database.db_utils import StockDbUtils, DbTable
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

    async def analyze_all(self) -> Dict[str, pd.DataFrame]:
        """
        Analyze all and save to Google Drive

        Customize it!
        """
        stage2 = await self.analyze_stage2()
        to_save = {"stage2": stage2}

        # Create excel file to tempdir
        with tempfile.TemporaryDirectory() as tempdir:
            file_path = f"{tempdir}/report.xlsx"
            create_xlsx_file(to_save, file_path)
            self.gdrive.upload(file_path, f"PostAnalysis_{self.date_str}.xlsx")

    async def analyze_stage2(self) -> pd.DataFrame:
        """
        - Stage 2 Analysis
        - Low volatility in recent 7 days.
        - Income > 0 in recent quarter
        """
        tickers = await StockDbUtils.read(DbTable.TICKERS, output="df")
        tickers = tickers[["ticker", "sector", "industry"]]

        # relative strength - today
        rs = await StockDbUtils.read(DbTable.RELATIVE_STRENGTH, output="df")
        rs["date"] = pd.to_datetime(rs["date"]).dt.date
        rs = rs[rs["date"] == self.date]

        # fundamental
        fundamentals = await StockDbUtils.read(DbTable.FUNDAMENTALS, output="df")

        report = await StockDbUtils.read(
            DbTable.REPORT, where={"date": self.date_ts}, output="df"
        )
        # report stage2 data
        stage2_stocks = report.loc[report["kind"] == "stage2", "data"].values[0]
        # low volatility data
        low_volatility_stocks = report.loc[
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
        stage2_overview = stage2_low_volatility_stocks.merge(
            tickers, on="ticker", how="left"
        )
        # join rs
        stage2_overview = stage2_overview.merge(rs, on="ticker", how="left")
        # sort by rs desc
        stage2_overview = stage2_overview.sort_values(by="rscore", ascending=False)
        stage2_overview = stage2_overview.drop(
            columns=["id", "date", "createdAt", "updatedAt"]
        )
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

        # join latest_quarterly_data  and stage2_overview on ticker
        stage2_overview = stage2_overview.merge(
            latest_quarterly_data, on="ticker", how="left"
        )

        # filter netIncome > 0
        stage2_ni_lt_0 = stage2_overview[stage2_overview["netIncome"] > 0]

        # Also save result to database
        tickers = stage2_ni_lt_0["ticker"].values.tolist()
        _ = {
            "date": pd.to_datetime(self.date),
            "kind": "PostAnalysis_stage2",
            "data": Json(tickers),
        }
        await StockDbUtils.insert(table=DbTable.REPORT, data=[_])
        return stage2_ni_lt_0


async def main():
    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    pa = PostAnalysis(yesterday)
    await pa.analyze_all()
    print("Done!")


if __name__ == "__main__":
    asyncio.run(main())
