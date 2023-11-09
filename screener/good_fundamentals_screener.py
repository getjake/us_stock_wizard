"""
Filter the stocks with good fundamentals
Run this script every week
"""
import logging

logging.basicConfig(level=logging.INFO)
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


class GoodFundamentalsScreener:
    """
    Good Fundamentals Screener


    Example:
    >>> screener = GoodFundamentalsScreener()
    >>> await screener.run()

    """

    def __init__(self) -> None:
        self.fs = FundamentalScreener()
        self.date = datetime.date.today()
        self.good_fundamental_stocks: List[str] = []

    async def run(self) -> None:
        await self.fs.initialize()
        self.good_fundamental_stocks = await self.fs.filter_fundamentals()
        if not self.good_fundamental_stocks:
            logging.warning("No good fundamental stocks found")
            return

        # Save the result to database
        _ = {
            "date": pd.to_datetime(self.date),
            "kind": "GoodFundamentals",
            "data": Json(self.good_fundamental_stocks),
        }
        await StockDbUtils.insert(table=DbTable.REPORT, data=[_])


if __name__ == "__main__":
    gfs = GoodFundamentalsScreener()
    asyncio.run(gfs.run())
