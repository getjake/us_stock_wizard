from prisma import Prisma
from typing import List
import pandas as pd


class DbTable(str):
    """
    Please update it in case the `schema.prisma` file modified.
    """

    TICKERS = "Tickers"
    FUNDAMENTALS = "Fundamentals"
    TRADING_CALENDAR = "TradingCalendar"
    DAILY_KLINE = "DailyKline"
    EARNING_CALL = "EarningCall"
    REPORT = "Report"
    LOGGINGS = "Loggings"
    RELATIVE_STRENGTH = "RelativeStrength"
    NAA200R = "Naa200r"


class StockDbUtils:
    @staticmethod
    def convert_to_dataframe(data: list) -> pd.DataFrame:
        """
        Convert the given data from database to dataframe
        """
        _data = [d.dict() for d in data]
        return pd.DataFrame(_data)

    @staticmethod
    async def insert(table: DbTable, data: List[dict]):
        """
        Insert list of data into a given table
        """
        db = Prisma()
        await db.connect()
        _target = getattr(db, table.lower())
        result = await _target.create_many(data, skip_duplicates=True)
        await db.disconnect()
        return result

    @staticmethod
    async def read(
        table: DbTable, where: dict = {}, output: str = "list"
    ) -> List[dict] | pd.DataFrame:
        """
        Read data from a given table

        Args:
            table (DbTable): table name
            where (dict, optional): filter condition. Defaults to {}.
            output (str, optional): output format. Defaults to "list". / "df
        """
        db = Prisma()
        await db.connect()
        _target = getattr(db, table.lower())
        result = await _target.find_many(where=where)
        if output == "list":
            _ = result
        elif output == "df":
            _ = StockDbUtils.convert_to_dataframe(result)
        await db.disconnect()
        return _

    @staticmethod
    async def update(table: DbTable, where: dict, data: dict):
        """
        Update data from a given table
        """
        db = Prisma()
        await db.connect()
        _target = getattr(db, table.lower())
        result = await _target.update_many(where=where, data=data)
        await db.disconnect()
        return result

    @staticmethod
    async def delete(table: DbTable, where: dict):
        """
        Delete data from a given table
        """
        db = Prisma()
        await db.connect()
        _target = getattr(db, table.lower())
        result = await _target.delete_many(where=where)
        await db.disconnect()
        return result
