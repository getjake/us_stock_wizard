import datetime
from typing import List, Optional
from prisma import Prisma, Json
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
    async def read_first(
        table: DbTable, where: dict = {}, date_arg: str = "date"
    ) -> Optional[dict]:
        """
        Find the first entry from a given table

        Args:
            table (DbTable): table name
            where (dict, optional): filter condition. Defaults to {}.
            date_arg (str, optional): date column name. Defaults to "date". Will fetch entry with latest date.
        """
        db = Prisma()
        await db.connect()
        _target = getattr(db, table.lower())
        res = await _target.find_first(where=where, order={date_arg: "desc"})
        if not res:
            return None
        return res.dict()

    @staticmethod
    async def read_groupby(table: DbTable, group_by: List[str]) -> List[dict]:
        """
        Read data from a given table and groupby ..

        Args:
            table (DbTable): table name
            group_by (List[str]): groupby columns
        """
        db = Prisma()
        await db.connect()
        _target = getattr(db, table.lower())
        result = await _target.group_by(group_by)
        await db.disconnect()
        return result

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

    @staticmethod
    async def create_logging(table: DbTable, success: bool, msg: str):
        """
        Create a logging entry
        """
        db = Prisma()
        await db.connect()
        _ = {"category": table, "data": Json({"success": success, "msg": msg})}
        result = await db.loggings.create(_)
        await db.disconnect()
        return result


class DbCleaner:
    @staticmethod
    async def remove_acquisitions():
        """
        Remove all acquisitions firms
        """
        data = await StockDbUtils.read(table=DbTable.TICKERS, output="df")
        data["acquisition"] = data["name"].apply(lambda x: "acquisition" in x.lower())
        # count the number of acquisitions
        data["acquisition"].value_counts()
        # fitler the tickers
        data = data[data["acquisition"] == True]
        # tickers
        tickers = data.ticker.values.tolist()

        for ticker in tickers:
            await StockDbUtils.delete(
                table=DbTable.DAILY_KLINE, where={"ticker": ticker}
            )
            await StockDbUtils.delete(
                table=DbTable.EARNING_CALL, where={"ticker": ticker}
            )
            await StockDbUtils.delete(
                table=DbTable.FUNDAMENTALS, where={"ticker": ticker}
            )
            await StockDbUtils.delete(table=DbTable.TICKERS, where={"ticker": ticker})

    @staticmethod
    async def remove_klines(days_ago: int = 400):
        """
        Remove outdated ohlc data to save space.
        """
        result = await StockDbUtils.read_groupby(DbTable.DAILY_KLINE, group_by=["date"])
        result = sorted(result, key=lambda x: x["date"])
        result = [
            x
            for x in result
            if x["date"]
            < (datetime.datetime.now() - datetime.timedelta(days=days_ago)).strftime(
                "%Y-%m-%d"
            )
        ]

        for _date in result:
            number = await StockDbUtils.delete(
                DbTable.DAILY_KLINE, {"date": _date["date"]}
            )
            print(f"Remove {_date['date']} with {number} rows.")
