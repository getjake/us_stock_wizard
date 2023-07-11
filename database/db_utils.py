import asyncio
from prisma import Prisma
from typing import List
import pandas as pd


class DbTable:
    TICKERS = "Tickers"
    FUNDAMENTALS = "Fundamentals"
    TRADING_CALENDAR = "TradingCalendar"
    DAILY_KLINE = "DailyKline"


class StockDbUtils:
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

    async def read(table: DbTable, where: dict = {}) -> List[dict]:
        """
        Read data from a given table
        """
        db = Prisma()
        await db.connect()
        _target = getattr(db, table.lower())
        result = await _target.find_many(where=where)
        await db.disconnect()
        return result

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
