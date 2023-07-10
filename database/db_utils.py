import asyncio
from prisma import Prisma
from typing import List


class StockDbUtils:
    @staticmethod
    async def insert(table: str, data: List[dict]):
        """
        Insert list of data into a given table
        """
        db = Prisma()
        await db.connect()

        _target = getattr(db, table)
        result = await _target.create_many(data)
        await db.disconnect()
        return result
