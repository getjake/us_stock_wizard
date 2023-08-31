"""
Reload all abnormal kline data to database.
- Refetch them and update to database
"""

import asyncio
import logging
from us_stock_wizard.src.kline import KlineFetch


async def main():
    kf = KlineFetch()
    succ, fail = await kf.refetch_abnormal_tickers()


if __name__ == "__main__":
    asyncio.run(main())
