"""
Update FULL fundamental data to database. 
Run this script every 7 days.
"""

import asyncio
import logging
from us_stock_wizard.src.fundamentals import Fundamentals
from us_stock_wizard.screener.good_fundamentals_screener import GoodFundamentalsScreener


async def main():
    fundamentals = Fundamentals()
    await fundamentals.handle_earning_call_data()
    await fundamentals.handle_all_is_data(filter="all")
    gfs = GoodFundamentalsScreener()
    await gfs.run()
    logging.info("Done Full Earning Call and Fundamental")


if __name__ == "__main__":
    asyncio.run(main())
