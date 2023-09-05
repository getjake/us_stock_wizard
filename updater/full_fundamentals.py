"""
Update FULL fundamental data to database. 
Run this script every 7 days.
"""

import asyncio
import logging
from us_stock_wizard.src.fundamentals import Fundamentals


async def main():
    fundamentals = Fundamentals()
    await fundamentals.handle_earning_call_data()
    await fundamentals.handle_all_is_data(filter="null")
    logging.info("Done Full Earning Call and Fundamental")


if __name__ == "__main__":
    asyncio.run(main())
