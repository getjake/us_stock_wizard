"""
Update all earning call and fundamental data to database

Run this script every 5 days.
"""

import asyncio
import logging
from us_stock_wizard.src.fundamentals import Fundamentals


async def main():
    fundamentals = Fundamentals()
    await fundamentals.handle_earning_call_data()
    # await fundamentals.update_is_data(days_ago=5)
    await fundamentals.handle_all_is_data()
    logging.info("Done Earning Call and Fundamental")


if __name__ == "__main__":
    fundamentals = Fundamentals()
    asyncio.run(main())
    logging.info("Done")
