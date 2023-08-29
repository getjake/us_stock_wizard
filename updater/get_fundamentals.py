"""
Update all earning call and fundamental data to database
Run this script every day.
"""

import asyncio
import logging
from us_stock_wizard.src.fundamentals import Fundamentals


async def main():
    fundamentals = Fundamentals()
    await fundamentals.handle_earning_call_data()
    await fundamentals.update_expired_data(days_ago=5)
    logging.info("Done Earning Call and Fundamental")


if __name__ == "__main__":
    asyncio.run(main())
