"""
1. Update all earning call and fundamental data to database
2. Update Stock Ticker Blank Fields for better Post Analysis

Run this script every day.
"""

import asyncio
import logging
from us_stock_wizard.src.fundamentals import Fundamentals
from us_stock_wizard.src.stocks import StockTickers


async def main():
    fundamentals = Fundamentals()
    await fundamentals.handle_earning_call_data()
    await fundamentals.update_expired_data(days_ago=5)
    logging.info("Done Earning Call and Fundamental")

    # Update Stock Ticker Blank Fields for better Post Analysis
    st = StockTickers()
    await st.update_blank_fields()


if __name__ == "__main__":
    asyncio.run(main())
