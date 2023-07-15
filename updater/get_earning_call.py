"""
Update Earning Call data to database

Update this script every 7 days.
"""

import asyncio
import logging
from us_stock_wizard.src.fundamentals import Fundamentals


if __name__ == "__main__":
    fundamentals = Fundamentals()
    asyncio.run(fundamentals.handle_earning_call_data())
    logging.info("Done")
