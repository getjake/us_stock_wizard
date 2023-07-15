"""
Update all fundamental data to database

Run this script every 5 days.
"""

import asyncio
import logging
from us_stock_wizard.src.fundamentals import Fundamentals


if __name__ == "__main__":
    fundamentals = Fundamentals()
    asyncio.run(fundamentals.update_is_data(days_ago=5))
    logging.info("Done")
