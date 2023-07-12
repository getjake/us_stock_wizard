"""
Update all fundamental data to database
"""

import asyncio
import logging
from us_stock_wizard.src.fundamentals import Fundamentals


if __name__ == "__main__":
    fundamentals = Fundamentals()
    asyncio.run(fundamentals.handle_all_is_data())
    logging.info("Done")
