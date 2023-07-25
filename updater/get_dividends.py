"""
Update Dividend / Split to database

Run this script on the trading days before market opens.
"""

import asyncio
import logging
from us_stock_wizard.src.stocks import StockDividends


async def main():
    sd = StockDividends()
    await sd.initialize()
    sd.handle_all()
    await sd.save_to_db()


if __name__ == "__main__":
    asyncio.run(main())
    logging.info("Done")
