"""
Update all kline data to database
"""

import asyncio
import logging
from us_stock_wizard.src.kline import KlineFetch


if __name__ == "__main__":
    kf = KlineFetch()

    asyncio.run(kf.initialize())
    asyncio.run(kf.update_all_tickers())
    logging.info("Done")
