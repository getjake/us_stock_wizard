"""
Update all ticker data to database
"""

import asyncio
import logging
from us_stock_wizard.src.stocks import StockTickers


if __name__ == "__main__":
    st = StockTickers()
    asyncio.run(st.handle_all_tickers())
    logging.info("Done")
