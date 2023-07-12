"""
Update Trading Calendar to database
"""

import asyncio
import logging
from us_stock_wizard.src.stocks import TradingCalendar


if __name__ == "__main__":
    tc = TradingCalendar()
    asyncio.run(tc.handle_calendar())
    logging.info("Done")
