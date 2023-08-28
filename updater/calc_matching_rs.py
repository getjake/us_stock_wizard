"""
Calculate the RS of all stocks in a date range, if match the criteria, save to database.
"""
import asyncio
import logging
from us_stock_wizard.screener.rs_calculator import RelativeStrengthCalculator


async def main():
    rsc = RelativeStrengthCalculator()
    await rsc.initialize()
    await rsc.export_high_rs(days_ago=90, threshold=85, lasting=10)
    logging.warning("Done for Calc matching RS.")


if __name__ == "__main__":
    asyncio.run(main())
