"""
Calculate the RS of all stocks in a date range, if match the criteria, save to database.
"""
import asyncio
import logging
from us_stock_wizard.screener.rs_calculator import RelativeStrengthCalculator
from us_stock_wizard.updater.all_in_one import check_trading_day


async def main():
    is_trading_day = await check_trading_day()
    if not is_trading_day:
        return
    rsc = RelativeStrengthCalculator()
    await rsc.initialize()
    await rsc.export_high_rs(days_ago=60, threshold=90, lasting=10)
    await rsc.export_new_born()
    logging.warning("Done for Calc matching RS.")


if __name__ == "__main__":
    asyncio.run(main())
