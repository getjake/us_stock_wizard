"""
Quick setup after cloning the repository.

0. Get all tickers.
1. Get trading calendar.
2. Get all kline data.
3. Get fundamental.
4. Get all RS data.
"""
import gc
import asyncio
import logging
import pandas as pd
from us_stock_wizard.src.stocks import StockTickers
from us_stock_wizard.src.stocks import TradingCalendar
from us_stock_wizard.src.kline import KlineFetch
from us_stock_wizard.src.fundamentals import Fundamentals
from us_stock_wizard.screener.good_fundamentals_screener import GoodFundamentalsScreener
from us_stock_wizard.screener.rs_calculator import RelativeStrengthCalculator


async def main():
    # 0. Get all tickers.
    st = StockTickers()
    await st.handle_all_tickers()
    del st
    gc.collect()

    # 1. Get trading calendar.
    tc = TradingCalendar()
    await tc.handle_calendar()
    del tc
    gc.collect()

    # 2. Get all kline data.
    kf = KlineFetch()
    await kf.initialize()
    succ, fail = await kf.refetch_abnormal_tickers()
    del kf
    gc.collect()

    # 3. Get fundamental.
    fundamentals = Fundamentals()
    await fundamentals.handle_earning_call_data()
    await fundamentals.handle_all_is_data(filter="all")

    del fundamentals
    gc.collect()

    gfs = GoodFundamentalsScreener()
    await gfs.run()
    logging.info("Done Full Earning Call and Fundamental")
    del gfs
    gc.collect()

    rs = RelativeStrengthCalculator()
    await rs.initialize()
    today = pd.Timestamp.today().date()
    start = today - pd.Timedelta(days=365 * 5)
    await rs.batch_get_all_rs(start, today)


if __name__ == "__main__":
    asyncio.run(main())
