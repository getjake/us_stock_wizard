"""
Please Exec it after market close on trading day
0. Get tickers
1. Trading Calendar
2. Get earning call
4. Get Fundamental
3. Get Kline
5. Get RS
6. Screen

--
Run screening.
"""

import asyncio
import logging
import pandas as pd
from us_stock_wizard.database.db_utils import StockDbUtils, DbTable
from us_stock_wizard.src.stocks import StockTickers
from us_stock_wizard.src.stocks import TradingCalendar
from us_stock_wizard.src.fundamentals import Fundamentals
from us_stock_wizard.src.kline import KlineFetch
from us_stock_wizard.screener.rs_calculator import RelativeStrengthCalculator
from us_stock_wizard.screener.daily_screener import DailyScreener
from us_stock_wizard.screener.post_analysis import PostAnalysis
from us_stock_wizard.screener.ipo_screener import IpoScreener
from us_stock_wizard.src.macro import Naa200R
from us_stock_wizard.src.common import DingTalkBot

bot = DingTalkBot()


async def check_trading_day() -> bool:
    """
    Check if today is a trading day
    """
    cal = await StockDbUtils.read(DbTable.TRADING_CALENDAR, output="df")
    cal["date"] = pd.to_datetime(cal["date"]).dt.date
    # check today in cal["date"]
    today = pd.Timestamp.today().date()
    is_today_trading = today in cal["date"].tolist()
    if not is_today_trading:
        logging.info("Today is not a trading day, skip")
        return False
    return True


async def get_tickers():
    st = StockTickers()
    await st.handle_all_tickers()
    await StockDbUtils.create_logging(
        DbTable.TICKERS, success=True, msg="Daily Routine Download Tickers Success"
    )
    logging.info("Done Ticker")


async def get_calendar():
    tc = TradingCalendar()
    await tc.handle_calendar()
    await StockDbUtils.create_logging(
        DbTable.TRADING_CALENDAR,
        success=True,
        msg="Daily Routine Download Trading Calendar Success",
    )
    logging.info("Done Calendar")


async def get_earning_call_fundamentals():
    fundamentals = Fundamentals()
    await fundamentals.handle_earning_call_data()
    await fundamentals.update_is_data(days_ago=5)
    await StockDbUtils.create_logging(
        DbTable.FUNDAMENTALS,
        success=True,
        msg="Daily Routine Download Earning Call and Fundamental Success",
    )
    logging.info("Done Earning Call and Fundamental")


async def get_kline():
    kf = KlineFetch()
    await kf.initialize()
    await kf.update_all_tickers()
    await StockDbUtils.create_logging(
        DbTable.DAILY_KLINE,
        success=True,
        msg="Daily Routine Download Kline Success",
    )
    logging.info("Done Kline")


async def get_rs():
    rs = RelativeStrengthCalculator()
    await rs.initialize()
    await rs.update_all_rs()
    await StockDbUtils.create_logging(
        DbTable.RELATIVE_STRENGTH,
        success=True,
        msg="Daily Routine Download RS Success",
    )


async def screen():
    ds = DailyScreener()
    await ds.initialize()
    await ds.screen_all()
    await ds.save()
    await StockDbUtils.create_logging(
        DbTable.DAILY_SCREENING,
        success=True,
        msg="Daily Routine Download Screening Success",
    )
    logging.info("Done Screening")


async def screen_ipo():
    screener = IpoScreener()
    await screener.initialize()
    await screener.screen_all()
    await screener.save()
    await StockDbUtils.create_logging(
        DbTable.IPO_SCREENING,
        success=True,
        msg="Daily Routine Download IPO Screening Success",
    )
    logging.info("Done IPO Screening")


async def get_naa200r():
    naa200r = Naa200R()
    await naa200r.initialize()
    await naa200r.analyze_all()
    await naa200r.save()
    await naa200r.export_image()
    await StockDbUtils.create_logging(
        DbTable.NAA200R,
        success=True,
        msg="Daily Routine Download NAA200R Success",
    )


async def run_post_analysis():
    pa = PostAnalysis()
    await pa.analyze_all()
    await StockDbUtils.create_logging(
        "DailyPostAnalysis",
        success=True,
        msg="Daily Routine Download Post Analysis Success",
    )


async def main():
    try:
        is_trading_day = await check_trading_day()
        if not is_trading_day:
            return
        await bot.send_msg("US-Stock-Wizard is updating right now...")
        await get_tickers()
        await get_calendar()
        await get_kline()
        await get_rs()
        await screen()
        await screen_ipo()
        await run_post_analysis()
        await bot.send_msg("US-Stock-Wizards Post analysis completed!")

    except Exception as e:
        err = f"US-Stock-Wizard all-in-one Error: {e}"
        await bot.send_msg(err)


if __name__ == "__main__":
    asyncio.run(main())
