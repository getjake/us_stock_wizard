import asyncio
import pandas as pd
import logging
from us_stock_wizard.screener.daily_screener import DailyScreener
from us_stock_wizard.database.db_utils import StockDbUtils, DbTable


async def check_trading_day() -> bool:
    cal = await StockDbUtils.read(DbTable.TRADING_CALENDAR, output="df")
    cal["date"] = pd.to_datetime(cal["date"]).dt.date
    # check today in cal["date"]
    today = pd.Timestamp.today().date()
    is_today_trading = today in cal["date"].tolist()
    if not is_today_trading:
        logging.warning("Today is not a trading day, skip")
    return is_today_trading


async def main():
    # is_today_trading = await check_trading_day()
    # if not is_today_trading:
    #     return
    screener = DailyScreener()
    await screener.initialize()
    await screener.screen_all()
    await screener.save()


if __name__ == "__main__":
    asyncio.run(main())
