import datetime
import pandas as pd
from us_stock_wizard.database.db_utils import StockDbUtils, DbTable
from us_stock_wizard.screener.rs_calculator import RelativeStrengthCalculator


async def main():
    rsc = RelativeStrengthCalculator()
    await rsc.initialize()
    cal = await StockDbUtils.read(DbTable.TRADING_CALENDAR, output="df")
    all_trading_dates = cal["date"].sort_values(ascending=True).tolist()
    all_trading_dates = [pd.Timestamp(d).date() for d in all_trading_dates]

    res = await StockDbUtils.read_groupby(DbTable.RELATIVE_STRENGTH, ["date"])
    dates = [pd.Timestamp(d["date"]).date() for d in res]
    dates_todo = list(set(all_trading_dates) - set(dates))
    dates_todo = [d for d in sorted(dates_todo) if d <= datetime.date.today()]

    count = 0
    for _date in dates_todo:
        print(f"Calculating RS for {_date}")
        await rsc.update_all_rs(_date)
        count += 1

    print(f"Calculated RS for {count} dates")


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
