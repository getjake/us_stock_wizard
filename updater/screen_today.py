import asyncio
from us_stock_wizard.screener.daily_screener import DailyScreener


async def main():
    screener = DailyScreener()
    await screener.initialize()
    await screener.screen_all()
    await screener.save()


if __name__ == "__main__":
    asyncio.run(main())
