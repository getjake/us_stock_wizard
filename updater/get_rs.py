import asyncio
from us_stock_wizard.screener.rs_calculator import RelativeStrengthCalculator


async def main():
    rs = RelativeStrengthCalculator()
    await rs.initialize()
    await rs.update_all_rs()


if __name__ == "__main__":
    asyncio.run(main())
