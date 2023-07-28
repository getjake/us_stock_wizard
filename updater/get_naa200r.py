"""
Update NAA200R  to database

Run this script every 1 days.
"""

import asyncio
import logging
from us_stock_wizard.src.macro import Naa200R


async def main():
    naa200r = Naa200R()
    await naa200r.initialize()
    await naa200r.analyze_all()
    await naa200r.save()
    logging.info("Done Naa200R")


if __name__ == "__main__":
    asyncio.run(main())
