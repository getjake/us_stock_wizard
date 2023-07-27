"""
Auto Add All Tickers to TradingView Watchlist
0. Define your Tradingview Watchlist ID.
1. Make sure you have `base.txt` in the same folder
2. Run this script, it will copy the script to your pasteboard
3. Go to TradingView.com, open Chrome console, paste the script and run
4. Refresh the page, you should see all tickers in your watchlist
"""
import logging
import json
import pyperclip
from us_stock_wizard.database.db_utils import StockDbUtils, DbTable

WATCHLIST_ID = "118272101"  # Change this to your watchlist ID


async def main():
    # read base.txt into a string
    with open("base.txt", "r") as myfile:
        js_template = myfile.read()
    if not js_template:
        logging.warning("No template found, quitting...")
        return
    js_template = js_template.replace("$WATCHLIST_ID$", WATCHLIST_ID)
    data = await StockDbUtils.read(table=DbTable.REPORT, output="df")
    tickers = await StockDbUtils.read(table=DbTable.TICKERS, output="df")
    # Tickers in the latest stage2 report
    data = data[data["kind"] == "PostAnalysis_stage2"]
    # Only use the latest report
    latest_tickers = data.iloc[-1]["data"]
    tickers = tickers[tickers["ticker"].isin(latest_tickers)]
    tickers["market_ticker"] = tickers["market"] + ":" + tickers["ticker"]
    ticker_exported = tickers["market_ticker"].tolist()
    logging.warning(f"We found {len(ticker_exported)} tickers")
    if not ticker_exported:
        logging.warning("No tickers found, quitting...")
        return
    _body = json.dumps(ticker_exported)
    exported_script = js_template.replace("$TICKERS$", _body)
    # Also save to pasteboard
    pyperclip.copy(exported_script)
    logging.warning(
        "Copied to pasteboard, now paste to Chrome console in Tradingview.com and run"
    )


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
