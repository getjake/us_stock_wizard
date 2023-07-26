"""
Auto Add All Tickers to TradingView Watchlist
0. Define your Tradingview Watchlist ID.
1. Make sure you have `base.txt` in the same folder
2. Run this script, it will copy the script to your pasteboard
3. Go to TradingView.com, open Chrome console, paste the script and run
"""
import json
import pyperclip
from us_stock_wizard.database.db_utils import StockDbUtils, DbTable

WATCHLIST_ID = "118272101"


async def main():
    # read base.txt into a string
    with open("base.txt", "r") as myfile:
        js_template = myfile.read()
    if not js_template:
        print("No template found, quitting...")
        return
    js_template = js_template.replace("$WATCHLIST_ID$", WATCHLIST_ID)
    data = await StockDbUtils.read(table=DbTable.REPORT, output="df")
    tickers = await StockDbUtils.read(table=DbTable.TICKERS, output="df")
    data = data[data["kind"] == "PostAnalysis_stage2"]
    latest_tickers = data.iloc[-1]["data"]
    tickers = tickers[tickers["ticker"].isin(latest_tickers)]
    tickers["market_ticker"] = tickers["market"] + ":" + tickers["ticker"]
    ticker_exported = tickers["market_ticker"].tolist()
    print(f"We found {len(ticker_exported)} tickers")
    if not ticker_exported:
        print("No tickers found, quitting...")
        return
    _body = json.dumps(ticker_exported)
    exported_script = js_template.replace("$TICKERS$", _body)
    # Also save to pasteboard
    pyperclip.copy(exported_script)
    print(
        "Copied to pasteboard, now paste to Chrome console in Tradingview.com and run"
    )


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
