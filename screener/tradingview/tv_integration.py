"""
TradingView Integration

- Auto Add All Tickers to TradingView Watchlist
"""

import urllib
import os
import logging
import json
from typing import Tuple, List, Dict
import pyperclip
from us_stock_wizard import StockRootDirectory
from us_stock_wizard.database.db_utils import StockDbUtils, DbTable


class TradingViewIntegration:
    """
    TradingView Integration for Watch List

    >>> from us_stock_wizard.screener.tradingview.tv_integration import TradingViewIntegration
    >>> tv = TradingViewIntegration()
    >>> await tv.handle_all()
    """

    def __init__(self) -> None:
        self.root = StockRootDirectory().root_dir()
        self.curr_dir = os.path.dirname(os.path.abspath(__file__))
        self.config: Dict[str, int] = {}
        self.js_template = ""
        self.load()

    def load(self) -> None:
        # Load config
        _ = os.path.join(self.root, "tv-config.json")
        if not os.path.exists(_):
            raise FileNotFoundError("tv-config.json not found")
        with open(_, "r") as f:
            self.config = json.load(f)

        # Load Template
        _ = os.path.join(self.curr_dir, "template.js")
        if not os.path.exists(_):
            raise FileNotFoundError("template.js not found")
        with open(_, "r") as f:
            self.js_template = f.read()

    async def get_data(self, kind: str) -> List[str]:
        """
        Get data from database
        """
        data = await StockDbUtils.read(table=DbTable.REPORT, output="df")
        tickers = await StockDbUtils.read(table=DbTable.TICKERS, output="df")
        data = data[data["kind"] == kind]

        latest_tickers = data.iloc[-1]["data"]
        tickers = tickers[tickers["ticker"].isin(latest_tickers)]
        tickers["market_ticker"] = tickers["market"] + ":" + tickers["ticker"]
        ticker_exported = tickers["market_ticker"].tolist()
        return ticker_exported

    async def handle_category(self, kind: str, id: int) -> str:
        tickers: List[str] = await self.get_data(kind)
        _id = str(id)
        _body = json.dumps(tickers)
        exported_script = self.js_template.replace("$TICKERS$", _body).replace(
            "$WATCHLIST_ID$", _id
        )
        return exported_script

    async def handle_all(self) -> str:
        input(
            "Please make sure your clear all watchlist first, then press enter to continue, or Ctrl-C to quit"
        )
        _ = ""
        for kind, tv_watchlist_id in self.config.items():
            logging.warning(f"Exporting {kind} to watchlist {tv_watchlist_id}")
            _ += await self.handle_category(kind, tv_watchlist_id)
            _ += "\n\n"
            logging.warning(f"Exported {kind} to watchlist {tv_watchlist_id}")

        # Also save to pasteboard
        pyperclip.copy(_)
        logging.warning(
            "Copied to pasteboard, now paste to Chrome console in Tradingview.com and run"
        )


if __name__ == "__main__":
    import asyncio

    asyncio.run(TradingViewIntegration().handle_all())
