"""
TradingView Integration

- Auto Add All Tickers to TradingView Watchlist
"""

import urllib
import os
import logging
import json
from typing import List, Dict
import pyperclip
from datetime import datetime
from us_stock_wizard import StockRootDirectory
from us_stock_wizard.src.common import NetworkRequests


class DataSource:
    API = "api"
    DB = "db"


class TradingViewIntegration:
    """
    TradingView Integration for Watch List

    >>> from us_stock_wizard.screener.tradingview.tv_integration import TradingViewIntegration
    >>> tv = TradingViewIntegration()
    >>> await tv.handle_all()
    """

    def __init__(
        self, source: DataSource = DataSource.DB, host: str = "", config: dict = {}
    ) -> None:
        self.source = source
        self.root = StockRootDirectory().root_dir()
        self.curr_dir = os.path.dirname(os.path.abspath(__file__))
        self.config: Dict[str, int] = config
        self.clear_all_template = ""
        self.insert_all_template = ""
        self.host = host
        self.load()

    def load(self) -> None:
        # Load Template

        # Clear-all script
        _ = os.path.join(self.curr_dir, "clear-all.js")
        if not os.path.exists(_):
            raise FileNotFoundError("clear-all.js not found")
        with open(_, "r") as f:
            self.clear_all_template = f.read()

        # Insert-all script
        _ = os.path.join(self.curr_dir, "insert-all.js")
        if not os.path.exists(_):
            raise FileNotFoundError("insert-all.js not found")
        with open(_, "r") as f:
            self.insert_all_template = f.read()

        # Load config
        if not self.config:
            _ = os.path.join(self.root, "tv-config.json")
            if not os.path.exists(_):
                raise FileNotFoundError("tv-config.json not found")
            with open(_, "r") as f:
                self.config = json.load(f)

        # Load API
        if not self.host:
            self.host = StockRootDirectory().env().get("API_HOST")

    @staticmethod
    def is_valid_date(date_string):
        try:
            datetime.strptime(date_string, "%Y-%m-%d")
            return True
        except ValueError:
            return False

    async def _get_data_from_db(self, kind: str, date: str) -> List[str]:
        from us_stock_wizard.database.db_utils import StockDbUtils, DbTable

        if not date:
            data = await StockDbUtils.read(table=DbTable.REPORT, output="df")
        else:
            data = await StockDbUtils.read(
                table=DbTable.REPORT, where={"date": date}, output="df"
            )
        tickers = await StockDbUtils.read(table=DbTable.TICKERS, output="df")
        data = data[data["kind"] == kind]

        latest_tickers = data.iloc[-1]["data"]
        tickers = tickers[tickers["ticker"].isin(latest_tickers)]
        tickers["market_ticker"] = tickers["market"] + ":" + tickers["ticker"]
        ticker_exported = tickers["market_ticker"].tolist()
        return ticker_exported

    async def _get_data_from_api(self, kind: str, date: str) -> List[str]:
        """
        Get data from API
        """
        if not self.host:
            raise ValueError("API_HOST env not set")

        url = urllib.parse.urljoin(self.host, f"/api/reports/{kind}")
        logging.info(f"Getting data from {url}")
        if not date:
            data = NetworkRequests._httpx_get_data(url=url, timeout=30)
            return data
        else:
            data = NetworkRequests._httpx_get_data(
                url=url, params={"date": date}, timeout=30
            )
        return data

    async def get_data(self, kind: str, date: str) -> List[str]:
        """
        Get data from database
        """
        if self.source == DataSource.DB:
            return await self._get_data_from_db(kind, date)
        elif self.source == DataSource.API:
            return await self._get_data_from_api(kind, date)
        raise ValueError(f"Unknown source {self.source}")

    def handle_binance_tickers(self, kind: str) -> List[str]:
        """
        Get all spot tickers from Binance API.

        kind: str `binance_usdt`, `binance_btc`
        """
        assert kind in [
            "binance_usdt",
            "binance_btc",
        ], "kind must be binance_usdt OR binance_btc"
        filter_str = kind.split("_")[-1]
        url = "https://api3.binance.com/api/v3/ticker/price"
        data = NetworkRequests._httpx_get_data(url=url, timeout=30)
        all_symbols = []
        for item in data:
            symbol = item["symbol"]
            if symbol.endswith(filter_str.upper()):
                _ = "BINANCE:" + symbol
                all_symbols.append(_)
        return all_symbols

    async def handle_category(self, kind: str, id: int, date: str) -> tuple[str, bool]:
        tickers = []
        if "binance" in kind.lower():
            tickers = self.handle_binance_tickers(kind)
        else:
            tickers: List[str] = await self.get_data(kind, date)

        if not tickers:
            # Not found any tickers
            logging.error(
                f"No tickers found in {kind} on {date}, maybe wrong date / not processed yet / not available"
            )
            return "\n\n", False

        _id = str(id)
        _body = json.dumps(tickers)
        exported_script = self.insert_all_template.replace("$TICKERS$", _body).replace(
            "$WATCHLIST_ID$", _id
        )
        return exported_script, True

    async def handle_all(self) -> str:
        """
        Allow the user to choose to delete the original list or not
        """
        # add keyboard exception

        try:
            is_delete: str = input("Would you like to delete the original list? (y/n):")
            _ = ""
            if is_delete.lower() == "y":
                for kind, tv_watchlist_id in self.config.items():
                    _ += self.clear_all_template.replace(
                        "$WATCHLIST_ID$", str(tv_watchlist_id)
                    )
                    _ += "\n\n"
                pyperclip.copy(_)  # Copy to pasteboard
                input(
                    "Clear-all Copied to pasteboard, now paste to Chrome console in Tradingview.com and run, then press enter to continue"
                )
            # Spec date
            date = None
            date_spec = input(
                "Specify date (YYYY-MM-DD)?: If not specified, just enter to use the latest date available in database."
            )
            if not date_spec:
                print("Using latest date available in database")
                date = None
            else:
                if not self.is_valid_date(date_spec):
                    logging.warning(
                        "Invalid date format, should be YYYY-MM-DD, quitting."
                    )
                    exit(1)
                date = date_spec
                logging.warning(f"Will use date: {date}")

            total_succ = 0
            total = 0
            for kind, tv_watchlist_id in self.config.items():
                total += 1
                logging.warning(f"Exporting {kind} to watchlist {tv_watchlist_id}")
                content, succ = await self.handle_category(kind, tv_watchlist_id, date)
                _ += content
                if succ:
                    total_succ += 1
                    logging.warning(f"Exported {kind} to watchlist {tv_watchlist_id}")
                else:
                    logging.warning(
                        f"Failed to export {kind} to watchlist {tv_watchlist_id}"
                    )
                _ += "\n\n"

            # Also save to pasteboard
            pyperclip.copy(_)
            if total_succ > 0:
                logging.warning(
                    f"Exported {total_succ}/{total} to pasteboard. now paste to Chrome console in Tradingview.com and run"
                )
            else:
                logging.warning(f"No data to export. No watchlist found. quitting.")

        except KeyboardInterrupt:
            print("\nKeyboard Interrupt, exiting")
            exit(0)


if __name__ == "__main__":
    import asyncio

    asyncio.run(TradingViewIntegration(source=DataSource.API).handle_all())
