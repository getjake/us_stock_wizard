import os
import asyncio
from typing import Optional
import pandas as pd
from us_stock_wizard.database.db_utils import StockDbUtils, DbTable


class TosIntegration:
    """
    Integration with ThinkOrSwim
    """

    def __init__(
        self, imported_csv: Optional[str] = None, skipped_rows: int = 3
    ) -> None:
        if not imported_csv:
            imported_csv = input(
                "Please Enter the CSV file Location from ThinkOrSwim: "
            )
        assert os.path.isfile(imported_csv), "Imported CSV File not exist"
        data = pd.read_csv(imported_csv, skiprows=skipped_rows)
        self.tickers = data["Symbol"].tolist()
        assert self.tickers, "No tickers found in CSV"
        self.filtered_tickers: list[str] = []

    async def filter_tickers(self) -> list[str]:
        """
        Filter tickers with multiple reports
        """
        available_tickers = set()
        kinds = ["stage2", "qull_m1", "qull_m3", "qull_m6", "ep"]

        for kind in kinds:
            _ = await StockDbUtils.read_first(
                DbTable.REPORT,
                where={"kind": kind},
            )
            available_tickers.update(_.get("data", []))

        for ticker in self.tickers:
            if ticker in available_tickers:
                self.filtered_tickers.append(ticker)

    def export_to_clipboard(self) -> bool:
        """
        Export filtered_tickers to clipboard
        """
        if not self.filtered_tickers:
            print("No tickers to export")
            return False

        df = pd.DataFrame(self.filtered_tickers, columns=["Symbol"])
        df.to_clipboard(index=False, header=False)
        print(
            f"Exported {len(self.filtered_tickers)} tickers to clipboard, paste them on the ThinkOrSwim Watchlist Importer"
        )
        return True


def main():
    """
    Main function
    """
    ti = TosIntegration()
    asyncio.run(ti.filter_tickers())
    ti.export_to_clipboard()


if __name__ == "__main__":
    main()
