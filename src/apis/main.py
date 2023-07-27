"""
FastAPI integration with Prisma

Run: 
> uvicorn main:app --reload --port 8090
"""
from typing import Optional, List
from fastapi import FastAPI
from us_stock_wizard.database.db_utils import StockDbUtils, DbTable

app = FastAPI()


@app.get("/")
async def root() -> dict:
    return {"message": "Welcome to use US Stock Wizard API!"}


@app.get("/api/reports/{kind}")
async def get_latest_report(kind: str) -> Optional[List[str]]:
    """
    Get Report by kind

    Example:
    kind: PostAnalysis_stage2
    """
    data = await StockDbUtils.read(table=DbTable.REPORT, output="df")
    data = data[data["kind"] == kind]
    latest_data = data.iloc[-1]["data"]
    return latest_data


@app.get("/api/tickers")
async def get_tickers() -> Optional[List[str]]:
    """
    Get all tickers in the NYSE and NASDAQ
    """
    tickers = await StockDbUtils.read(table=DbTable.TICKERS, output="df")
    if tickers.empty:
        return []
    tickers["market_ticker"] = tickers["market"] + ":" + tickers["ticker"]
    ticker_exported = tickers["market_ticker"].tolist()
    return ticker_exported
