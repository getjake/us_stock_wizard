"""
FastAPI integration with Prisma

Run: 
> uvicorn main:app --reload --port 8090
"""
from typing import Optional, List
from fastapi import FastAPI, Depends, HTTPException
from fastapi.responses import HTMLResponse
from us_stock_wizard.database.db_utils import StockDbUtils, DbTable
from us_stock_wizard.src.plot import StockPlot
import pandas as pd

app = FastAPI()

# Cache -> Ticker -> Market:Ticker
tickers_mapping: dict = {}


@app.get("/")
async def root() -> dict:
    return {"message": "Welcome to use US Stock Wizard API!"}


@app.get("/api/reports/{kind}")
async def get_report(kind: str, date: str="latest") -> Optional[List[str]]:
    """
    Get Report by kind
    Args:
        kind: eg. PostAnalysis_stage2
        date: YYYY-MM-DD
    """
    global tickers_mapping
    if not tickers_mapping:
        await get_tickers()

    # Select Date
    if date == "latest":
        all_dates = await StockDbUtils.read_groupby(
            table=DbTable.REPORT, group_by=["date"]
        )
        chosen_date = pd.DataFrame(all_dates)["date"].max()
    else:
        chosen_date = pd.to_datetime(date)

    data = await StockDbUtils.read(
        table=DbTable.REPORT, where={"date": chosen_date}, output="df"
    )
    if data.empty:
        return []

    data = data[data["kind"] == kind]
    if data.empty:
        return []
        
    latest_data: List[str] = data.iloc[-1]["data"]
    _ = [tickers_mapping.get(ticker, ticker) for ticker in latest_data]
    return _


@app.get("/api/tickers")
async def get_tickers() -> Optional[dict]:
    """
    Get all tickers in the NYSE and NASDAQ
    """
    global tickers_mapping
    tickers = await StockDbUtils.read(table=DbTable.TICKERS, output="df")
    if tickers.empty:
        return []
    tickers["market_ticker"] = tickers["market"] + ":" + tickers["ticker"]
    tickers.set_index("ticker", inplace=True)
    tickers = tickers[["market_ticker"]]
    _exported = tickers["market_ticker"].to_dict()
    tickers_mapping = _exported
    return _exported


@app.get("/plot/{ticker}", response_class=HTMLResponse)
async def get_plot(ticker: str):
    """
    Get plot by ticker
    """
    try:
        plot = StockPlot(ticker.upper())
        result = await plot.handle()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
