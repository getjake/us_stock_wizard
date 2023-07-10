import logging
from enum import Enum
from typing import List
from dotenv import load_dotenv
import httpx
import yfinance as yf
import pandas as pd


class StockMarket(Enum, str):
    NASDAQ = "NASDAQ"
    NYSE = "NYSE"


class StockUtils:
    NASDAQ_URL = "https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=25&exchange=NASDAQ&download=true"
    NYSE_URL = "https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=25&exchange=NYSE&download=true"
    HEADERS = {
        "authority": "api.nasdaq.com",
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "accept-language": "en-US,en;q=0.9",
        "cache-control": "max-age=0",
        "dnt": "1",
        "sec-ch-ua": '"Not.A/Brand";v="8", "Chromium";v="114", "Google Chrome";v="114"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "none",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    }

    def __init__(self) -> None:
        pass

    def get_tickers(self, market: StockMarket) -> pd.DataFrame:
        if market == StockMarket.NASDAQ:
            url = self.NASDAQ_URL
        elif market == StockMarket.NYSE:
            url = self.NYSE_URL
        else:
            raise Exception("Invalid market")

        resp = httpx.get(url, headers=self.HEADERS)
        if resp.status_code != 200:
            raise Exception(f"Error getting tickers for market: {market}")
        data_headers = resp.json()["data"]["headers"]
        data_rows = resp.json()["data"]["rows"]
        df = pd.DataFrame(data_rows, columns=data_headers)
        return df

    def get_all_tickers(self) -> bool:
        pass
