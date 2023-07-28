"""
TradingView Integration

- Auto Add All Tickers to TradingView Watchlist
- Download Red-flag list 
"""

import urllib
import os
import logging
import json
from typing import Tuple
import pyperclip
from us_stock_wizard import StockRootDirectory
from us_stock_wizard.database.db_utils import StockDbUtils, DbTable
