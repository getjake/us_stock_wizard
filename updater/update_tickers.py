from us_stock_wizard.src.stocks import StockTickers

st = StockTickers()
await st.handle_all_tickers()
