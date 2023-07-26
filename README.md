# US Stock Wizard

> Built by Trader, for Traders.

This is a simple tool to help you find the best stock to buy in US stock market.

It include the following features:

- [x] Find all the symbols in the US stock market (NYSE, NASDAQ)
- [x] Database Managment
- [x] Automatically update the database, kline, and fundamentals
- [x] Filter all stocks meeting your criteria.
- [x] Automatically generate a report in Google Drive for you.

We use the following tools to build this project:

- Prisma
- PostgreSQL
- Google Drive API
- Pandas

## Config

You need to create a `.env` file in the root directory of this project, and add the following content:

```env
ALPHA_VANTAGE_API_KEYS=key1,key2,key3
GDRIVE_PARENT_FOLDER_ID=your_google_parent_folder_id
DINGTALK_KEY=your_dingtalk_key
DINGTALK_SECRET=your_dingtalk_secret
```

If you would like to be notified when the report is generated, you can apply for a DingTalk robot, and add the key and secret to the `.env` file.

Apply for Google Drive API according to [this video](https://www.youtube.com/watch?v=tamT_iGoZDQ). Save Credentials.json to the root directory of this project.

Then, create `./src/googleauth.json` and paste the credentials.json content to it.

## Cron Job

Assuming your project is located at `/path/to/us_stock_wizard`, and your timezone is UTC-4, you can add the following cron job to your server:

```bash
# Update fundamental data every day at 9:30
30 09 * * * python3 /path/to/us_stock_wizard/updater/get_fundamentals.py
# Update dividend data every day at 9:30 to accelerete the candlestick chart generation
30 07 * * * python3 /path/to/us_stock_wizard/updater/get_dividends.py
# After market close, update candlestick chart every day and generate a report for you.
05 16 * * * python3 /path/to/us_stock_wizard/updater/all_in_one.py
```
