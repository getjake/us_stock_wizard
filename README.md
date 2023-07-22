# US Stock Wizard

This is a simple tool to help you find the best stock to buy in US stock market.

It include the following features:

- [x] Find all the symbols in the US stock market (NYSE, NASDAQ)
- [x] Database Managment
- [x] Automatically update the database, kline, and fundamentals

We use the following tools to build this project:

- Prisma
- PostgreSQL
- Google Drive API
- Pandas

## Config

You need to create a `.env` file in the root directory of this project, and add the following content:

```env
ALPHA_VANTAGE_API_KEYS=key1,key2,key3
```

Apply for Google Drive API according to [this video](https://www.youtube.com/watch?v=tamT_iGoZDQ). Save Credentials.json to the root directory of this project.

Then, create `./src/googleauth.json` and paste the credentials.json content to it.
