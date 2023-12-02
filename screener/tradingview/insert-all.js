var tickers = JSON.parse('$TICKERS$');
var watchlist_id = '$WATCHLIST_ID$';

fetch(
  `https://www.tradingview.com/api/v1/symbols_list/custom/${watchlist_id}/append/`,
  {
    headers: {
      accept: '*/*',
      'accept-language': 'en-US,en;q=0.9',
      'content-type': 'application/json',
      'sec-ch-ua':
        '"Not/A)Brand";v="99", "Google Chrome";v="115", "Chromium";v="115"',
      'sec-ch-ua-mobile': '?0',
      'sec-ch-ua-platform': '"macOS"',
      'sec-fetch-dest': 'empty',
      'sec-fetch-mode': 'cors',
      'sec-fetch-site': 'same-origin',
      'x-language': 'en',
      'x-requested-with': 'XMLHttpRequest',
    },
    referrer:
      'https://www.tradingview.com/chart/LYSu4azL/?symbol=NASDAQ%3AAAPL',
    referrerPolicy: 'origin-when-cross-origin',
    body: JSON.stringify(tickers),
    method: 'POST',
    mode: 'cors',
    credentials: 'include',
  },
);
