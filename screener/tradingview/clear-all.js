var watchlist_id = '$WATCHLIST_ID$';

// Clear data first
fetch(
  `https://www.tradingview.com/api/v1/symbols_list/custom/${watchlist_id}/replace/?unsafe=true`,
  {
    headers: {
      accept: '*/*',
      'accept-language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7',
      'content-type': 'application/json',
      'sec-ch-ua':
        '"Google Chrome";v="115", "Chromium";v="115", "Not?A_Brand";v="24"',
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
    body: '[]',
    method: 'POST',
    mode: 'cors',
    credentials: 'include',
  },
);
