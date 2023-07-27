// ==UserScript==
// @name     Download JSON
// @version  1
// @grant    none
// @include  https://www.tradingview.com/*
// ==/UserScript==

window.addEventListener('keydown', function (e) {
  // Check for Cmd (or Ctrl in non-Mac OS) + [
  if ((e.metaKey || e.ctrlKey) && e.key === '[') {
    e.preventDefault(); // Prevent default action

    fetch('https://www.tradingview.com/api/v1/symbols_list/colored/red', {
      headers: {
        accept: '*/*',
        'accept-language': 'en-US,en;q=0.9',
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
        'https://www.tradingview.com/chart/LYSu4azL/?symbol=NASDAQ%3AACGL',
      referrerPolicy: 'origin-when-cross-origin',
      body: null,
      method: 'GET',
      mode: 'cors',
      credentials: 'include',
    })
      .then((response) => response.json()) // parse JSON from response
      .then((data) => {
        // Only keep the symbols section of the data
        var symbols = data.symbols;

        // Create a Blob from the symbols data
        var file = new Blob([JSON.stringify(symbols)], {
          type: 'application/json',
        });
        var a = document.createElement('a'),
          url = URL.createObjectURL(file);

        // Set up the download link
        a.href = url;
        a.download = 'symbols.json';
        document.body.appendChild(a);
        a.click();

        // Clean up
        setTimeout(function () {
          document.body.removeChild(a);
          window.URL.revokeObjectURL(url);
        }, 0);
      })
      .catch((error) => console.error(error));
  }
});
