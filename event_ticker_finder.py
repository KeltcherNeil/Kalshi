"""
Find the ticker of an of a specific event(EX: Bruins vs Capitals)
"""

import requests
import pandas as pd

BASE_URL = "https://api.elections.kalshi.com"
def get_ticker_list(ticker, year, month, day, limit_found=200):
    """
    Returns a list of Kalshi event tickers for a given series and date.

    Args:
        ticker (str):       Kalshi series ticker, e.g. 'kxnhlgame'. Case-insensitive.
        year (int):         Two-digit year, e.g. 26 for 2026.
        month (str):        Three-letter month abbreviation, e.g. 'MAR'. Case-insensitive.
        day (int):          Day of the month, e.g. 5.
        limit_found (int):  Max number of events to fetch before stopping. Default 200.

    Returns:
        list[str]: Event tickers matching the date, e.g. ['KXNHLGAME-26MAR05-BOS-WSH'].
    """
    events = []
    ticker = ticker.upper()
    month = month.upper()
    cursor = None

    # Build the date string to filter by, e.g. "26MAR08"
    date_str = f"{year}{month}{int(day):02d}"

    while len(events) < limit_found:
        url = f"{BASE_URL}/trade-api/v2/events?series_ticker={ticker}&limit=200"
        if cursor:
            url += f"&cursor={cursor}"

        resp = requests.get(url).json()
        events.extend(resp.get("events", []))

        cursor = resp.get("cursor")
        if not cursor:
            break

    event_tickers = []
    for mark in events:
        tick = mark['event_ticker']
        if date_str in tick:
            event_tickers.append(tick)

    df = pd.DataFrame(event_tickers)

    df.to_csv(f'{ticker} for {month}, {day}, {year}', index=False, header=False)


get_ticker_list('kxnhlgame', 26, 'MAR', 18)
