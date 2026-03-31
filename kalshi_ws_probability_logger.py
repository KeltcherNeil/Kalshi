"""Stream Kalshi ticker updates over WebSocket and log probability data.

This script can subscribe by `market_ticker` directly or by `event_ticker`
(+ optional `--team`) and then:
1) print live probability updates, and
2) write raw ticks + 15-second bucketed probabilities to CSV files.

Examples:
    python3 kalshi_ws_probability_logger.py --event-ticker KXNHLGAME-26FEB03SEAANA --team SEA --duration 600
    python3 kalshi_ws_probability_logger.py --market-ticker KXNHLGAME-26FEB03SEAANA-SEA --duration 300
    python3 kalshi_ws_probability_logger.py --market-ticker KXNHLGAME-26MAR15NSHEDM-NSH  --duration 300

"""

from __future__ import annotations

import argparse
import asyncio
import csv
import datetime as dt
import json
import os
from typing import Any, Dict, Optional, Tuple

import requests
import websockets

from Kalshi_auth import API_KEY_ID, create_signature, private_key

WS_HOST = "wss://api.elections.kalshi.com"
WS_PATH = "/trade-api/ws/v2"
REST_BASE = "https://api.elections.kalshi.com"


def utc_now_ms() -> int:
    return int(dt.datetime.now(dt.timezone.utc).timestamp() * 1000)


def iso_utc_from_ms(timestamp_ms: int) -> str:
    return dt.datetime.fromtimestamp(timestamp_ms / 1000, tz=dt.timezone.utc).isoformat()


def parse_probability_value(value: Any) -> Optional[float]:
    """Parse Kalshi price value into percent [0, 100]."""
    if value is None:
        return None

    if isinstance(value, (int, float)):
        numeric = float(value)
    elif isinstance(value, str):
        try:
            numeric = float(value)
        except ValueError:
            return None
    else:
        return None

    # Kalshi can return integer cents (0..100) or dollar decimals (0.00..1.00).
    if 0.0 <= numeric <= 1.0:
        return numeric * 100.0
    if 0.0 <= numeric <= 100.0:
        return numeric
    return None


def resolve_market_ticker(event_ticker: str, team: Optional[str]) -> str:
    response = requests.get(
        f"{REST_BASE}/trade-api/v2/markets",
        params={"event_ticker": event_ticker},
        timeout=20,
    )
    response.raise_for_status()
    markets = response.json().get("markets", [])
    if not markets:
        raise ValueError(f"No markets found for event_ticker={event_ticker}")

    if team:
        team_upper = team.upper()
        for market in markets:
            ticker = str(market.get("ticker", "")).upper()
            if ticker.endswith(f"-{team_upper}"):
                return str(market["ticker"])
        raise ValueError(
            f"Could not match team '{team}' to market ticker. "
            f"Candidates: {[m.get('ticker') for m in markets]}"
        )

    markets_sorted = sorted(markets, key=lambda m: str(m.get("ticker", "")))
    return str(markets_sorted[0]["ticker"])


def ws_auth_headers() -> Dict[str, str]:
    timestamp = str(utc_now_ms())
    signature = create_signature(private_key, timestamp, "GET", WS_PATH)
    return {
        "KALSHI-ACCESS-KEY": API_KEY_ID,
        "KALSHI-ACCESS-SIGNATURE": signature,
        "KALSHI-ACCESS-TIMESTAMP": timestamp,
    }


def extract_prob_from_ticker_msg(
    payload: Dict[str, Any], last_probability: Optional[float]
) -> Tuple[Optional[float], str]:
    """Infer probability from ticker payload with a robust fallback order."""
    # Best direct fields first.
    direct_fields = [
        "yes_price",
        "last_price",
        "price",
        "yes_bid",
        "yes_ask",
    ]
    for field in direct_fields:
        parsed = parse_probability_value(payload.get(field))
        if parsed is not None:
            return parsed, field

    # Midpoint if top-of-book values are present.
    yes_bid = parse_probability_value(payload.get("yes_bid"))
    yes_ask = parse_probability_value(payload.get("yes_ask"))
    if yes_bid is not None and yes_ask is not None:
        return (yes_bid + yes_ask) / 2.0, "mid(yes_bid,yes_ask)"

    return last_probability, "carry_forward"


def ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


async def run_stream(
    market_ticker: str,
    duration_seconds: int,
    raw_csv_path: str,
    bucket_csv_path: str,
    bucket_seconds: int,
) -> None:
    ensure_parent_dir(raw_csv_path)
    ensure_parent_dir(bucket_csv_path)

    headers = ws_auth_headers()
    ws_url = f"{WS_HOST}{WS_PATH}"

    raw_file = open(raw_csv_path, "w", newline="", encoding="utf-8")
    bucket_file = open(bucket_csv_path, "w", newline="", encoding="utf-8")
    raw_writer = csv.writer(raw_file)
    bucket_writer = csv.writer(bucket_file)

    raw_writer.writerow(
        [
            "received_time_utc",
            "received_ts_ms",
            "market_ticker",
            "probability_percent",
            "probability_source",
            "yes_bid",
            "yes_ask",
            "last_price",
            "raw_type",
        ]
    )
    bucket_writer.writerow(
        [
            "bucket_start_utc",
            "bucket_start_ts_ms",
            "market_ticker",
            "probability_percent",
        ]
    )

    print(f"Connecting: {ws_url}")
    print(f"Market: {market_ticker}")
    print(f"Raw output: {raw_csv_path}")
    print(f"{bucket_seconds}s buckets: {bucket_csv_path}")

    last_probability: Optional[float] = None
    current_bucket_start: Optional[int] = None
    current_bucket_probability: Optional[float] = None
    deadline = utc_now_ms() + duration_seconds * 1000
    bucket_ms = bucket_seconds * 1000

    try:
        async with websockets.connect(
            ws_url,
            additional_headers=headers,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=10,
        ) as ws:
            subscribe_message = {
                "id": 1,
                "cmd": "subscribe",
                "params": {"channels": ["ticker"], "market_tickers": [market_ticker]},
            }
            await ws.send(json.dumps(subscribe_message))

            while utc_now_ms() < deadline:
                timeout_seconds = max(0.1, (deadline - utc_now_ms()) / 1000.0)
                try:
                    raw_message = await asyncio.wait_for(ws.recv(), timeout=timeout_seconds)
                except asyncio.TimeoutError:
                    break

                received_ms = utc_now_ms()
                data = json.loads(raw_message)
                msg_type = data.get("type")
                payload = data.get("msg", {}) if isinstance(data.get("msg"), dict) else {}

                if msg_type != "ticker":
                    continue

                probability, source = extract_prob_from_ticker_msg(payload, last_probability)
                if probability is None:
                    continue

                last_probability = probability
                yes_bid = payload.get("yes_bid")
                yes_ask = payload.get("yes_ask")
                last_price = payload.get("last_price")

                raw_writer.writerow(
                    [
                        iso_utc_from_ms(received_ms),
                        received_ms,
                        market_ticker,
                        f"{probability:.4f}",
                        source,
                        yes_bid,
                        yes_ask,
                        last_price,
                        msg_type,
                    ]
                )
                raw_file.flush()
                print(f"{iso_utc_from_ms(received_ms)} {market_ticker} {probability:.2f}%")

                bucket_start = (received_ms // bucket_ms) * bucket_ms
                if current_bucket_start is None:
                    current_bucket_start = bucket_start
                    current_bucket_probability = probability
                    continue

                if bucket_start == current_bucket_start:
                    current_bucket_probability = probability
                    continue

                # Bucket boundary crossed; write previous bucket with its latest value.
                if current_bucket_probability is not None:
                    bucket_writer.writerow(
                        [
                            iso_utc_from_ms(current_bucket_start),
                            current_bucket_start,
                            market_ticker,
                            f"{current_bucket_probability:.4f}",
                        ]
                    )
                    bucket_file.flush()

                current_bucket_start = bucket_start
                current_bucket_probability = probability

            if current_bucket_start is not None and current_bucket_probability is not None:
                bucket_writer.writerow(
                    [
                        iso_utc_from_ms(current_bucket_start),
                        current_bucket_start,
                        market_ticker,
                        f"{current_bucket_probability:.4f}",
                    ]
                )
                bucket_file.flush()
    finally:
        raw_file.close()
        bucket_file.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Log Kalshi websocket ticker data and 15s probability buckets."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--market-ticker", help="Kalshi market ticker, e.g. KXNHLGAME-26FEB03SEAANA-SEA")
    group.add_argument("--event-ticker", help="Kalshi event ticker, e.g. KXNHLGAME-26FEB03SEAANA")
    parser.add_argument("--team", default=None, help="Team code when using --event-ticker (e.g. SEA)")
    parser.add_argument("--duration", type=int, default=600, help="Streaming duration in seconds (default: 600)")
    parser.add_argument(
        "--bucket-seconds",
        type=int,
        default=15,
        help="Bucket size for aggregated probability output (default: 15)",
    )
    parser.add_argument(
        "--raw-out",
        default="kalshi_ws_raw.csv",
        help="Raw tick CSV output path (default: kalshi_ws_raw.csv)",
    )
    parser.add_argument(
        "--bucket-out",
        default="kalshi_ws_15s.csv",
        help="Bucketed CSV output path (default: kalshi_ws_15s.csv)",
    )
    args = parser.parse_args()

    if args.bucket_seconds <= 0:
        raise ValueError("--bucket-seconds must be > 0")
    if args.duration <= 0:
        raise ValueError("--duration must be > 0")

    market_ticker = args.market_ticker
    if not market_ticker:
        market_ticker = resolve_market_ticker(args.event_ticker, args.team)

    asyncio.run(
        run_stream(
            market_ticker=market_ticker,
            duration_seconds=args.duration,
            raw_csv_path=args.raw_out,
            bucket_csv_path=args.bucket_out,
            bucket_seconds=args.bucket_seconds,
        )
    )


if __name__ == "__main__":
    main()
