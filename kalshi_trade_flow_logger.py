"""Log Kalshi trade flow and track cumulative bought/sold quantities for one market.

This script listens to the websocket `trade` channel and writes a CSV of each
trade update plus running totals:
- cumulative_bought_qty
- cumulative_sold_qty

Examples:
    python3 kalshi_ws_trade_flow_logger.py --market-ticker KXNHLGAME-26MAR07BOSCAR-BOS --duration 1800
    python3 kalshi_ws_trade_flow_logger.py --event-ticker KXNHLGAME-26MAR07BOSCAR --team BOS --duration 1800
    python3 kalshi_ws_trade_flow_logger.py --event-ticker KXNHLGAME-26MAR07WSHBOS --team BOS --duration 200
    python3 kalshi_ws_trade_flow_logger.py --event-ticker KXNHLGAME-26MAR18NJNYR --team NJ --duration 200


"""

from __future__ import annotations

import argparse
import asyncio
import csv
import datetime as dt
import json
import os
from typing import Any, Dict, Optional, Tuple

import websockets

from kalshi_ws_probability_logger import (
    WS_HOST,
    WS_PATH,
    parse_probability_value,
    resolve_market_ticker,
    ws_auth_headers,
)


def utc_now_ms() -> int:
    return int(dt.datetime.now(dt.timezone.utc).timestamp() * 1000)


def iso_utc_from_ms(timestamp_ms: int) -> str:
    return dt.datetime.fromtimestamp(timestamp_ms / 1000, tz=dt.timezone.utc).isoformat()


def ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def parse_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def infer_quantity(payload: Dict[str, Any]) -> Optional[float]:
    # Kalshi `trade` messages commonly use `count_fp` (stringified float).
    for key in ("count_fp", "count", "qty", "quantity", "size", "volume", "amount"):
        parsed = parse_float(payload.get(key))
        if parsed is not None:
            return parsed
    return None


def infer_side(payload: Dict[str, Any]) -> str:
    """Return 'buy', 'sell', or 'unknown'."""
    for key in ("side", "taker_side", "aggressor_side", "action"):
        raw = payload.get(key)
        if raw is None:
            continue
        side = str(raw).strip().lower()
        if side in {"buy", "yes", "bid"}:
            return "buy"
        if side in {"sell", "no", "ask"}:
            return "sell"
    return "unknown"


def infer_probability_percent(payload: Dict[str, Any]) -> Tuple[Optional[float], str]:
    """Infer implied YES probability (%) from trade payload fields."""
    for key in ("yes_price_dollars", "yes_price", "price", "last_price"):
        parsed = parse_probability_value(payload.get(key))
        if parsed is not None:
            return parsed, key
    return None, ""

def infer_fee(payload: Dict[str, Any]) -> Tuple[Optional[float], str]:
    """Infer fee (dollars) from fill payload fields when available."""
    for key in ("fee_dollars", "fee", "fee_fp", "fee_dollars_fp"):
        parsed = parse_float(payload.get(key))
        if parsed is not None:
            return parsed, key
    return None, ""


async def run_trade_flow_stream(
    market_ticker: str,
    duration_seconds: int,
    out_csv_path: str,
) -> None:
    ensure_parent_dir(out_csv_path)
    headers = ws_auth_headers()
    ws_url = f"{WS_HOST}{WS_PATH}"

    cumulative_bought = 0.0
    cumulative_sold = 0.0
    deadline = utc_now_ms() + duration_seconds * 1000

    with open(out_csv_path, "w", newline="", encoding="utf-8") as out_file:
        writer = csv.writer(out_file)
        writer.writerow(
            [
                "received_time_utc",
                "received_ts_ms",
                "market_ticker",
                "message_type",
                "inferred_side",
                "quantity",
                "probability_percent",
                "probability_source",
                "fee_dollars",
                "fee_source",
                "cumulative_bought_qty",
                "cumulative_sold_qty",
                "raw_message_json",
            ]
        )

        print(f"Connecting: {ws_url}")
        print(f"Market: {market_ticker}")
        print(f"Output: {out_csv_path}")

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
                # `trade` is public market prints; `fill` is your account's fills (includes fees).
                "params": {"channels": ["trade", "fill"], "market_tickers": [market_ticker]},
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
                msg_type = str(data.get("type", ""))
                payload = data.get("msg", {})

                if msg_type not in {"trade", "fill"} or not isinstance(payload, dict):
                    continue

                quantity = infer_quantity(payload)
                if quantity is None:
                    quantity = 0.0
                side = infer_side(payload)
                probability_percent, probability_source = infer_probability_percent(payload)
                fee_dollars, fee_source = infer_fee(payload) if msg_type == "fill" else (None, "")

                if side == "buy":
                    cumulative_bought += quantity
                elif side == "sell":
                    cumulative_sold += quantity

                writer.writerow(
                    [
                        iso_utc_from_ms(received_ms),
                        received_ms,
                        market_ticker,
                        msg_type,
                        side,
                        f"{quantity:.6f}",
                        "" if probability_percent is None else f"{probability_percent:.4f}",
                        probability_source,
                        "" if fee_dollars is None else f"{fee_dollars:.6f}",
                        fee_source,
                        f"{cumulative_bought:.6f}",
                        f"{cumulative_sold:.6f}",
                        json.dumps(data, separators=(",", ":")),
                    ]
                )
                out_file.flush()

                prob_text = "NA" if probability_percent is None else f"{probability_percent:.2f}%"
                fee_text = "NA" if fee_dollars is None else f"${fee_dollars:.4f}"
                print(
                    f"{iso_utc_from_ms(received_ms)} "
                    f"type={msg_type} side={side} qty={quantity:.2f} "
                    f"p={prob_text} "
                    f"fee={fee_text} "
                    f"cum_buy={cumulative_bought:.2f} cum_sell={cumulative_sold:.2f}"
                )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Track bought/sold trade flow for a Kalshi market over websocket."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--market-ticker", help="Kalshi market ticker")
    group.add_argument("--event-ticker", help="Kalshi event ticker")
    parser.add_argument("--team", default=None, help="Team code when using --event-ticker (e.g. BOS)")
    parser.add_argument("--duration", type=int, default=1800, help="Run duration in seconds (default: 1800)")
    parser.add_argument(
        "--out",
        default=None,
        help="Output CSV path (default: <market_ticker>_trade_flow.csv)",
    )
    args = parser.parse_args()

    if args.duration <= 0:
        raise ValueError("--duration must be > 0")

    market_ticker = args.market_ticker
    if not market_ticker:
        market_ticker = resolve_market_ticker(args.event_ticker, args.team)

    output_path = args.out or f"{market_ticker}_trade_flow.csv"
    asyncio.run(
        run_trade_flow_stream(
            market_ticker=market_ticker,
            duration_seconds=args.duration,
            out_csv_path=output_path,
        )
    )


if __name__ == "__main__":
    main()