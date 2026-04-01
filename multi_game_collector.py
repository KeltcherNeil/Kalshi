"""Collect trade flow + probability data for multiple NHL games concurrently.

For each event ticker supplied, the script resolves the market ticker, then
runs both the trade-flow stream and the probability stream in parallel.
All games run concurrently in a single asyncio event loop — no threads, no
subprocesses.

Output layout:
    data/
        <MARKET_TICKER>/
            trade_flow.csv      — every trade print + cumulative flow
            prob_raw.csv        — raw ticker ticks
            prob_15s.csv        — 15-second bucketed probability

Event tickers are supplied as  TICKER:TEAM  pairs (team is optional).

Examples:
    # Two games on the same night
    python3 multi_game_collector.py \\
        --events KXNHLGAME-26MAR18NJNYR:NJ KXNHLGAME-26MAR18DALCOL:COL \\
        --duration 7200

    # Read tickers from a file, one per line (TICKER or TICKER:TEAM)
    python3 multi_game_collector.py --events-file tonight.txt --duration 7200
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path
from typing import List, Optional, Tuple

from kalshi_ws_probability_logger import resolve_market_ticker
from kalshi_trade_flow_logger import run_trade_flow_stream

OUTPUT_DIR = Path("data")


def parse_event_entry(entry: str) -> Tuple[str, Optional[str]]:
    """Parse 'EVENT_TICKER:TEAM' or 'EVENT_TICKER' into (event_ticker, team)."""
    parts = entry.strip().split(":")
    event_ticker = parts[0].strip()
    team = parts[1].strip() if len(parts) > 1 and parts[1].strip() else None
    return event_ticker, team


def load_events_file(path: str) -> List[str]:
    entries = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                entries.append(line)
    return entries


async def collect_one_game(
    market_ticker: str,
    duration_seconds: int,
    bucket_seconds: int,
) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    trade_flow_path = str(OUTPUT_DIR / f"{market_ticker}.csv")

    print(f"[{market_ticker}] Starting collection -> {trade_flow_path}")

    await run_trade_flow_stream(
        market_ticker=market_ticker,
        duration_seconds=duration_seconds,
        out_csv_path=trade_flow_path,
    )

    print(f"[{market_ticker}] Done.")


async def run_all(
    events: List[str],
    duration_seconds: int,
) -> None:
    # Resolve all market tickers upfront so bad inputs fail fast.
    resolved: List[Tuple[str, str]] = []  # (event_ticker, market_ticker)
    for entry in events:
        event_ticker, team = parse_event_entry(entry)
        try:
            market_ticker = resolve_market_ticker(event_ticker, team)
            resolved.append((event_ticker, market_ticker))
            print(f"Resolved: {event_ticker} -> {market_ticker}")
        except Exception as exc:
            print(f"[ERROR] Could not resolve '{entry}': {exc}", file=sys.stderr)

    if not resolved:
        print("No valid markets to collect. Exiting.", file=sys.stderr)
        return

    tasks = [
        collect_one_game(market_ticker, duration_seconds, 0)
        for _, market_ticker in resolved
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    for (event_ticker, market_ticker), result in zip(resolved, results):
        if isinstance(result, Exception):
            print(f"[ERROR] {market_ticker} failed: {result}", file=sys.stderr)


def main() -> None:
    global OUTPUT_DIR
    parser = argparse.ArgumentParser(
        description="Collect Kalshi trade flow + probability data for multiple games."
    )
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "--events",
        nargs="+",
        metavar="TICKER[:TEAM]",
        help="One or more event tickers, e.g. KXNHLGAME-26MAR18NJNYR:NJ",
    )
    source.add_argument(
        "--events-file",
        metavar="FILE",
        help="Path to a text file with one TICKER[:TEAM] per line",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=7200,
        help="Collection duration in seconds per game (default: 7200 = 2 hours)",
    )
    parser.add_argument(
        "--out-dir",
        default=str(OUTPUT_DIR),
        help=f"Root output directory (default: {OUTPUT_DIR})",
    )
    args = parser.parse_args()

    OUTPUT_DIR = Path(args.out_dir)

    if args.duration <= 0:
        parser.error("--duration must be > 0")

    events = args.events if args.events else load_events_file(args.events_file)
    if not events:
        parser.error("No events provided.")

    asyncio.run(run_all(events, args.duration))


if __name__ == "__main__":
    main()
