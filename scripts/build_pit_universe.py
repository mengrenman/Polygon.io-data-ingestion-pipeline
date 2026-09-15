#!/usr/bin/env python
"""
Build a point-in-time universe (membership per rebalance date) from a market-layout day lake and the
market tickers table. Survivorship-free by construction: membership on a date uses only data up to that
date, so later-delisted names stay in for the periods they traded.

Example (top 1000 US common stocks by trailing 63-day dollar volume, monthly):
  python scripts/build_pit_universe.py \
      --lake /data/parquet_lake/day/all \
      --market-tickers /data/parquet_lake/refdata/_market/market_tickers.parquet \
      --out /data/parquet_lake/universes/top1000_cs_monthly --top-n 1000 --emit-watchlist
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from polygon_ingest.universe import (build_membership, classify_segments, load_market_day_lake,  # noqa: E402
                                     summarize, trading_segments)


def main() -> None:
    ap = argparse.ArgumentParser(description="Point-in-time universe from a market-layout day lake.")
    ap.add_argument("--lake", type=Path, required=True, help="market-layout DAY lake root (<root>/<YYYY>/<MM>.parquet)")
    ap.add_argument("--market-tickers", type=Path, default=None, help="refdata/_market/market_tickers.parquet (type/exchange/name)")
    ap.add_argument("--out", type=Path, required=True, help="output directory")
    ap.add_argument("--top-n", type=int, default=1000)
    ap.add_argument("--lookback", type=int, default=63, help="trailing trading days for dollar volume")
    ap.add_argument("--min-days", type=int, default=40, help="observations required in the window (excludes fresh listings)")
    ap.add_argument("--max-stale", type=int, default=5, help="must have traded within this many trading days")
    ap.add_argument("--min-price", type=float, default=None)
    ap.add_argument("--min-adv", type=float, default=None, help="minimum trailing dollar volume (USD)")
    ap.add_argument("--freq", default="M", choices=["D", "W", "M", "Q"], help="rebalance frequency")
    ap.add_argument("--types", default="CS", help="comma-separated security types admitted for typed records")
    ap.add_argument("--exchanges", default=None, help="comma-separated primary exchanges admitted (default: any)")
    ap.add_argument("--untyped-policy", default="admit", choices=["admit", "exclude", "exclude-if-current-fund"],
                    help="earlier segments of recycled symbols have no type record: admit them unless the symbol looks like a "
                         "derivative (default; recovers Bear Stearns, Meta-under-FB, Wachovia... but also QQQ 2003-04 / VXX), "
                         "exclude them, or exclude only when the current record is a fund (drops QQQ/VXX and Bear Stearns).")
    ap.add_argument("--exclude-untyped", action="store_true", help="same as --untyped-policy exclude")
    ap.add_argument("--exclude-tickers", default=None, help="comma-separated symbols or a JSON list file to exclude by hand (e.g. QQQ,VXX)")
    ap.add_argument("--gap-days", type=int, default=60, help="calendar gap that splits a symbol's history into segments (recycled tickers); "
                                                            "exchange-listed names print daily, Bear Stearns -> ETN was 69 days")
    ap.add_argument("--start", default=None); ap.add_argument("--end", default=None)
    ap.add_argument("--static-list", type=Path, default=None, help="JSON ticker list to compare against (e.g. data/ticker_lists/spx_ndx_combined.json)")
    ap.add_argument("--emit-watchlist", action="store_true", help="also write watchlist.json (union of all members) for `poly bars --watch`")
    args = ap.parse_args()

    args.out.mkdir(parents=True, exist_ok=True)
    print(f"[universe] loading day lake {args.lake} ...")
    df = load_market_day_lake(args.lake, start=None, end=args.end)   # full history needed for trailing windows
    print(f"[universe] {len(df):,} rows, {df['ticker'].nunique():,} tickers, {df['date'].min().date()} -> {df['date'].max().date()}")
    tk = pd.read_parquet(args.market_tickers) if args.market_tickers else None

    excl = None
    if args.exclude_tickers:
        pth = Path(args.exclude_tickers)
        excl = json.loads(pth.read_text()) if pth.exists() else [t.strip() for t in args.exclude_tickers.split(",") if t.strip()]
    seg = classify_segments(trading_segments(df, gap_days=args.gap_days), tk,
                            share_types=[t.strip().upper() for t in args.types.split(",") if t.strip()],
                            exchanges=[e.strip().upper() for e in args.exchanges.split(",")] if args.exchanges else None,
                            untyped_policy="exclude" if args.exclude_untyped else args.untyped_policy,
                            exclude_tickers=excl)
    seg.to_parquet(args.out / "segments.parquet", index=False)
    print(f"[universe] segments: {len(seg):,} ({int((~seg['is_last']).sum()):,} earlier segments of recycled symbols); "
          f"eligible: {int(seg['eligible'].sum()):,}; reasons: {seg['reason'].value_counts().to_dict()}")

    mem = build_membership(df, seg, top_n=args.top_n, lookback=args.lookback, min_days=args.min_days, max_stale=args.max_stale,
                           min_price=args.min_price, min_adv=args.min_adv, freq=args.freq, start=args.start, end=args.end)
    mem.to_parquet(args.out / "membership.parquet", index=False)
    static = json.loads(args.static_list.read_text()) if args.static_list else None
    summary = summarize(mem, tk, static)
    summary["params"] = {k: (str(v) if isinstance(v, Path) else v) for k, v in vars(args).items()}
    (args.out / "summary.json").write_text(json.dumps(summary, indent=2, default=str))
    print(f"[universe] membership: {len(mem):,} rows over {summary.get('rebalances', 0)} rebalances, {summary.get('unique_tickers', 0):,} unique tickers -> {args.out / 'membership.parquet'}")
    if "share_of_members_not_an_active_common_stock_today" in summary:
        g = summary["share_of_members_not_an_active_common_stock_today"]
        print("[universe] share of members no longer an active common stock today, by year: " + ", ".join(f"{y}: {v:.0%}" for y, v in list(g.items())[::4]))
    if summary.get("untyped_segments_admitted_top"):
        top = summary["untyped_segments_admitted_top"][:8]
        print(f"[universe] {summary['untyped_segments_admitted_count']} recycled symbols admitted on their earlier (untyped) segment; review: "
              + ", ".join(f"{x['ticker']}({x['from'][:4]}-{x['to'][:4]})" for x in top))
    if args.emit_watchlist:
        wl = sorted(mem["ticker"].unique().tolist())
        (args.out / "watchlist.json").write_text(json.dumps(wl))
        print(f"[universe] watchlist.json: {len(wl):,} tickers (union of all members)")


if __name__ == "__main__":
    main()
