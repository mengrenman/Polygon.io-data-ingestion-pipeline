#!/usr/bin/env python
"""
Point-in-time holders for every trading segment in a market-layout day lake, and corporate actions re-filed
under the symbol their company held on the event date. No requests: it reads the snapshots that
`run_pullers.py --bulk --asof-dates annual` pulled into the market dir.

Writes to --market-dir:
  holder_lines.parquet              one row per (ticker, piece): the company, a holder id shared across its symbols
  market_dividends_refiled.parquet  dividends with `ticker` = symbol on the ex-date, `filed_ticker`, `refile_rule`
  market_splits_refiled.parquet     the same for splits
  holder_lines_report.csv           every record not filed as-is, and every unresolved or unplaced one

Then derive with `run_pullers.py --bulk --tables "" --holder-lines ...` and rebuild the adjusted lake beside the
old one. See docs/symbol-reuse-after-lake-end.md.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd
import pyarrow.dataset as ds

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from polygon_pullers.asof import holder_lines, refile_actions  # noqa: E402
from polygon_pullers.bulk import (HOLDER_LINES, MARKET_DIVIDENDS, MARKET_DIVIDENDS_REFILED, MARKET_SPLITS,  # noqa: E402
                                  MARKET_SPLITS_REFILED, MARKET_TICKERS, MARKET_TICKERS_ASOF)


def load_days(lake: Path) -> pd.DataFrame:
    """(ticker, date, open, close) per session from a market-layout day lake, ET trading dates, one row per
    ticker-day (the higher-volume row where a source file double-stamps a day)."""
    t = ds.dataset(str(lake), format="parquet").to_table(columns=["ticker", "datetime", "open", "close", "volume"]).to_pandas()
    t["date"] = t["datetime"].dt.tz_convert("US/Eastern").dt.tz_localize(None).dt.normalize()
    t = t.sort_values(["ticker", "date", "volume"], ascending=[True, True, False]).drop_duplicates(["ticker", "date"])
    return t[["ticker", "date", "open", "close"]].reset_index(drop=True)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--lake", type=Path, required=True, help="market-layout DAY lake (<root>/<YYYY>/<MM>.parquet)")
    ap.add_argument("--market-dir", type=Path, required=True, help="refdata/_market with the tables and the snapshots")
    ap.add_argument("--extra-asof", type=Path, default=None,
                    help="extra point-in-time rows (same columns, e.g. per-ticker lookups for unobserved segments)")
    ap.add_argument("--overrides", type=Path, default=None,
                    help="CSV (kind, id, ticker): reviewed filings that win over every rule; empty ticker drops the record")
    args = ap.parse_args()

    md = args.market_dir
    asof = pd.read_parquet(md / MARKET_TICKERS_ASOF)
    if args.extra_asof:
        asof = pd.concat([asof, pd.read_parquet(args.extra_asof)], ignore_index=True)
    days = load_days(args.lake)
    print(f"[lines] {len(days):,} sessions, {days['ticker'].nunique():,} tickers; {len(asof):,} snapshot rows "
          f"on {asof['asof'].nunique()} date(s)")
    lines = holder_lines(days, asof, pd.read_parquet(md / MARKET_TICKERS))
    lines.to_parquet(md / HOLDER_LINES, index=False)
    print(f"[lines] {len(lines):,} pieces on {lines['ticker'].nunique():,} tickers, {lines['line'].nunique():,} lines; "
          f"{int(lines['evidence'].str.contains('handoff').sum())} in-segment handoffs, "
          f"{int((lines['evidence'] == 'unobserved').sum())} unobserved segments")

    ov = pd.read_csv(args.overrides, dtype=str, keep_default_na=False) if args.overrides else None
    report = []
    for kind, src, dst, dcol in (("split", MARKET_SPLITS, MARKET_SPLITS_REFILED, "execution_date"),
                                 ("dividend", MARKET_DIVIDENDS, MARKET_DIVIDENDS_REFILED, "ex_dividend_date")):
        t = pd.read_parquet(md / src)
        o = ov[ov["kind"] == kind] if ov is not None else None
        r = refile_actions(t, lines, date_col=dcol, kind=kind, days=days, overrides=o)
        r.to_parquet(md / dst, index=False)
        counts = r["refile_rule"].value_counts().to_dict()
        print(f"[{kind}s] {len(t):,} -> {len(r):,} ({len(t) - len(r):,} duplicate copies dropped); {counts}")
        flag = r[(r["ticker"] != r["filed_ticker"]) | r["refile_rule"].isin(["unresolved", "price_test",
                                                                             "frequency_mismatch", "override"])]
        report.append(flag.assign(kind=kind).rename(columns={dcol: "date"}))
    rep = pd.concat(report, ignore_index=True)
    rep.to_csv(md / "holder_lines_report.csv", index=False)
    print(f"[report] {len(rep):,} records re-filed or decided by evidence -> {md / 'holder_lines_report.csv'}")


if __name__ == "__main__":
    main()
