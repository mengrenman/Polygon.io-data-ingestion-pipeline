#!/usr/bin/env python
from __future__ import annotations
import argparse, sys
from pathlib import Path
import pandas as pd
import urllib.request

def fetch_html(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=20) as r:
        return r.read()

def parse_tickers_from_tables(tables, candidates=("Symbol","Ticker","Ticker symbol")):
    # Find the first table with a likely ticker column
    for tbl in tables:
        for c in candidates:
            if c in tbl.columns:
                col = c
                s = tbl[col].astype(str)
                # normalize: BRK.B -> BRK-B
                s = s.str.strip().str.upper().str.replace(".", "-", regex=False)
                # some rows contain notes separated by spaces/slashes—take the first token
                s = s.str.split().str[0]
                # drop empties
                s = s[s.str.len() > 0]
                return sorted(s.unique())
    raise ValueError("No ticker-like column found in any table")

def build(outdir: Path) -> None:
    outdir.mkdir(parents=True, exist_ok=True)

    # S&P 500
    spx_html = fetch_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
    spx_tables = pd.read_html(spx_html)
    spx = parse_tickers_from_tables(spx_tables, candidates=("Symbol","Ticker"))
    pd.Series(spx).to_csv(outdir / "spx.txt", index=False, header=False)
    pd.Series(spx).to_json(outdir / "spx.json", orient="values")

    # Nasdaq-100
    ndx_html = fetch_html("https://en.wikipedia.org/wiki/Nasdaq-100")
    ndx_tables = pd.read_html(ndx_html)
    ndx = parse_tickers_from_tables(ndx_tables, candidates=("Ticker","Ticker symbol"))
    pd.Series(ndx).to_csv(outdir / "ndx.txt", index=False, header=False)
    pd.Series(ndx).to_json(outdir / "ndx.json", orient="values")

    # Combined
    comb = sorted(set(spx) | set(ndx))
    pd.Series(comb).to_csv(outdir / "spx_ndx_combined.txt", index=False, header=False)
    pd.Series(comb).to_json(outdir / "spx_ndx_combined.json", orient="values")

    print(f"Wrote: {outdir}/spx(.json/.txt), ndx(.json/.txt), spx_ndx_combined(.json/.txt)")

def main():
    ap = argparse.ArgumentParser(description="Build SPX/NDX ticker lists from Wikipedia")
    ap.add_argument("--outdir", type=Path, default=Path("data/ticker_lists"))
    args = ap.parse_args()
    try:
        build(args.outdir)
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        sys.exit(2)

if __name__ == "__main__":
    main()
