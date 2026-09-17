#!/usr/bin/env python
from __future__ import annotations
import argparse, io, json, sys
from pathlib import Path
import pandas as pd
import urllib.request

# Wikipedia rejects urllib's default agent with HTTP 403, so identify the client.
USER_AGENT = "polygonio-ingestion-pipeline/1.0 (ticker list builder)"

def fetch_tables(url: str) -> list[pd.DataFrame]:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=20) as r:
        html = r.read().decode("utf-8", errors="replace")
    return pd.read_html(io.StringIO(html))          # StringIO: read_html no longer accepts a raw string

def parse_tickers_from_tables(tables, candidates=("Symbol","Ticker","Ticker symbol")):
    """First table with a ticker-like column -> sorted unique symbols in POLYGON form.

    Polygon writes share classes with a dot (`BRK.B`, `BF.B`), which is also how Wikipedia writes
    them, so the symbol is passed through unchanged. An earlier version rewrote `.` to `-`, which
    silently produced tickers that match nothing in the lake.
    """
    for tbl in tables:
        for c in candidates:
            if c in tbl.columns:
                # index constituents are common stock, which Polygon writes in capitals, so upper-casing
                # a Wikipedia cell is safe here - unlike on flat-file symbols, where a lowercase letter
                # is the share class (see polygon_ingest.tickers)
                s = tbl[c].astype(str).str.strip().str.upper()
                s = s.str.split().str[0]            # some cells carry a trailing footnote
                s = s[s.str.len() > 0]
                return sorted(s.unique())
    raise ValueError("No ticker-like column found in any table")

def build(outdir: Path) -> None:
    outdir.mkdir(parents=True, exist_ok=True)

    # S&P 500
    spx = parse_tickers_from_tables(fetch_tables("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"),
                                    candidates=("Symbol","Ticker"))
    pd.Series(spx).to_csv(outdir / "spx.txt", index=False, header=False)
    pd.Series(spx).to_json(outdir / "spx.json", orient="values")

    # Nasdaq-100. Wikipedia removed the constituents table from this article, so the scrape can
    # legitimately fail; fall back to a previously saved list rather than aborting the whole build.
    try:
        ndx = parse_tickers_from_tables(fetch_tables("https://en.wikipedia.org/wiki/Nasdaq-100"),
                                        candidates=("Ticker","Ticker symbol","Symbol"))
    except Exception as e:
        prev = outdir / "ndx.json"
        legacy = outdir / "nasdaq100.json"
        src = prev if prev.exists() else (legacy if legacy.exists() else None)
        if src is None:
            raise ValueError(f"Nasdaq-100 table unavailable ({e}) and no saved list at {prev} or {legacy}") from e
        ndx = sorted(json.loads(src.read_text()))
        print(f"[WARN] Nasdaq-100 scrape failed ({e}); reused {src} ({len(ndx)} tickers)", file=sys.stderr)
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
