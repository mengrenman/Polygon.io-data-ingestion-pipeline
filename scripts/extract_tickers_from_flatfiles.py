#!/usr/bin/env python
from __future__ import annotations
import argparse, sys, gzip
from pathlib import Path
from typing import Iterable
import pandas as pd

def detect_header(csvgz: Path) -> list[str]:
    with gzip.open(csvgz, "rt") as f:
        return f.readline().strip().split(",")

def detect_ticker_col(header: Iterable[str]) -> str | None:
    for cand in ("ticker","T","symbol","S"):
        if cand in header:
            return cand
    return None

def sample_tickers(csvgz: Path, ticker_col: str, nrows: int) -> list[str]:
    """Tickers in one flat file, exactly as Polygon writes them.

    Two details that are easy to get wrong and both lose real symbols:

    * ``keep_default_na=False`` - pandas would otherwise read the ticker ``NA`` (Nano Labs) as a
      null and drop it.
    * no ``.upper()`` - Polygon encodes share class in letter case (``AAp`` is Alcoa's preferred,
      distinct from ``AAP``; ``AANw`` is a warrant), so upper-casing merges different securities.
    """
    try:
        df = pd.read_csv(
            csvgz,
            usecols=[ticker_col],
            compression="gzip",
            dtype={ticker_col: "string"},
            keep_default_na=False,
            na_values=[""],
            nrows=nrows or None,          # 0 / None -> read the whole file
        )
        s = df[ticker_col].dropna().astype(str).str.strip()
        return s[s.str.len() > 0].tolist()
    except Exception:
        return []

def main():
    ap = argparse.ArgumentParser(description="Extract deduped tickers from Polygon flatfiles")
    ap.add_argument("--src", required=True, type=Path, help="Root folder to scan recursively")
    ap.add_argument("--outdir", type=Path, default=Path("data/ticker_lists"))
    ap.add_argument("--name", type=str, default="all_polygonio_tickers", help="base filename for outputs")
    ap.add_argument("--pattern", type=str, default="*.csv.gz", help="glob pattern to match files")
    ap.add_argument("--nrows", type=int, default=0,
                    help="rows to read per file; 0 (default) reads every row. A small sample misses "
                         "any symbol that sorts late in the file, so only use it for a quick look.")
    ap.add_argument("--limit-files", type=int, default=0, help="optional cap on number of files scanned")
    args = ap.parse_args()

    args.outdir.mkdir(parents=True, exist_ok=True)
    files = sorted(args.src.rglob(args.pattern))
    if args.limit_files > 0:
        files = files[: args.limit_files]
    if not files:
        print(f"[WARN] No files matched under {args.src} with pattern {args.pattern}")
        sys.exit(0)

    seen = set()
    for p in files:
        hdr = detect_header(p)
        col = detect_ticker_col(hdr)
        if not col:
            continue
        for t in sample_tickers(p, col, args.nrows):
            if t:
                seen.add(t)

    out_json = args.outdir / f"{args.name}.json"
    out_txt  = args.outdir / f"{args.name}.txt"
    ser = pd.Series(sorted(seen))
    ser.to_json(out_json, orient="values")
    ser.to_csv(out_txt, index=False, header=False)
    print(f"Wrote: {out_json} and {out_txt}  (tickers={len(ser)})")

if __name__ == "__main__":
    main()
