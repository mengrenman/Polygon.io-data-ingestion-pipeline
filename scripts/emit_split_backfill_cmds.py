# emit_split_backfill_cmds.py
from pathlib import Path
import pandas as pd
import sys

REFDIR   = Path("refdata/spx_ndx_combined")                   # <-- adjust if needed
PRICES   = Path("lake/minute")                                # point to your unadjusted minute lake root
OUTDIR   = Path("lake_adj/minute")                            # adjusted minute lake root
TICKERS  = Path("data/ticker_lists/spx_ndx_combined.json")    # or None to process all seen in the lake
WRITE_W  = 8                                                  # writer threads
ADJUST   = "both"                                             # "splits" or "both" (TR = splits+divs)

spl = pd.read_parquet(REFDIR / "stock_splits.parquet")
if "execution_date" not in spl.columns:
    raise SystemExit("stock_splits.parquet missing 'execution_date'")

spl["execution_date"] = pd.to_datetime(spl["execution_date"])
spl["ticker"] = spl["ticker"].astype(str).str.upper()

# group by ticker and emit a ±14d window around each split date
delta = pd.Timedelta(days=14)
rows = []
for tkr, g in spl.groupby("ticker"):
    for d in g["execution_date"].sort_values().unique():
        start = (d - delta).date().isoformat()
        end   = (d + delta).date().isoformat()
        rows.append((tkr, start, end))

# Print commands (one per window). Overlaps are fine; factor_builder will just overwrite those days.
for tkr, start, end in rows:
    print(
f"""python factor_builder.py \\
  --prices "{PRICES}" \\
  --refdir "{REFDIR}" \\
  --tickers "{TICKERS}" \\
  --start {start} --end {end} \\
  --granularity minute --minute-stream \\
  --outdir "{OUTDIR}" \\
  --adjust {ADJUST} --write-workers {WRITE_W}"""
    )
