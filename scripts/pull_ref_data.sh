#!/usr/bin/env bash
set -euo pipefail

# resolve repo root relative to this script’s location
SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# collection you’re pulling (easy to reuse later)
COLL="spx_ndx_combined"

# load .env if present (for POLYGON_API_KEY)
if [ -f "$REPO_ROOT/.env" ]; then
  set -o allexport
  # shellcheck disable=SC1090
  source "$REPO_ROOT/.env"
  set +o allexport
fi

: "${POLYGON_API_KEY:?POLYGON_API_KEY is required (put it in .env or export it)}"

# Where refdata lives (override to keep it next to your lakes, e.g. /Users/you/local/parquet_lake/refdata)
REFDATA_ROOT="${POLYGON_REFDATA_ROOT:-$REPO_ROOT/refdata}"
OUTDIR="$REFDATA_ROOT/$COLL"
MARKET_DIR="${POLYGON_MARKET_DIR:-$REFDATA_ROOT/_market}"
mkdir -p "$OUTDIR"

# Default: BULK mode - market-wide tickers/splits/dividends tables (a few hundred requests, any universe
# size), then this collection's files are derived by filtering. Re-runs are incremental.
#   POLYGON_PER_TICKER=1        use the per-ticker pullers instead (several requests per ticker)
#   POLYGON_PROBE_DATES=d1,d2   also resolve who held each ticker on those dates (recycled tickers; +1 req/ticker/date)
#   POLYGON_EVENTS=1            also pull per-ticker symbol history (+1 req/ticker)
#   POLYGON_MIN_INTERVAL_SEC    pace requests on rate-limited plans (12.5 for 5 req/min; see README)
if [ "${POLYGON_PER_TICKER:-0}" = "1" ]; then
  python "$REPO_ROOT/legacy_scripts/run_pullers.py" \
    --tickers "$REPO_ROOT/data/ticker_lists/${COLL}.json" \
    --outdir  "$OUTDIR" \
    ${POLYGON_PROBE_DATES:+--probe-dates "$POLYGON_PROBE_DATES"}
else
  python "$REPO_ROOT/legacy_scripts/run_pullers.py" \
    --tickers "$REPO_ROOT/data/ticker_lists/${COLL}.json" \
    --outdir  "$OUTDIR" \
    --bulk --market-dir "$MARKET_DIR" \
    ${POLYGON_PROBE_DATES:+--probe-dates "$POLYGON_PROBE_DATES"} \
    ${POLYGON_EVENTS:+--events} \
    "$@"
fi
