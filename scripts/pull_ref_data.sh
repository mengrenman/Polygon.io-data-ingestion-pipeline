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

# ensure output dir exists
OUTDIR="$REPO_ROOT/refdata/$COLL"
mkdir -p "$OUTDIR"

# POLYGON_PROBE_DATES (optional, comma-separated YYYY-MM-DD): also resolve who held each ticker on those
# dates, so a recycled ticker's previous company gets its own id (+1 request per ticker per date).
# POLYGON_MIN_INTERVAL_SEC paces requests on rate-limited plans (see README troubleshooting).
python "$REPO_ROOT/legacy_scripts/run_pullers.py" \
  --tickers "$REPO_ROOT/data/ticker_lists/${COLL}.json" \
  --outdir  "$OUTDIR" \
  ${POLYGON_PROBE_DATES:+--probe-dates "$POLYGON_PROBE_DATES"}
