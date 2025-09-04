#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<USAGE
Build adjusted parquet lakes (split-adjusted + total-return) from unadjusted lakes + refdata.

Usage:
  $(basename "$0") -t <minute|day> -c <collection> [options]

Required:
  -t, --tf <minute|day>         Timeframe to build
  -c, --collection <name>       Ticker collection (expects:
                                data/ticker_lists/<name>.json and refdata/<name>/)

Options:
  -p, --prices <dir>            Unadjusted lake root (defaults: see below)
  -o, --outdir <dir>            Adjusted lake root (defaults: see below)
  -s, --start YYYY-MM-DD        Optional start date filter
  -e, --end   YYYY-MM-DD        Optional end date filter
  -w, --workers N               CPU workers (day mode; default 40)
  -W, --write-workers N         Writers (default 8)
  -S, --stream-read-workers N   Minute streaming readers (default 8, minute only)
  -m, --materialize <minimal|full>  (default minimal)
  -v, --verbose
  -n, --dry-run                 Print the command, don’t run
  -h, --help

Defaults:
  prices (day):    \$HOME/data/polygonio_data/parquet_lake/day_aggs_v1/\$COLL
  prices (minute): \$HOME/data/polygonio_data/parquet_lake/minute_aggs_v1/\$COLL
  outdir (day):    \$HOME/data/polygonio_data/parquet_lake/day_aggs_v1/\${COLL}_adjusted
  outdir (minute): \$HOME/data/polygonio_data/parquet_lake/minute_aggs_v1/\${COLL}_adjusted
USAGE
}

# Resolve repo root no matter where we run from
SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Defaults
TF=""
COLL=""
PRICES=""
OUTDIR=""
START=""
END=""
WORKERS=90
WRITE_WORKERS=8
STREAM_READ_WORKERS=8
MATERIALIZE="minimal"
VERBOSE=0
DRYRUN=0

# Parse args
while [[ $# -gt 0 ]]; do
  case "$1" in
    -t|--tf) TF="$2"; shift 2 ;;
    -c|--collection) COLL="$2"; shift 2 ;;
    -p|--prices) PRICES="$2"; shift 2 ;;
    -o|--outdir) OUTDIR="$2"; shift 2 ;;
    -s|--start) START="$2"; shift 2 ;;
    -e|--end) END="$2"; shift 2 ;;
    -w|--workers) WORKERS="$2"; shift 2 ;;
    -W|--write-workers) WRITE_WORKERS="$2"; shift 2 ;;
    -S|--stream-read-workers) STREAM_READ_WORKERS="$2"; shift 2 ;;
    -m|--materialize) MATERIALIZE="$2"; shift 2 ;;
    -v|--verbose) VERBOSE=1; shift ;;
    -n|--dry-run) DRYRUN=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1"; usage; exit 1 ;;
  esac
done

[[ -z "$TF" || -z "$COLL" ]] && { echo "Missing -t/--tf or -c/--collection"; usage; exit 1; }
[[ "$TF" != "minute" && "$TF" != "day" ]] && { echo "tf must be minute or day"; exit 1; }

TICKERS_JSON="$REPO_ROOT/data/ticker_lists/${COLL}.json"
REFDIR="$REPO_ROOT/refdata/${COLL}"
[[ -f "$TICKERS_JSON" ]] || { echo "Missing $TICKERS_JSON"; exit 1; }
[[ -d "$REFDIR" ]] || { echo "Missing $REFDIR"; exit 1; }

# Default PRICES/OUTDIR if not provided
if [[ -z "$PRICES" ]]; then
  if [[ "$TF" == "day" ]]; then
    PRICES="$HOME/data/polygonio_data/parquet_lake/day_aggs_v1/$COLL"
  else
    PRICES="$HOME/data/polygonio_data/parquet_lake/minute_aggs_v1/$COLL"
  fi
fi
if [[ -z "$OUTDIR" ]]; then
  if [[ "$TF" == "day" ]]; then
    OUTDIR="$HOME/data/polygonio_data/parquet_lake/day_aggs_v1/${COLL}_adjusted"
  else
    OUTDIR="$HOME/data/polygonio_data/parquet_lake/minute_aggs_v1/${COLL}_adjusted"
  fi
fi

# Pick factor_builder path (legacy or repo root)
FB="$REPO_ROOT/legacy_scripts/factor_builder.py"
[[ -f "$FB" ]] || FB="$REPO_ROOT/factor_builder.py"

CMD=( python "$FB"
  --prices "$PRICES"
  --refdir "$REFDIR"
  --tickers "$TICKERS_JSON"
  --granularity "$TF"
  --outdir "$OUTDIR"
  --adjust both
  --write-workers "$WRITE_WORKERS"
  --materialize "$MATERIALIZE"
)

# Timeframe specific
if [[ "$TF" == "minute" ]]; then
  CMD+=( --minute-stream --stream-read-workers "$STREAM_READ_WORKERS" )
else
  CMD+=( --workers "$WORKERS" )
fi

# Optional dates
[[ -n "$START" ]] && CMD+=( --start "$START" )
[[ -n "$END"   ]] && CMD+=( --end   "$END" )
[[ "$VERBOSE" -eq 1 ]] && CMD+=( --verbose )

echo "+ ${CMD[*]}"
if [[ "$DRYRUN" -eq 0 ]]; then
  "${CMD[@]}"
fi
