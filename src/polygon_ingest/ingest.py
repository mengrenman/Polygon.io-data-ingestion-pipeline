#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Unified Polygon.io CSV.GZ (MINUTE or DAY aggregates) → Parquet lake + Manifest + Logging

Source layout (assumed):
  <src>/<YYYY>/<MM>/<YYYY-MM-DD>.csv.gz   (hyphens/underscores both OK)

Output layout:
  tf=minute : <out>/<TICKER>/<YYYY>/<MM>/<DD>.parquet
  tf=day    : <out>/<TICKER>/<YYYY>/<MM>.parquet

Highlights
- One codepath for both timeframes (tf={"minute","day"})
- Robust header detection & timestamp unit inference (s/ms/us/ns or ISO8601)
- Case-insensitive watchlist and --only TICKER filtering
- Stable progress bar that stays at 100%
- Optional manifest with progress bar and threaded scan
- Optional file logging with --log-file and --quiet-console

Examples:
  # Minute
  PYARROW_NUM_THREADS=1 \
  python ingest.py \
    --tf minute \
    --src /path/to/minute_aggs_v1 \
    --out /path/to/parquet_lake/minute_aggs_v1 \
    --workers 40 \
    --watch /path/to/tickers.json \
    --log-file /path/to/logs/minute_ingest.log \
    --write-manifest \
    --manifest-out /path/to/parquet_lake/minute_aggs_v1/manifest_minute.json \
    --manifest-workers 8 \
    --quiet-console

  # Day
  python ingest.py \
    --tf day \
    --src /path/to/day_aggs_v1 \
    --out /path/to/parquet_lake/day_aggs_v1 \
    --workers 40 \
    --watch /path/to/tickers.json \
    --log-file /path/to/logs/day_ingest.log \
    --write-manifest \
    --manifest-out /path/to/parquet_lake/day_aggs_v1/manifest_day.json \
    --manifest-workers 4 \
    --quiet-console
"""

from __future__ import annotations
import os, re, json, gzip, argparse, threading, time, sys
from pathlib import Path
from typing import Optional, Sequence, Tuple, Dict, List, Literal
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

# ── config ────────────────────────────────────────────────────────────────────
CHUNK_DEFAULT = 5_000_000
TS_CANDS      = ["window_start","t","timestamp","ts","epoch","start_time"]
TICKER_CANDS  = ["ticker","T","symbol","S"]
SHORTMAP      = {"o":"open","h":"high","l":"low","c":"close","v":"volume","n":"transactions","vw":"vwap"}
DATE_RE       = re.compile(r"(?P<y>\d{4})[^\d]?(?P<m>\d{2})(?:[^\d]?(?P<d>\d{2}))?")
LOCAL_TZ      = "US/Eastern"

Tf = Literal["minute", "day"]

# ── globals for inherited/IPC state ───────────────────────────────────────────
PROG_COUNTER = None   # set in pool initializer (for progress)
LOG_QUEUE = None      # mp queue set in pool initializer (for file logging)
QUIET_CONSOLE = False # main-thread setting

def pool_initializer(shared_counter, log_queue):
    """Workers inherit the counter & logging queue."""
    global PROG_COUNTER, LOG_QUEUE
    PROG_COUNTER = shared_counter
    LOG_QUEUE = log_queue

# ── lightweight logger (queue -> file) ────────────────────────────────────────
def start_logger_thread(log_path: Optional[Path], quiet_console: bool):
    """
    Returns: (queue, thread, stop_event)
    Put strings on the queue from ANY process to log.
    """
    import multiprocessing as mp
    try:
        ctx = mp.get_context("fork")
    except ValueError:
        ctx = mp.get_context("spawn")
    q = ctx.Queue()
    stop_evt = threading.Event()

    def _logger():
        f = None
        try:
            if log_path:
                log_path.parent.mkdir(parents=True, exist_ok=True)
                f = open(log_path, "a", buffering=1, encoding="utf-8")
            while not stop_evt.is_set() or not q.empty():
                try:
                    line = q.get(timeout=0.1)
                except Exception:
                    continue
                if f: f.write(line.rstrip("\n") + "\n")
                if not quiet_console:
                    tqdm.write(line.rstrip("\n"))
        finally:
            if f:
                f.flush(); f.close()

    t = threading.Thread(target=_logger, name="log-writer", daemon=True)
    t.start()
    return q, t, stop_evt

def print_critical(msg: str):
    try:
        tqdm.write(msg)
    except Exception:
        print(msg, flush=True)

def LOG(msg: str, *, critical: bool = False):
    if LOG_QUEUE:
        LOG_QUEUE.put(msg)
    if critical or not QUIET_CONSOLE:
        print_critical(msg)

# ── helpers ───────────────────────────────────────────────────────────────────
def detect_header(path: str) -> List[str]:
    with gzip.open(path, "rt") as f:
        return f.readline().strip().split(",")

def detect_col(header: List[str], candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in header:
            return c
    return None

def parse_year_month_from_path(p: Path) -> Tuple[int, int]:
    # Prefer parent dirs .../YYYY/MM/...
    try:
        y_dir = p.parent.parent.name
        m_dir = p.parent.name
        if len(y_dir) == 4 and y_dir.isdigit() and m_dir.isdigit():
            y, m = int(y_dir), int(m_dir)
            if 1 <= m <= 12:
                return y, m
    except Exception:
        pass
    # Fallback: filename
    m = DATE_RE.search(p.stem)
    if m:
        y, mo = int(m.group("y")), int(m.group("m"))
        if 1 <= mo <= 12:
            return y, mo
    raise ValueError(f"Cannot parse year/month from {p}")

def slice_owner(year: int, month: int, n_workers: int) -> int:
    return ((year - 2000) * 12 + (month - 1)) % n_workers

# robust ts conversion (ints s/ms/us/ns or ISO8601 strings)
def to_datetime_utc(series: pd.Series) -> pd.Series:
    s = series
    if s.dtype == object:
        all_digit = s.dropna().map(lambda x: str(x).isdigit()).all()
        if all_digit: s = s.astype("int64")
        else:         return pd.to_datetime(s, utc=True, errors="coerce")
    sample = int(pd.Series(s).dropna().iloc[0])
    if   sample > 1_000_000_000_000_000_000: unit = "ns"
    elif sample > 1_000_000_000_000_000:     unit = "us"
    elif sample > 1_000_000_000_000:         unit = "ms"
    else:                                     unit = "s"
    return pd.to_datetime(s, unit=unit, utc=True)

# ── worker (minute/day via tf switch) ─────────────────────────────────────────
def worker(
    csv_list: Sequence[str],
    out_root: Path,
    watch_upper: Optional[set[str]],
    only_upper: Optional[str],
    chunk: int,
    worker_id: int,
    tf: Tf,
):
    global PROG_COUNTER, LOG_QUEUE

    ts_col = None
    ticker_col = None
    dtypes: Dict[str,str] = {}
    usecols: List[str] = []

    rows_in = rows_kept = dropped_watch = dropped_only = 0

    if tf == "minute":
        # per (ticker,year,month,day)
        buckets: Dict[Tuple[str,int,int,int], List[pd.DataFrame]] = defaultdict(list)
    else:
        # per (ticker,year,month)
        buckets: Dict[Tuple[str,int,int], List[pd.DataFrame]] = defaultdict(list)

    try:
        for csv_path in csv_list:
            if PROG_COUNTER is not None:
                with PROG_COUNTER.get_lock():
                    PROG_COUNTER.value += 1

            # Detect header & build usecols/dtypes once
            if ts_col is None or ticker_col is None:
                header = detect_header(csv_path)
                ts_col     = detect_col(header, TS_CANDS)
                ticker_col = detect_col(header, TICKER_CANDS)
                if ts_col is None or ticker_col is None:
                    if LOG_QUEUE: LOG_QUEUE.put(f"[warn] missing ts/ticker in {csv_path}: {header}")
                    continue

                dtypes = {ticker_col: "string", ts_col: "object"}
                for want, dtype in (("open","float32"),("high","float32"),
                                    ("low","float32"),("close","float32"),
                                    ("volume","int64"),("transactions","int64"),
                                    ("o","float32"),("h","float32"),
                                    ("l","float32"),("c","float32"),
                                    ("v","int64"),("n","int64"),("vw","float32")):
                    if want in header: dtypes[want] = dtype
                usecols = list(dtypes)

            for df in pd.read_csv(
                csv_path,
                usecols=usecols,
                dtype=dtypes,
                compression="gzip",
                chunksize=chunk
            ):
                rows_in += len(df)

                # expand short columns if present
                ren = {k:v for k,v in SHORTMAP.items() if k in df.columns and v not in df.columns}
                if ren: df.rename(columns=ren, inplace=True)

                # unify ticker col
                if ticker_col != "ticker":
                    df.rename(columns={ticker_col: "ticker"}, inplace=True)
                df["ticker"] = df["ticker"].astype("string").str.upper()

                # filters
                if only_upper:
                    before = len(df)
                    df = df.loc[df["ticker"] == only_upper]
                    dropped_only += (before - len(df))
                    if df.empty: continue

                if watch_upper is not None:
                    before = len(df)
                    df = df.loc[df["ticker"].isin(watch_upper)]
                    dropped_watch += (before - len(df))
                    if df.empty: continue

                if df.empty: continue

                # timestamps → UTC + local clock column
                dt_utc = to_datetime_utc(df[ts_col])
                df["yr_utc"] = dt_utc.dt.year.astype("Int16")
                df["mo_utc"] = dt_utc.dt.month.astype("Int8")
                if tf == "minute":
                    df["day_utc"] = dt_utc.dt.day.astype("Int8")
                df["datetime"] = dt_utc.dt.tz_convert(LOCAL_TZ)

                if ts_col in df.columns:
                    df.drop(columns=[ts_col], inplace=True)

                need = ["yr_utc","mo_utc"] + (["day_utc"] if tf == "minute" else [])
                df = df.dropna(subset=need)
                if df.empty: continue

                rows_kept += len(df)

                # bucketize
                if tf == "minute":
                    for (sym, yr, mo, dd), sub in df.groupby(["ticker","yr_utc","mo_utc","day_utc"], observed=True):
                        buckets[(str(sym), int(yr), int(mo), int(dd))].append(sub)
                else:
                    for (sym, yr, mo), sub in df.groupby(["ticker","yr_utc","mo_utc"], observed=True):
                        buckets[(str(sym), int(yr), int(mo))].append(sub)

    finally:
        # Write parquet per bucket
        if tf == "minute":
            base_cols = ["datetime","ticker","open","high","low","close","volume","transactions","vwap","yr_utc","mo_utc","day_utc"]
            for (sym, yr, mo, dd), parts in buckets.items():
                outdir = out_root / sym / f"{yr:04d}" / f"{mo:02d}"
                outdir.mkdir(parents=True, exist_ok=True)
                fout_tmp = outdir / f"{dd:02d}.parquet.inprogress"
                fout     = outdir / f"{dd:02d}.parquet"
                final = pd.concat(parts, ignore_index=True)
                cols = [c for c in base_cols if c in final.columns] + [c for c in final.columns if c not in base_cols]
                final = final[cols].sort_values(["datetime","ticker"])
                table = pa.Table.from_pandas(final, preserve_index=False)
                pq.write_table(table, fout_tmp, compression="zstd")
                fout_tmp.replace(fout)
        else:
            base_cols = ["datetime","ticker","open","high","low","close","volume","transactions","vwap","yr_utc","mo_utc"]
            for (sym, yr, mo), parts in buckets.items():
                outdir = out_root / sym / f"{yr:04d}"
                outdir.mkdir(parents=True, exist_ok=True)
                fout_tmp = outdir / f"{mo:02d}.parquet.inprogress"
                fout     = outdir / f"{mo:02d}.parquet"
                final = pd.concat(parts, ignore_index=True)
                cols = [c for c in base_cols if c in final.columns] + [c for c in final.columns if c not in base_cols]
                final = final[cols].sort_values(["datetime","ticker"])
                table = pa.Table.from_pandas(final, preserve_index=False)
                pq.write_table(table, fout_tmp, compression="zstd")
                fout_tmp.replace(fout)

        if LOG_QUEUE:
            LOG_QUEUE.put(
                f"[worker {worker_id:3d}] rows_in={rows_in:,} rows_kept={rows_kept:,} "
                f"dropped_only={dropped_only:,} dropped_watch={dropped_watch:,} "
                f"written_files={len(buckets)}"
            )

    return worker_id, len(buckets)

# ── main progress thread (stays at 100%) ─────────────────────────────────────
def progress_thread(total_files, counter, stop_event):
    pbar = tqdm(total=total_files, desc="Progress", dynamic_ncols=True, leave=True)
    last = 0
    try:
        while True:
            with counter.get_lock():
                cur = counter.value
            if cur > last:
                pbar.update(cur - last); last = cur
            if last >= total_files:
                pbar.n = total_files; pbar.refresh(); break
            if stop_event.is_set():
                if last < total_files: pbar.update(total_files - last)
                pbar.refresh(); break
            time.sleep(0.1)
    finally:
        pbar.close()

# ── manifest builder with progress bar (and optional threads) ────────────────
def _scan_one_parquet(ticker: str, p: Path):
    """Return a manifest entry dict for a single parquet file."""
    try:
        df = pd.read_parquet(p, columns=["datetime"])
        if df.empty:
            return ticker, None
        start = df["datetime"].min()
        end   = df["datetime"].max()
        rows  = int(len(df))
        return ticker, {"path": str(p), "start": str(start), "end": str(end), "rows": rows}
    except Exception as ex:
        return ticker, f"[WARN] manifest: failed reading {p}: {ex}"

def build_manifest(out_root: Path, manifest_path: Path, logger=None, workers: int = 4) -> None:
    """
    Build a manifest JSON listing each parquet file and its datetime min/max.
    Shows a tqdm progress bar over all parquet files, optionally threaded.
    """
    t0 = time.time()
    print_critical("Building manifest: scanning parquet files ...")
    if logger: logger("Building manifest: scanning parquet files ...")

    # Gather all (ticker, path) pairs
    pairs: List[tuple[str, Path]] = []
    for ticker_dir in sorted(p for p in out_root.iterdir() if p.is_dir()):
        ticker = ticker_dir.name
        for p in sorted(ticker_dir.rglob("*.parquet")):
            pairs.append((ticker, p))

    total = len(pairs)
    manifest: Dict[str, List[Dict[str, str]]] = {}
    errors: List[str] = []

    if total == 0:
        print_critical("[INFO] No parquet files found to include in manifest.")
    else:
        with tqdm(total=total, desc="Manifest", unit="file", dynamic_ncols=True, leave=True) as pbar:
            max_workers = max(1, int(workers))
            if max_workers > 1:
                # THREAD-POOL SCAN (I/O bound; keep PYARROW_NUM_THREADS=1 to avoid oversubscription)
                with ThreadPoolExecutor(max_workers=max_workers) as ex:
                    futs = [ex.submit(_scan_one_parquet, t, p) for (t, p) in pairs]
                    for fut in as_completed(futs):
                        t, res = fut.result()
                        if isinstance(res, str):
                            errors.append(res)
                        elif res:
                            manifest.setdefault(t, []).append(res)
                        pbar.update(1)
            else:
                # SEQUENTIAL SCAN
                for t, p in pairs:
                    _, res = _scan_one_parquet(t, p)
                    if isinstance(res, str):
                        errors.append(res)
                    elif res:
                        manifest.setdefault(t, []).append(res)
                    pbar.update(1)

    # Now write JSON (fast)
    print_critical("Writing manifest JSON ...")
    if logger: logger("Writing manifest JSON ...")
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)

    dt = time.time() - t0
    done_msg = f"[INFO] Manifest written to {manifest_path} (files: {total}, tickers: {len(manifest)}, took {dt:.2f}s)"
    print_critical(done_msg)
    if logger: logger(done_msg)
    if errors:
        for e in errors[:10]:
            print_critical(e)
            if logger: logger(e)
        if len(errors) > 10:
            more = f"... plus {len(errors)-10} more errors during manifest scan."
            print_critical(more)
            if logger: logger(more)

# ── driver ───────────────────────────────────────────────────────────────────
def run_ingest(
    tf: Tf,
    src_root: Path,
    out_root: Path,
    *,
    watch: Optional[Path] = None,
    only: Optional[str] = None,
    workers: int = os.cpu_count()//2 or 1,
    chunk: int = CHUNK_DEFAULT,
    log_file: Optional[Path] = None,
    quiet_console: bool = False,
    write_manifest: bool = False,
    manifest_out: Optional[Path] = None,
    manifest_workers: int = 4,
):
    import multiprocessing as mp

    out_root.mkdir(parents=True, exist_ok=True)

    # Start logger thread
    global LOG_QUEUE, QUIET_CONSOLE
    QUIET_CONSOLE = bool(quiet_console)
    LOG_QUEUE, log_thread, log_stop = start_logger_thread(log_file, QUIET_CONSOLE)

    # CRITICAL startup messages
    LOG(f"[INFO] Starting {tf.upper()} ingest", critical=True)
    LOG(f"[INFO] Source: {src_root}", critical=True)
    LOG(f"[INFO] Output: {out_root}", critical=True)
    if watch: LOG(f"[INFO] Watchlist: {watch}", critical=True)
    if only:  LOG(f"[INFO] Only: {only.upper()}", critical=True)

    # Load watch list
    def load_watch_upper(path: Optional[Path]) -> Optional[set[str]]:
        if not path: return None
        if not path.exists(): raise FileNotFoundError(str(path))
        if path.suffix.lower() == ".json":
            vals = json.load(open(path))
        else:
            vals = [line.strip() for line in open(path) if line.strip()]
        return {str(x).upper() for x in vals}
    watch_upper = load_watch_upper(watch)
    only_upper = only.upper() if only else None

    n_workers = max(1, int(workers))
    chunk = int(chunk)

    # Collect files
    all_csvs: List[str] = sorted(str(p) for p in src_root.rglob("*.csv.gz"))
    if not all_csvs:
        LOG(f"[ERROR] No .csv.gz files under {src_root}", critical=True)
        log_stop.set(); log_thread.join(); sys.exit(1)

    LOG(f"[INFO] Found {len(all_csvs)} source files (.csv.gz)", critical=True)

    # Partition by (year, month) to distribute work deterministically
    owned_csvs: List[List[str]] = [[] for _ in range(n_workers)]
    skipped: List[str] = []
    for path in all_csvs:
        try:
            y, m = parse_year_month_from_path(Path(path))
        except Exception:
            skipped.append(path); continue
        owner = slice_owner(y, m, n_workers)
        owned_csvs[owner].append(path)
    if skipped:
        LOG(f"[warn] {len(skipped)} files skipped (cannot parse YYYY/MM); first 10:", critical=True)
        for s in skipped[:10]: LOG(f"  {s}", critical=True)

    total_files = sum(len(lst) for lst in owned_csvs)
    LOG(f"[INFO] Total files scheduled for processing: {total_files}", critical=True)

    # Multiprocessing context
    try:
        ctx = mp.get_context("fork")
    except ValueError:
        ctx = mp.get_context("spawn")

    # Progress bar
    progress_counter = ctx.Value('i', 0)
    stop_event = threading.Event()
    LOG("[INFO] Launching workers ...", critical=True)
    thr = threading.Thread(
        target=progress_thread,
        args=(total_files, progress_counter, stop_event),
        daemon=False
    )
    thr.start()

    # Process pool
    with ProcessPoolExecutor(
        max_workers=n_workers,
        mp_context=ctx,
        initializer=pool_initializer,
        initargs=(progress_counter, LOG_QUEUE)
    ) as ex:
        futures = []
        for wid in range(n_workers):
            futures.append(ex.submit(
                worker, owned_csvs[wid], out_root, watch_upper, only_upper, chunk, wid, tf
            ))
        for f in futures:
            wid, nkeys = f.result()
            LOG(f"worker {wid:3d}: wrote {nkeys} parquet partitions")

    # Finish progress
    with progress_counter.get_lock():
        progress_counter.value = total_files
    stop_event.set()
    thr.join()
    LOG("[INFO] All workers completed.", critical=True)

    # Manifest (with progress bar)
    if write_manifest:
        default_name = f"manifest_{tf}.json"
        manifest_path = manifest_out if manifest_out else (out_root / default_name)
        build_manifest(out_root, manifest_path, logger=LOG, workers=max(1, manifest_workers))

    # Stop logger thread
    log_stop.set()
    log_thread.join()
    LOG("[INFO] Done.", critical=True)

# ── CLI ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="Batch ingest Polygon CSV.GZ (MINUTE or DAY) → Parquet + optional manifest + logging"
    )
    ap.add_argument("--tf", required=True, choices=["minute","day"], help="timeframe")
    ap.add_argument("--src", required=True, type=Path, help="root folder (supports nested YYYY/MM)")
    ap.add_argument("--out", required=True, type=Path, help="destination Parquet lake")
    ap.add_argument("--watch", type=Path, default=None, help="JSON/TXT ticker list (optional, case-insensitive)")
    ap.add_argument("--only", type=str, default=None, help="Restrict to a single ticker (e.g., AAPL)")
    ap.add_argument("--workers", type=int, default=os.cpu_count()//2 or 1, help="parallel workers")
    ap.add_argument("--chunk", type=int, default=CHUNK_DEFAULT, help="rows per pandas.read_csv chunk")
    # Manifest options
    ap.add_argument("--write-manifest", action="store_true", help="After ingest, write a manifest JSON")
    ap.add_argument("--manifest-out", type=Path, default=None, help="Path for manifest JSON (default: <out>/manifest_<tf>.json)")
    ap.add_argument("--manifest-workers", type=int, default=8, help="Threads to scan parquet files for manifest (>=1)")
    # Logging
    ap.add_argument("--log-file", type=Path, default=None, help="Write logs to this file")
    ap.add_argument("--quiet-console", action="store_true", help="Reduce console output (keep progress bar)")
    args = ap.parse_args()

    run_ingest(
        tf=args.tf,
        src_root=args.src,
        out_root=args.out,
        watch=args.watch,
        only=args.only,
        workers=args.workers,
        chunk=args.chunk,
        log_file=args.log_file,
        quiet_console=args.quiet_console,
        write_manifest=args.write_manifest,
        manifest_out=args.manifest_out,
        manifest_workers=args.manifest_workers,
    )
