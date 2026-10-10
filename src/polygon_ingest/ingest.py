#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Unified Polygon.io CSV.GZ (MINUTE or DAY aggregates) → Parquet lake + Manifest + Logging

Source layout (assumed):
  <src>/<YYYY>/<MM>/<YYYY-MM-DD>.csv.gz   (hyphens/underscores both OK)

Output layout:
  tf=minute : <out>/<TICKER>/<YYYY>/<MM>/<DD>.parquet
  tf=day    : <out>/<TICKER>/<YYYY>/<MM>.parquet
  YYYY/MM/DD is the US/Eastern *trading date* (not the UTC date). The 'datetime' column is
  tz-aware US/Eastern and prices are float64.

Highlights
- One codepath for both timeframes (tf={"minute","day"})
- Robust header detection & timestamp unit inference (s/ms/us/ns or ISO8601)
- Tickers stored exactly as Polygon spells them (the case carries the share class); watchlist and
  --only match exactly, with --ignore-case for lists typed in the wrong case (see polygon_ingest.tickers)
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
import os, re, json, gzip, argparse, threading, time, sys, uuid
import datetime as _dt
from pathlib import Path
from typing import Callable, Optional, Sequence, Tuple, Dict, List, Literal
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from tqdm import tqdm

try:
    from .tickers import case_collisions, clean_list, fs_folds_case
except ImportError:      # running this file directly (python ingest.py ...), as the docstring shows
    from tickers import case_collisions, clean_list, fs_folds_case

# ── config ────────────────────────────────────────────────────────────────────
CHUNK_DEFAULT = 5_000_000
TS_CANDS      = ["window_start","t","timestamp","ts","epoch","start_time"]
TICKER_CANDS  = ["ticker","T","symbol","S"]
SHORTMAP      = {"o":"open","h":"high","l":"low","c":"close","v":"volume","n":"transactions","vw":"vwap"}
DATE_RE       = re.compile(r"(?P<y>\d{4})[^\d]?(?P<m>\d{2})(?:[^\d]?(?P<d>\d{2}))?")
LOCAL_TZ      = "US/Eastern"
# Footer key a market minute file shares with its .idx.parquet sidecar: one random token per write, so a reader can
# tell that the sidecar describes this version of the file (lake_io.read_day_index; factor_builder mirrors it).
SIDECAR_PAIR_KEY = b"polygon_ingest.sidecar_pair"

Tf = Literal["minute", "day"]
Layout = Literal["ticker", "market"]   # ticker: <out>/<TICKER>/<YYYY>/<MM>[/<DD>]; market: <out>/<YYYY>/<MM>[/<DD>] all tickers

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

# ── layouts, bounded-memory flushing ──────────────────────────────────────────
def file_session(p: Path) -> Optional[_dt.date]:
    """The session a flat file holds, from its YYYY-MM-DD name; None when the name carries no full date."""
    mm = re.search(r"(\d{4})[-_](\d{2})[-_](\d{2})", Path(p).stem)
    if not mm:
        return None
    try:
        return _dt.date(int(mm.group(1)), int(mm.group(2)), int(mm.group(3)))
    except ValueError:
        return None


def source_period(p: Path, tf: Tf) -> Tuple[int, ...]:
    """(y, m) for day, (y, m, d) for minute, from the source file's YYYY/MM dirs and YYYY-MM-DD name."""
    y, m = parse_year_month_from_path(p)
    if tf == "day":
        return (y, m)
    mm = re.search(r"(\d{4})[-_](\d{2})[-_](\d{2})", p.stem)
    if mm and int(mm.group(1)) == y and int(mm.group(2)) == m:
        return (y, m, int(mm.group(3)))
    return (y, m, 1)   # day unknown: conservative (flushes later, never earlier)


def _flushable(bucket_keys: List[tuple], cur: Tuple[int, ...], tf: Tf) -> List[tuple]:
    """
    Bucket keys that can no longer receive rows and can be written now. Bucket keys end in (yr, mo[, dd]).
    Rows from a source file land on that file's ET trading date or, at most, an adjacent calendar day, so
    once the worker is processing month M, months <= M-2 are final (day); once it is processing date D,
    dates < D-1 are final (minute). This bounds a worker's memory to about two periods instead of all of them.
    """
    out: List[tuple] = []
    if tf == "day":
        cur_idx = cur[0] * 12 + cur[1]
        for k in bucket_keys:
            if k[-2] * 12 + k[-1] <= cur_idx - 2:
                out.append(k)
    else:
        import datetime as _dt
        cur_d = _dt.date(cur[0], cur[1], cur[2])
        for k in bucket_keys:
            if _dt.date(k[-3], k[-2], k[-1]) < cur_d - _dt.timedelta(days=1):
                out.append(k)
    return out


def day_index(df: pd.DataFrame) -> pd.DataFrame:
    """Per-ticker index of a ticker-major minute day file: first/last close, row count and row positions.
    Written as <DD>.idx.parquet next to market-layout minute files so an adjuster can scan a day's edges
    without reading the data columns, and readers can skip files a ticker is absent from."""
    pos = np.arange(len(df))
    g = pd.DataFrame({"ticker": df["ticker"].to_numpy(), "close": df["close"].to_numpy(), "_pos": pos}).groupby("ticker", sort=True)
    idx = g.agg(first_close=("close", "first"), last_close=("close", "last"), n_rows=("close", "size"),
                row_start=("_pos", "min"), row_end=("_pos", "max")).reset_index()
    # groupby drops null keys, so rows without a ticker would sit outside every range and a reader slicing by
    # the sidecar would never see them; the worker drops them before this point
    if int(idx["n_rows"].sum()) != len(df):
        raise ValueError(f"sidecar would cover {int(idx['n_rows'].sum()):,} of {len(df):,} rows "
                         f"({int(df['ticker'].isna().sum()):,} without a ticker)")
    return idx


def _with_pair_token(table: pa.Table, token: bytes) -> pa.Table:
    return table.replace_schema_metadata({**(table.schema.metadata or {}), SIDECAR_PAIR_KEY: token})


def _write_with_sidecar(table: pa.Table, fout: Path, index: pd.DataFrame, row_group_size: int) -> None:
    """Write a market minute file and its <DD>.idx.parquet so that the sidecar on disk never describes another
    version of the data file. Both go to temp names first, carrying one token in their footers; the old sidecar
    is removed before the data file is renamed into place and the new one is renamed in after it. A crash in
    between leaves the file without a sidecar (readers then scan it), never beside a stale one: a stale sidecar
    slices the new file at the old row positions and returns another ticker's bars under the requested name."""
    token = uuid.uuid4().hex.encode()
    idx_path = fout.with_name(fout.stem + ".idx.parquet")
    tmp, idx_tmp = fout.with_name(fout.name + ".inprogress"), idx_path.with_name(idx_path.name + ".inprogress")
    pq.write_table(_with_pair_token(table, token), tmp, compression="zstd", row_group_size=row_group_size)
    pq.write_table(_with_pair_token(pa.Table.from_pandas(index, preserve_index=False), token), idx_tmp)
    idx_path.unlink(missing_ok=True)
    tmp.replace(fout)
    idx_tmp.replace(idx_path)


def _clashes(key: tuple, layout: Layout, fold_guard: bool, spelling_of: Dict[str, str],
             clashes: set) -> bool:
    """True when this bucket's ticker would be written to a directory another spelling already owns.

    Only the ticker layout names a directory after a symbol, and only a case-folding filesystem makes
    <out>/AAP and <out>/AAp the same directory. There the second write silently replaces the first, so
    the bucket is refused and the run fails rather than losing one of the two securities.
    """
    if not (fold_guard and layout == "ticker"):
        return False
    ticker = str(key[0])
    first = spelling_of.setdefault(ticker.upper(), ticker)
    if first != ticker:
        clashes.add((first, ticker))
        return True
    return False


def _bucket_period(key: tuple, tf: Tf) -> tuple:
    """(yr, mo) of a day bucket, (yr, mo, dd) of a minute bucket; bucket keys end in it in either layout."""
    return tuple(key[-3:] if tf == "minute" else key[-2:])


def _bucket_path(out_root: Path, key: tuple, tf: Tf, layout: Layout) -> Path:
    """ticker layout <out>/<TICKER>/<YYYY>/<MM>[/<DD>].parquet, market layout <out>/<YYYY>/<MM>[/<DD>].parquet."""
    root = out_root / str(key[0]) if layout == "ticker" else out_root
    if tf == "minute":
        yr, mo, dd = _bucket_period(key, tf)
        return root / f"{yr:04d}" / f"{mo:02d}" / f"{dd:02d}.parquet"
    yr, mo = _bucket_period(key, tf)
    return root / f"{yr:04d}" / f"{mo:02d}.parquet"


def _write_bucket(out_root: Path, key: tuple, parts: List[pd.DataFrame], tf: Tf, layout: Layout) -> None:
    """Write one bucket at _bucket_path, replacing the file there whole."""
    base_cols = ["datetime", "ticker", "open", "high", "low", "close", "volume", "transactions", "vwap", "yr_et", "mo_et"] + (["day_et"] if tf == "minute" else [])
    final = pd.concat(parts, ignore_index=True)
    cols = [c for c in base_cols if c in final.columns] + [c for c in final.columns if c not in base_cols]
    if layout == "market":
        # ticker-major so one symbol is a contiguous slice and row-group statistics can prune it on read
        final = final[cols].sort_values(["ticker", "datetime"])
        row_group_size: Optional[int] = 131_072
    else:
        final = final[cols].sort_values(["datetime", "ticker"])
        row_group_size = None
    fout = _bucket_path(out_root, key, tf, layout)
    fout.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(final, preserve_index=False)
    if layout == "market" and tf == "minute" and "close" in final.columns:
        _write_with_sidecar(table, fout, day_index(final.reset_index(drop=True)), row_group_size)
        return
    fout_tmp = fout.with_name(fout.name + ".inprogress")
    if row_group_size:
        pq.write_table(table, fout_tmp, compression="zstd", row_group_size=row_group_size)
    else:
        pq.write_table(table, fout_tmp, compression="zstd")
    fout_tmp.replace(fout)


def _uncovered(key: tuple, covered: Optional[frozenset], out_root: Path, tf: Tf, layout: Layout) -> bool:
    """True when bucket `key` would replace an existing file whose period --src holds no flat file for. Its rows
    were stamped into that month or session by a neighboring file (the 2019-08-12 day file carries 29 rows dated
    08-13), and writing them would replace the whole file with those few rows. `covered` None: no guard."""
    return (covered is not None and _bucket_period(key, tf) not in covered
            and _bucket_path(out_root, key, tf, layout).exists())


def _lake_sessions(files: Sequence[Path], threads: int = 8) -> set:
    """ET trading dates present in lake files (their `datetime` column; tz-naive values are read as UTC)."""
    def one(f: Path) -> set:
        s = pd.Series(pc.unique(pq.read_table(f, columns=["datetime"]).column("datetime")).to_pandas())
        s = pd.to_datetime(s)
        if s.dt.tz is None:
            s = s.dt.tz_localize("UTC")
        return set(s.dt.tz_convert(LOCAL_TZ).dt.date)
    with ThreadPoolExecutor(max_workers=max(1, threads)) as ex:
        return set().union(*ex.map(one, files)) if files else set()


def month_coverage_gaps(out_root: Path, csvs: Sequence[str], layout: Layout,
                        rewrites: Optional[Callable[[str], bool]] = None) -> Dict[Tuple[int, int], List[_dt.date]]:
    """Sessions a day run would drop. A day lake is one file per month (per ticker in the ticker layout) and the
    ingester rewrites each month it has rows for from those rows alone, with no date filter, so a refresh tree
    starting 2025-08-14 would replace day/all/2025/08.parquet with the sessions after 08-13 alone. Returns, for each month
    --src touches whose file already exists, the sessions in that file that --src holds no flat file for.
    `rewrites` limits a ticker-layout check to the symbol directories the run writes (its --watch / --only)."""
    have: Dict[Tuple[int, int], set] = defaultdict(set)
    months: set = set()
    for c in csvs:
        try:
            months.add(parse_year_month_from_path(Path(c)))
        except ValueError:
            continue
        d = file_session(Path(c))
        if d is not None:
            have[(d.year, d.month)].add(d)
    gaps: Dict[Tuple[int, int], List[_dt.date]] = {}
    for y, m in sorted(months):
        rel = Path(f"{y:04d}") / f"{m:02d}.parquet"
        files = ([out_root / rel] if layout == "market" else sorted(out_root.glob(f"*/{rel}")))
        files = [f for f in files if f.is_file() and (layout == "market" or rewrites is None or rewrites(f.parent.parent.name))]
        if not files:
            continue
        missing = sorted(_lake_sessions(files) - have[(y, m)])
        if missing:
            gaps[(y, m)] = missing
    return gaps


def _selection(watch: Optional[set], only: Optional[str], ignore_case: bool) -> Optional[Callable[[str], bool]]:
    """The tickers a run keeps, as the worker filters them (--only, then --watch); None when it keeps every ticker."""
    if watch is None and only is None:
        return None
    fold = (lambda t: t.upper()) if ignore_case else (lambda t: t)
    wanted = {fold(t) for t in watch} if watch is not None else None
    return lambda t: (only is None or fold(t) == fold(only)) and (wanted is None or fold(t) in wanted)


def _market_file_tickers(f: Path, tf: Tf) -> set:
    """Tickers in a market-layout file: a minute file's sidecar when it describes the file, else the ticker column."""
    if tf == "minute":
        try:
            from .lake_io import read_day_index
        except ImportError:   # lake_io imports this module, so not at the top
            from lake_io import read_day_index
        idx = read_day_index(f)
        if idx is not None:
            return set(idx["ticker"])
    return set(pc.unique(pq.read_table(f, columns=["ticker"]).column("ticker")).to_pylist())


def market_subset_losses(out_root: Path, periods: Sequence[tuple], tf: Tf, keeps: Callable[[str], bool],
                         threads: int = 8) -> Dict[Path, Tuple[int, List[str]]]:
    """Tickers a --watch / --only run would strip from a market lake. A market file holds every ticker of its month
    (day) or session (minute), and the worker replaces it whole with the rows of the tickers `keeps` selects, so
    `poly bars --layout market --watch list.json --out lake/day/all` would leave each month it touches holding the
    list alone. Returns, for each existing file of `periods` ((yr, mo) day, (yr, mo, dd) minute) that holds tickers
    outside the selection, how many there are and the first few. Rows without a ticker are dropped on ingest anyway."""
    files = sorted(p for p in (_bucket_path(out_root, k, tf, "market") for k in set(periods)) if p.is_file())

    def one(f: Path):
        lost = sorted(t for t in _market_file_tickers(f, tf) if t and str(t).strip() and not keeps(t))
        return f, (len(lost), lost[:5])
    with ThreadPoolExecutor(max_workers=max(1, threads)) as ex:
        return {f: lost for f, lost in ex.map(one, files) if lost[0]}


# ── worker (minute/day via tf switch) ─────────────────────────────────────────
def worker(
    csv_list: Sequence[str],
    out_root: Path,
    watch: Optional[set[str]],
    only: Optional[str],
    chunk: int,
    worker_id: int,
    tf: Tf,
    layout: Layout = "ticker",
    ignore_case: bool = False,
    fold_guard: bool = False,
    covered: Optional[frozenset] = None,
):
    global PROG_COUNTER, LOG_QUEUE

    ts_col = None
    ticker_col = None
    dtypes: Dict[str,str] = {}
    usecols: List[str] = []

    rows_in = rows_kept = dropped_watch = dropped_only = dropped_null = 0
    untouched: Dict[str, int] = {}   # existing files a stray bucket was not allowed to replace (see _uncovered) -> rows
    watch_upper = {t.upper() for t in watch} if watch is not None else None
    only_upper = only.upper() if only is not None else None
    matched: set[str] = set()        # watchlist symbols this worker actually saw
    near_misses: set[str] = set()    # symbols dropped that differ from a watchlist entry only in case
    classified: set[str] = set()     # spellings already checked against the watchlist (see below)
    # <out>/<TICKER> is one directory per symbol, so on a case-folding filesystem AAP and AAp are the
    # same path. Source files are sharded by (year, month), so both spellings of a symbol in the same
    # period always reach the same worker - a per-worker map catches every collision that could occur.
    spelling_of: Dict[str, str] = {}
    clashes: set[Tuple[str, str]] = set()

    # bucket key: ticker layout (ticker, yr, mo[, dd]); market layout (yr, mo[, dd]). Buckets are written as
    # soon as they can no longer receive rows (see _flushable), bounding memory to ~2 periods per worker.
    buckets: Dict[tuple, List[pd.DataFrame]] = defaultdict(list)
    written = 0

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
                # float64: float32 carries ~7 significant digits, which loses cents above ~$100k
                # (BRK.A) and is marginal in the tens of thousands. zstd keeps the size cost small.
                for want, dtype in (("open","float64"),("high","float64"),
                                    ("low","float64"),("close","float64"),
                                    ("volume","int64"),("transactions","int64"),
                                    ("o","float64"),("h","float64"),
                                    ("l","float64"),("c","float64"),
                                    ("v","int64"),("n","int64"),("vw","float64")):
                    if want in header: dtypes[want] = dtype
                usecols = list(dtypes)

            for df in pd.read_csv(
                csv_path,
                usecols=usecols,
                dtype=dtypes,
                compression="gzip",
                chunksize=chunk,
                # Only an empty field is missing: pandas' default NA tokens include "NA", which is a real
                # ticker (Nano Labs), and would silently turn every one of its rows into a null ticker.
                keep_default_na=False,
                na_values=[""],
            ):
                rows_in += len(df)

                # expand short columns if present
                ren = {k:v for k,v in SHORTMAP.items() if k in df.columns and v not in df.columns}
                if ren: df.rename(columns=ren, inplace=True)

                # unify ticker col
                if ticker_col != "ticker":
                    df.rename(columns={ticker_col: "ticker"}, inplace=True)
                # Stored exactly as Polygon writes it: the case is the share class. AAp is the Alcoa
                # $3.75 preferred, a different security from AAP (Advance Auto Parts); upper-casing
                # merged the two into one symbol. See polygon_ingest.tickers.
                df["ticker"] = df["ticker"].astype("string")
                # A bar with no symbol belongs to no security. Polygon's minute files hold 365 such rows in 37
                # sessions (2006-08/09, 2013-11/12, 2014-01), all-zero bars that used to land after the last
                # ticker block of the day file and outside every sidecar range.
                blank = df["ticker"].fillna("").str.strip().eq("").to_numpy(dtype=bool)
                if blank.any():
                    dropped_null += int(blank.sum())
                    df = df.loc[~blank]
                    if df.empty: continue

                # filters
                if only is not None:
                    before = len(df)
                    hit = df["ticker"].str.upper().isin([only_upper]) if ignore_case else df["ticker"].isin([only])
                    df = df.loc[hit]
                    dropped_only += (before - len(df))
                    if df.empty: continue

                if watch is not None:
                    before = len(df)
                    if ignore_case:
                        keep = df["ticker"].str.upper().isin(watch_upper)
                    else:
                        keep = df["ticker"].isin(watch)
                        # A symbol that differs from a watchlist entry only in case is a *different*
                        # security, so it is dropped - but silently dropping it is the surprise a
                        # caller most needs told about, so collect the spellings for the summary.
                        # Only new spellings are upper-cased, which keeps this off the per-row path.
                        fresh = set(df.loc[~keep, "ticker"].drop_duplicates().dropna()) - classified
                        if fresh:
                            classified |= fresh
                            near_misses |= {t for t in fresh if t.upper() in watch_upper}
                    matched.update(df.loc[keep, "ticker"].drop_duplicates().dropna())
                    df = df.loc[keep]
                    dropped_watch += (before - len(df))
                    if df.empty: continue

                if df.empty: continue

                # timestamps → tz-aware US/Eastern; partition on the ET *trading date*.
                # Polygon minute bars run 04:00–20:00 ET, so partitioning on the UTC date would
                # push bars from 19:00/20:00 ET onward into the next day's file (and give them
                # the next day's split/dividend factors downstream).
                dt_utc = to_datetime_utc(df[ts_col])
                dt_et  = dt_utc.dt.tz_convert(LOCAL_TZ)
                df["yr_et"] = dt_et.dt.year.astype("Int16")
                df["mo_et"] = dt_et.dt.month.astype("Int8")
                if tf == "minute":
                    df["day_et"] = dt_et.dt.day.astype("Int8")
                df["datetime"] = dt_et

                if ts_col in df.columns:
                    df.drop(columns=[ts_col], inplace=True)

                need = ["yr_et","mo_et"] + (["day_et"] if tf == "minute" else [])
                df = df.dropna(subset=need)
                if df.empty: continue

                rows_kept += len(df)

                # bucketize
                keys = (["yr_et", "mo_et"] if layout == "market" else ["ticker", "yr_et", "mo_et"]) + (["day_et"] if tf == "minute" else [])
                for k, sub in df.groupby(keys, observed=True):
                    key = tuple((str(x) if (i == 0 and layout == "ticker") else int(x)) for i, x in enumerate(k))
                    buckets[key].append(sub)

            # Flush buckets that can no longer receive rows (bounded memory; see _flushable)
            try:
                cur = source_period(Path(csv_path), tf)
            except Exception:
                cur = None
            if cur is not None:
                for k in _flushable(list(buckets.keys()), cur, tf):
                    if _clashes(k, layout, fold_guard, spelling_of, clashes):
                        buckets.pop(k)
                        continue
                    if _uncovered(k, covered, out_root, tf, layout):
                        untouched[str(_bucket_path(out_root, k, tf, layout))] = sum(len(x) for x in buckets.pop(k))
                        continue
                    _write_bucket(out_root, k, buckets.pop(k), tf, layout)
                    written += 1

    finally:
        # Write whatever is still buffered
        for k in list(buckets.keys()):
            if _clashes(k, layout, fold_guard, spelling_of, clashes):
                buckets.pop(k)
                continue
            if _uncovered(k, covered, out_root, tf, layout):
                untouched[str(_bucket_path(out_root, k, tf, layout))] = sum(len(x) for x in buckets.pop(k))
                continue
            _write_bucket(out_root, k, buckets.pop(k), tf, layout)
            written += 1

        if LOG_QUEUE:
            LOG_QUEUE.put(
                f"[worker {worker_id:3d}] rows_in={rows_in:,} rows_kept={rows_kept:,} "
                f"dropped_only={dropped_only:,} dropped_watch={dropped_watch:,} "
                f"dropped_null_ticker={dropped_null:,} written_files={written}"
            )

    if clashes:
        pairs = ", ".join(f"{a} vs {b}" for a, b in sorted(clashes)[:6])
        raise RuntimeError(
            f"ticker-layout lake on a case-folding filesystem: {pairs} would share one directory, so one "
            f"security's files would replace the other's. Their rows were not written. Use --layout market, "
            f"write to a case-sensitive volume, or restrict --watch to one spelling of each symbol."
        )
    return worker_id, written, matched, near_misses, dropped_null, untouched

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

def build_manifest(out_root: Path, manifest_path: Path, logger=None, workers: int = 4, layout: Layout = "ticker") -> None:
    """
    Build a manifest JSON listing each parquet file and its datetime min/max.
    Shows a tqdm progress bar over all parquet files, optionally threaded.
    """
    t0 = time.time()
    print_critical("Building manifest: scanning parquet files ...")
    if logger: logger("Building manifest: scanning parquet files ...")

    # Gather all (ticker, path) pairs
    pairs: List[tuple[str, Path]] = []
    if layout == "market":
        # one key for the whole lake; each entry carries the date range of an all-ticker file
        pairs = [("__market__", p) for p in sorted(out_root.rglob("*.parquet")) if not p.name.endswith(".idx.parquet")]
    else:
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
    layout: Layout = "ticker",
    ignore_case: bool = False,
    replace_month: bool = False,
    replace_with_subset: bool = False,
):
    """Ingest every flat file under `src_root` into `out_root`. Each output file is replaced whole, so a run is
    refused (before anything is written) when a day month file holds sessions --src lacks, unless
    `replace_month`, and when a --watch / --only run would rewrite a market-layout file holding tickers outside the
    selection, unless `replace_with_subset`; see month_coverage_gaps, market_subset_losses and _uncovered."""
    import multiprocessing as mp

    out_root.mkdir(parents=True, exist_ok=True)

    # Load the watchlist before anything else: it decides whether a ticker-layout lake can be written
    # safely on this filesystem (see _clashes).
    def load_watch(path: Optional[Path]) -> Optional[List[str]]:
        """Ticker list, spelled as the caller wrote it (json list or one per line)."""
        if not path: return None
        if not path.exists(): raise FileNotFoundError(str(path))
        vals = json.load(open(path)) if path.suffix.lower() == ".json" else open(path).read().splitlines()
        return clean_list(vals)
    watch_list = load_watch(watch)
    only = next(iter(clean_list([only])), None) if only else None

    fold_guard = layout == "ticker" and fs_folds_case(out_root)
    if fold_guard:
        selected = [only] if only is not None else watch_list
        clashing = case_collisions(selected) if (selected is not None and not ignore_case) else {}
        if clashing:
            pairs = "; ".join(f"{k}: {', '.join(v)}" for k, v in sorted(clashing.items())[:6])
            raise SystemExit(
                f"[ERROR] {out_root} is on a filesystem that folds letter case, so <out>/AAP and <out>/AAp are\n"
                f"        one directory, but these entries differ only in case and name different securities:\n"
                f"        {pairs}\n"
                f"        Use --layout market, write to a case-sensitive volume, or keep one spelling per symbol."
            )

    # Start logger thread
    global LOG_QUEUE, QUIET_CONSOLE
    QUIET_CONSOLE = bool(quiet_console)
    LOG_QUEUE, log_thread, log_stop = start_logger_thread(log_file, QUIET_CONSOLE)

    # CRITICAL startup messages
    LOG(f"[INFO] Starting {tf.upper()} ingest", critical=True)
    LOG(f"[INFO] Source: {src_root}", critical=True)
    LOG(f"[INFO] Output: {out_root}", critical=True)
    LOG(f"[INFO] Layout: {layout}", critical=True)
    if watch: LOG(f"[INFO] Watchlist: {watch} ({len(watch_list)} symbols, "
                  f"{'case-insensitive' if ignore_case else 'matched exactly'})", critical=True)
    if only:  LOG(f"[INFO] Only: {only}", critical=True)

    watch_set = set(watch_list) if watch_list is not None else None

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

    # Pre-flight: every output file is replaced whole, so each check below runs before any worker starts and a
    # refused run leaves the lake as it was.
    def refuse(msg: str):
        LOG(msg, critical=True)
        log_stop.set(); log_thread.join()
        raise SystemExit(msg)

    keeps = _selection(watch_set, only, ignore_case)
    # periods --src holds a flat file for
    if tf == "day":
        src_periods = frozenset(parse_year_month_from_path(Path(c)) for c in all_csvs if c not in skipped)
    else:
        src_periods = frozenset((d.year, d.month, d.day) for c in all_csvs if (d := file_session(Path(c))) is not None)

    # A market file holds every ticker of its period and is rewritten from the selected tickers' rows alone, so a
    # --watch / --only run must not touch a file holding any other ticker. A ticker-layout run writes only the
    # selected symbols' directories. --replace-month does not cover this: it accepts losing sessions, not tickers.
    if layout == "market" and keeps is not None:
        losses = market_subset_losses(out_root, src_periods, tf, keeps)
        if losses and not replace_with_subset:
            sel = " and ".join(([f"--watch {watch} ({len(watch_set)} symbol(s))"] if watch_set is not None else [])
                               + ([f"--only {only}"] if only is not None else []))
            lines = [f"        {f.relative_to(out_root)}: {n:,} other ticker(s) ({', '.join(ex)}{', ...' if n > len(ex) else ''})"
                     for f, (n, ex) in sorted(losses.items())[:12]]
            if len(losses) > 12:
                lines.append(f"        ... and {len(losses) - 12} more file(s)")
            refuse(f"[ERROR] {sel} keeps only the selected tickers, and a market-layout file holds every ticker of its "
                   f"{'month' if tf == 'day' else 'session'} and is rewritten whole, so these {len(losses)} existing "
                   f"file(s) under {out_root} would lose every other ticker:\n" + "\n".join(lines) +
                   "\n        Write the subset to its own --out (or use --layout ticker), drop --watch/--only to rebuild "
                   "those files for every ticker, or pass --replace-with-subset to rewrite them with the selection alone.")
        if losses:
            LOG(f"[warn] --replace-with-subset: rewriting {len(losses)} existing market file(s) with the selected "
                f"tickers alone; every other ticker in them is dropped", critical=True)

    # A day month file is rewritten from what --src holds for that month, so --src must hold every session the
    # file already has.
    if tf == "day" and not replace_month:
        gaps = month_coverage_gaps(out_root, all_csvs, layout, keeps)
        if gaps:
            lines = [f"        {y:04d}-{m:02d}: {len(v)} session(s) missing from --src "
                     f"({v[0]}{' .. ' + str(v[-1]) if len(v) > 1 else ''})" for (y, m), v in sorted(gaps.items())[:12]]
            if len(gaps) > 12:
                lines.append(f"        ... and {len(gaps) - 12} more month(s)")
            refuse(f"[ERROR] {len(gaps)} month file(s) under {out_root} hold sessions --src has no flat file for, and "
                   f"the ingester rewrites a month whole from --src, so they would be lost:\n" + "\n".join(lines) +
                   "\n        Put every flat file of those months under --src, or pass --replace-month to rewrite "
                   "them from --src alone.")
    # a bucket outside the periods --src covers may create a file but never replace one
    covered = None if replace_month else src_periods

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
                worker, owned_csvs[wid], out_root, watch_set, only, chunk, wid, tf, layout,
                ignore_case, fold_guard, covered
            ))
        matched: set[str] = set()
        near_misses: set[str] = set()
        dropped_null = 0
        untouched: Dict[str, int] = {}
        for f in futures:
            wid, nkeys, m, n, nnull, unt = f.result()
            matched |= m
            near_misses |= n
            dropped_null += nnull
            untouched.update(unt)
            LOG(f"worker {wid:3d}: wrote {nkeys} parquet partitions")

    # Finish progress
    with progress_counter.get_lock():
        progress_counter.value = total_files
    stop_event.set()
    thr.join()
    LOG("[INFO] All workers completed.", critical=True)
    if dropped_null:
        LOG(f"[INFO] {dropped_null:,} row(s) with an empty ticker field dropped (no symbol to file them under)", critical=True)
    if untouched:
        LOG(f"[warn] {len(untouched)} existing file(s) left as they were: rows stamped into a period --src holds no "
            f"flat file for would have replaced them whole (pass --replace-month to write them anyway):", critical=True)
        for path, n in sorted(untouched.items())[:20]:
            LOG(f"  {path} ({n:,} row(s) not written)", critical=True)

    # Matching is exact, so a watchlist entry spelled in the wrong case now finds nothing where it
    # used to find the symbol. Say so, and name the symbols that were skipped for that reason - they
    # are real securities (preferred series, warrants, rights) that happen to share a common stock's
    # letters, and the caller has to decide whether they wanted them.
    if watch_list is not None:
        missed = [t for t in watch_list if t not in matched]
        if missed:
            LOG(f"[INFO] {len(missed)} of {len(watch_list)} watchlist symbols matched no rows: "
                f"{', '.join(missed[:20])}{' ...' if len(missed) > 20 else ''}", critical=True)
        if near_misses:
            LOG(f"[INFO] {len(near_misses)} symbol(s) differing from a watchlist entry only in letter case "
                f"were NOT ingested: {', '.join(sorted(near_misses)[:20])}"
                f"{' ...' if len(near_misses) > 20 else ''}", critical=True)
            LOG("[INFO]   Polygon spells share class in case (a lowercase p/w/r marks a preferred series, "
                "warrant or right), so these are different securities. Add the exact spelling to the "
                "watchlist to keep them, or pass --ignore-case to match on letters alone.", critical=True)

    # Manifest (with progress bar)
    if write_manifest:
        default_name = f"manifest_{tf}.json"
        manifest_path = manifest_out if manifest_out else (out_root / default_name)
        build_manifest(out_root, manifest_path, logger=LOG, workers=max(1, manifest_workers), layout=layout)

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
    ap.add_argument("--watch", type=Path, default=None,
                    help="JSON/TXT ticker list (optional). Matched exactly: Polygon spells share class in "
                         "letter case, so AAP selects Advance Auto Parts and not the AAp preferred")
    ap.add_argument("--only", type=str, default=None, help="Restrict to a single ticker, matched exactly (e.g., AAPL)")
    ap.add_argument("--ignore-case", action="store_true",
                    help="Match --watch/--only on letters alone, so AAP also selects AAp, AAPw, ... "
                         "(different securities; not safe for --layout ticker on a case-folding filesystem)")
    ap.add_argument("--layout", choices=["ticker", "market"], default="ticker",
                    help="ticker: <out>/<TICKER>/<YYYY>/<MM>[/<DD>].parquet | market: <out>/<YYYY>/<MM>[/<DD>].parquet with all tickers (whole universe)")
    ap.add_argument("--replace-month", action="store_true",
                    help="Rewrite each day month file from --src even where --src lacks sessions the file already "
                         "holds (they are dropped), and let rows a flat file stamps into a neighboring month or "
                         "session replace that file. Without it such a run is refused before anything is written.")
    ap.add_argument("--replace-with-subset", action="store_true",
                    help="With --layout market and --watch/--only, rewrite existing period files with the selected "
                         "tickers alone (every other ticker in them is dropped). Without it a run that would rewrite "
                         "a file holding tickers outside the selection is refused before anything is written.")
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
        manifest_workers=args.manifest_workers, layout=args.layout,
        ignore_case=args.ignore_case, replace_month=args.replace_month,
        replace_with_subset=args.replace_with_subset,
    )
