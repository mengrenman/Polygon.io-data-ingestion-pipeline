#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
polygon_lake_loader: load Polygon.io parquet lakes (day/minute) efficiently.

Layouts this module understands
-------------------------------
DAY lake (monthly parquet):
    <root>/<TICKER>/<YYYY>/<MM>.parquet

MINUTE lake (daily parquet):
    <root>/<TICKER>/<YYYY>/<MM>/<DD>.parquet

Manifests (optional, recommended for speed)
-------------------------------------------
A JSON produced by the ingesters we built, e.g.:
{
  "AAPL": [
    {"path": ".../AAPL/2024/01.parquet", "start": "...", "end": "...", "rows": 123},
    ...
  ],
  "MSFT": [...]
}

Public API
----------
- load_polygonio_lake(...)
- select_lake_files(...)

CLI
---
Examples:
  python polygon_lake_loader.py \
    --tickers AAPL,MSFT \
    --start 2024-01-01 --end 2024-12-31 \
    --root /data/parquet_lake/day_aggs_v1 \
    --granularity day \
    --manifest /data/parquet_lake/day_aggs_v1/manifest_day.json \
    --to-timezone UTC \
    --set-index --index-multi \
    --out /tmp/day_sample.parquet

  python polygon_lake_loader.py \
    --tickers AAPL \
    --start 2024-08-01 --end 2024-08-05 \
    --root /data/parquet_lake/minute_aggs_v1 \
    --granularity minute \
    --manifest /data/parquet_lake/minute_aggs_v1/manifest_minute.json \
    --show-progress
"""

from __future__ import annotations
from pathlib import Path
from typing import Iterable, List, Optional, Literal, Dict, Any, Tuple
import os, glob, json, datetime as dt

# Keep Arrow from oversubscribing CPU when we also parallelize at Python level
os.environ.setdefault("PYARROW_NUM_THREADS", "2")

import pandas as pd
from pandas.api.types import is_datetime64tz_dtype
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed

__all__ = [
    "load_polygonio_lake",
    "select_lake_files",
]

# ───────────────────────── helpers: dates & listing ──────────────────────────
def _month_range(start: dt.date, end: dt.date) -> List[tuple[int,int]]:
    y, m = start.year, start.month
    out = []
    while (y < end.year) or (y == end.year and m <= end.month):
        out.append((y, m))
        if m == 12: y, m = y + 1, 1
        else: m += 1
    return out

def _list_files_day(root: Path, tickers: Iterable[str], s: dt.date, e: dt.date) -> List[Path]:
    """Monthly parquet per ticker/year/month: <root>/<TICKER>/<YYYY>/<MM>.parquet"""
    files: List[Path] = []
    months = _month_range(s, e)
    for t in tickers:
        tdir = root / t
        for yy, mm in months:
            f = tdir / f"{yy:04d}" / f"{mm:02d}.parquet"
            if f.exists(): files.append(f)
    return files

def _list_files_minute(root: Path, tickers: Iterable[str], s: dt.date, e: dt.date) -> List[Path]:
    """
    Daily parquet per ticker/year/month/day:
      <root>/<TICKER>/<YYYY>/<MM>/<DD>.parquet
    We enumerate months, then glob all days; we still filter rows by [s,e] later.
    """
    files: List[Path] = []
    months = _month_range(s, e)
    for t in tickers:
        tdir = root / t
        for yy, mm in months:
            ddir = tdir / f"{yy:04d}" / f"{mm:02d}"
            if ddir.exists():
                files.extend(Path(p) for p in glob.glob(str(ddir / "*.parquet")))
    return files

# ───────────────────────── manifest-aware selection ──────────────────────────
def _safe_parse_ts(x: Any, source_tz: str) -> pd.Timestamp:
    """Parse timestamps from manifest (stringified pandas timestamps)."""
    ts = pd.to_datetime(x, errors="coerce", utc=False)
    if ts.tz is None:
        # Our ingesters save tz-aware strings; but just in case, localize.
        ts = ts.tz_localize(source_tz)
    return ts

def _select_from_manifest(
    manifest_path: Path,
    tickers: Iterable[str],
    s: pd.Timestamp,
    e: pd.Timestamp,
    *,
    source_tz: str = "US/Eastern",
) -> List[Path]:
    """
    Read manifest JSON and return file paths whose [start,end] overlaps [s,e] for tickers.
    """
    with open(manifest_path, "r") as f:
        man: Dict[str, List[Dict[str, Any]]] = json.load(f)

    sel: List[Path] = []
    seen: set[str] = set()
    tset = {str(t).upper() for t in tickers}

    for t in tset:
        for ent in man.get(t, []):
            p = Path(ent["path"])
            key = str(p)
            if key in seen: 
                continue
            start = _safe_parse_ts(ent.get("start"), source_tz)
            end   = _safe_parse_ts(ent.get("end"), source_tz)
            if pd.isna(start) or pd.isna(end):
                continue
            # overlap test
            if (end >= s) and (start <= e) and p.exists():
                sel.append(p); seen.add(key)
    return sel

def select_lake_files(
    tickers: Iterable[str],
    start_date: str | dt.date | dt.datetime,
    end_date: str | dt.date | dt.datetime,
    root: str | Path,
    *,
    granularity: Literal["day","minute"] = "day",
    manifest: Optional[str | Path] = None,
    manifest_fallback: bool = True,
    source_tz: str = "US/Eastern",
    debug: bool = False,
) -> List[Path]:
    """
    Choose the parquet files to read, optionally using a manifest for speed.

    Returns: list[Path]
    """
    # Normalize inputs
    tickers = [str(t).upper() for t in tickers]
    root = Path(root)

    s = pd.to_datetime(start_date)
    e = pd.to_datetime(end_date)
    if s.tzinfo is None: s = s.tz_localize(source_tz)
    if e.tzinfo is None: e = e.tz_localize(source_tz)

    if isinstance(start_date, (dt.date, str)) and not isinstance(start_date, dt.datetime):
        s = s.normalize()
    if isinstance(end_date, (dt.date, str)) and not isinstance(end_date, dt.datetime):
        e = (e.normalize() + pd.Timedelta(days=1)) - pd.Timedelta(nanoseconds=1)

    # Try manifest
    files: List[Path] = []
    used_manifest = False
    if manifest:
        try:
            mpath = Path(manifest)
            if not mpath.exists():
                raise FileNotFoundError(str(mpath))
            files = _select_from_manifest(mpath, tickers, s, e, source_tz=source_tz)
            used_manifest = True
            if debug:
                print(f"[DEBUG] Manifest {mpath} → {len(files)} files")
        except Exception as ex:
            if debug:
                print(f"[DEBUG] Manifest selection failed: {ex} (fallback={manifest_fallback})")
            if not manifest_fallback:
                raise

    # Fallback walk
    if not files:
        files = (
            _list_files_minute(root, tickers, s.date(), e.date())
            if granularity == "minute"
            else _list_files_day(root, tickers, s.date(), e.date())
        )
        if debug:
            how = "FS walk" if not used_manifest else "Manifest empty → FS walk"
            print(f"[DEBUG] {how} (granularity={granularity}) → {len(files)} files")

    return files

# ───────────────────────── parquet read helper ───────────────────────────────
def _read_parquet(path: Path, columns: Optional[List[str]]) -> pd.DataFrame:
    return pd.read_parquet(path, columns=columns)

# ───────────────────────── public loader ─────────────────────────────────────
def load_polygonio_lake(
    tickers: Iterable[str],
    start_date: str | dt.date | dt.datetime,
    end_date: str | dt.date | dt.datetime,
    root: str | Path,
    *,
    columns: Optional[List[str]] = None,
    source_tz: str = "US/Eastern",  # used only if timestamps are tz-naive
    to_timezone: Optional[str] = None,
    set_index: bool = False,
    index_multi: bool = False,
    debug: bool = False,
    workers: Optional[int] = None,
    parallel_backend: Literal["thread","process"] = "thread",
    granularity: Literal["day","minute"] = "day",
    manifest: Optional[str | Path] = None,
    manifest_fallback: bool = True,
    show_progress: bool = False,
) -> pd.DataFrame:
    """
    Load Polygon parquet lake into one DataFrame filtered to [start_date, end_date].

    - If `manifest` is given, selects files via manifest; otherwise walks the lake.
    - `show_progress` controls tqdm bars (default False for import-friendly usage).
    """
    tickers = [str(t).upper() for t in tickers]

    # Bounds for row filtering (reuse select_lake_files parsing rules)
    s = pd.to_datetime(start_date)
    e = pd.to_datetime(end_date)
    if s.tzinfo is None: s = s.tz_localize(source_tz)
    if e.tzinfo is None: e = e.tz_localize(source_tz)
    if isinstance(start_date, (dt.date, str)) and not isinstance(start_date, dt.datetime):
        s = s.normalize()
    if isinstance(end_date, (dt.date, str)) and not isinstance(end_date, dt.datetime):
        e = (e.normalize() + pd.Timedelta(days=1)) - pd.Timedelta(nanoseconds=1)

    # Columns to read: always include datetime & ticker if user specified columns
    read_cols = None
    if columns is not None:
        need = {"datetime", "ticker"}
        read_cols = list(dict.fromkeys(list(columns) + list(need)))

    # Choose files
    files = select_lake_files(
        tickers, start_date, end_date, root,
        granularity=granularity,
        manifest=manifest,
        manifest_fallback=manifest_fallback,
        source_tz=source_tz,
        debug=debug
    )

    if not files:
        if debug:
            src = f"manifest:{manifest}" if manifest else f"root:{root}"
            print(f"[DEBUG] No files for {tickers} in {s}..{e} ({src})")
        return pd.DataFrame(columns=read_cols or ["datetime","ticker"])

    # Parallel read
    dfs: List[pd.DataFrame] = []
    if debug and not show_progress:
        for f in files:
            try:
                dfs.append(_read_parquet(f, read_cols))
            except Exception as ex:
                print(f"[DEBUG] Failed to read {f}: {ex}")
    else:
        max_workers = workers or max(4, min(32, (os.cpu_count() or 8)))
        Exec = ThreadPoolExecutor if parallel_backend == "thread" else ProcessPoolExecutor
        progress_ctx = tqdm(total=len(files), desc="Loading Parquet", unit="file") if show_progress else None
        try:
            with Exec(max_workers=max_workers) as ex:
                futs = {ex.submit(_read_parquet, f, read_cols): f for f in files}
                for fut in as_completed(futs):
                    f = futs[fut]
                    try:
                        dfs.append(fut.result())
                    except Exception as exn:
                        if show_progress:
                            progress_ctx.write(f"[WARN] Failed to read {f}: {exn}")
                        elif debug:
                            print(f"[DEBUG] Failed to read {f}: {exn}")
                    finally:
                        if show_progress:
                            progress_ctx.update(1)
        finally:
            if progress_ctx is not None:
                progress_ctx.close()

    if not dfs:
        return pd.DataFrame(columns=read_cols or ["datetime","ticker"])

    df = pd.concat(dfs, ignore_index=True)

    # Timezone sanity (ingesters write tz-aware; still normalize defensively)
    if "datetime" not in df.columns:
        raise KeyError("Expected 'datetime' column in parquet files.")

    if not is_datetime64tz_dtype(df["datetime"]):
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
        df["datetime"] = df["datetime"].dt.tz_localize(source_tz, nonexistent="shift_forward", ambiguous="NaT")

    if to_timezone:
        df["datetime"] = df["datetime"].dt.tz_convert(to_timezone)

    # Filter rows by time range & tickers
    df = df[(df["datetime"] >= s) & (df["datetime"] <= e)]
    if "ticker" in df.columns:
        df["ticker"] = df["ticker"].astype(str).str.upper()
        df = df[df["ticker"].isin(set(tickers))]

    # Sort & index
    sort_cols = ["ticker","datetime"] if "ticker" in df.columns else ["datetime"]
    df = df.sort_values(sort_cols)
    if set_index:
        if index_multi and "ticker" in df.columns:
            df = df.set_index(["ticker","datetime"]).sort_index()
        else:
            df = df.set_index("datetime").sort_index()

    return df

# ───────────────────────────── CLI (optional) ────────────────────────────────
def _parse_columns_arg(arg: Optional[str]) -> Optional[List[str]]:
    if not arg: return None
    # accept comma/space separated
    raw = [x.strip() for x in arg.replace(",", " ").split()]
    return [c for c in raw if c]

if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(
        description="Load Polygon parquet lake (day/minute), optionally using a manifest."
    )
    ap.add_argument("--tickers", required=True, help="Comma or space separated list, e.g. 'AAPL,MSFT'")
    ap.add_argument("--start", required=True, help="Start date (YYYY-MM-DD or timestamp)")
    ap.add_argument("--end", required=True, help="End date (YYYY-MM-DD or timestamp)")
    ap.add_argument("--root", required=True, type=Path, help="Parquet lake root directory")
    ap.add_argument("--granularity", choices=["day","minute"], default="day")
    ap.add_argument("--manifest", type=Path, default=None, help="Optional manifest JSON path")
    ap.add_argument("--no-fallback", action="store_true", help="Do not fallback to FS walk if manifest fails/empty")
    ap.add_argument("--columns", type=str, default=None, help="Columns to keep (comma/space list). 'datetime' and 'ticker' auto-added if provided")
    ap.add_argument("--to-timezone", type=str, default=None, help="Convert timestamps to this timezone (e.g. 'UTC')")
    ap.add_argument("--set-index", action="store_true")
    ap.add_argument("--index-multi", action="store_true", help="Use MultiIndex (ticker, datetime) if --set-index")
    ap.add_argument("--workers", type=int, default=None, help="Parallel readers (default: auto)")
    ap.add_argument("--backend", choices=["thread","process"], default="thread")
    ap.add_argument("--debug", action="store_true")
    ap.add_argument("--show-progress", action="store_true", help="Show tqdm while reading")
    ap.add_argument("--out", type=Path, default=None, help="Optional output file (.parquet/.csv/.feather)")
    args = ap.parse_args()

    tickers = [t for t in args.tickers.replace(",", " ").split() if t]
    cols = _parse_columns_arg(args.columns)

    df = load_polygonio_lake(
        tickers=tickers,
        start_date=args.start,
        end_date=args.end,
        root=args.root,
        columns=cols,
        to_timezone=args.to_timezone,
        set_index=args.set_index,
        index_multi=args.index_multi,
        debug=args.debug,
        workers=args.workers,
        parallel_backend=args.backend,  # type: ignore
        granularity=args.granularity,   # type: ignore
        manifest=args.manifest,
        manifest_fallback=not args.no_fallback,
        show_progress=args.show_progress,
    )

    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        suffix = args.out.suffix.lower()
        if suffix == ".parquet":
            df.to_parquet(args.out)
        elif suffix in (".csv", ".txt"):
            df.to_csv(args.out, index=df.index.name is not None)
        elif suffix in (".feather", ".ft"):
            df.reset_index().to_feather(args.out)
        else:
            raise ValueError(f"Unsupported --out suffix: {args.out.suffix}")
        print(f"[INFO] Wrote {len(df):,} rows → {args.out}")
    else:
        # Print a tiny summary to avoid dumping a huge frame.
        n = len(df)
        cols = list(df.columns)
        print(f"[INFO] Loaded rows: {n:,}; columns: {cols[:8]}{'...' if len(cols)>8 else ''}")
        if n:
            print(f"[INFO] datetime range: {df['datetime'].min()} → {df['datetime'].max()}")


# --- schema-safe readers (append-only, guarded) ------------------------------
from pathlib import Path as _Path
import pandas as _pd
import pyarrow.parquet as _pq

if "load_series" not in globals() or "load_events" not in globals():
# --- widen the columns we’re willing to read -------------------------------
    _DESIRED_COLS = [
        # time keys (any of these may exist)
        "datetime", "date", "timestamp",
        # base
        "ticker","open","high","low","close","volume","vwap",
        # split-adjusted as written by factor_builder.py
        "open_split","high_split","low_split","close_split","volume_split",
        # canonical names we expose to notebooks
        "open_sa","high_sa","low_sa","close_sa","vwap_sa","volume_sa",
        # total return
        "close_tr",
        # optional factors if present
        "split_price_factor","tr_price_factor",
    ]

    def _collect_paths(root: _Path, tf: str, ticker: str):
        base = _Path(root) / str(ticker).upper()
        if not base.exists():
            return []
        return sorted(base.glob("*/*/*.parquet")) if tf == "minute" else sorted(base.glob("*/*.parquet"))

    def _file_cols(path: _Path) -> set[str]:
        try:
            return set(_pq.ParquetFile(path).schema_arrow.names)
        except Exception:
            return set()

    def _read_subset(paths: list[_Path]) -> _pd.DataFrame:
        """Read a set of parquet files, accept datetime OR date/timestamp, and return a frame with a tz-aware 'datetime'."""
        want = set(_DESIRED_COLS)
        dfs: list[_pd.DataFrame] = []
        for p in paths:
            cols_avail = _file_cols(p)
            cols = list(cols_avail & want)
            if not cols:
                continue
            df = _pd.read_parquet(p, columns=cols)

            # harmonize to a single 'datetime' column (UTC-aware)
            if "datetime" in df.columns:
                dt = _pd.to_datetime(df["datetime"], utc=True, errors="coerce")
            elif "timestamp" in df.columns:
                dt = _pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
            elif "date" in df.columns:
                dt = _pd.to_datetime(df["date"], utc=True, errors="coerce")
            else:
                # no time-like column—skip this file
                continue

            df = df.drop(columns=[c for c in ("timestamp","date") if c in df.columns])
            df["datetime"] = dt
            dfs.append(df)

        if not dfs:
            return _pd.DataFrame(columns=_DESIRED_COLS)

        out = _pd.concat(dfs, ignore_index=True)
        return out.sort_values("datetime").reset_index(drop=True)


    def load_series(unadj_root: _Path, adj_root: _Path, tf: str, ticker: str, start=None, end=None) -> _pd.DataFrame:
        base_cols = ["datetime","ticker","open","high","low","close","volume","vwap"]
        adj_cols  = ["open_sa","high_sa","low_sa","close_sa","vwap_sa","volume_sa","close_tr"]

        df_un = _read_subset(_collect_paths(unadj_root, tf, ticker))
        df_ad = _read_subset(_collect_paths(adj_root,   tf, ticker))

        # nothing to do?
        if df_un.empty and df_ad.empty:
            return _pd.DataFrame(columns=_DESIRED_COLS)

        # Map factor_builder’s *_split → canonical *_sa
        split_to_sa = {
            "open_split":"open_sa","high_split":"high_sa","low_split":"low_sa",
            "close_split":"close_sa","volume_split":"volume_sa",
        }
        for src, dst in split_to_sa.items():
            if dst not in df_ad.columns and src in df_ad.columns:
                df_ad[dst] = df_ad[src]

        # Build merge keys
        if tf == "day":
            # merge on calendar date, not exact time
            df_un["__d"] = df_un["datetime"].dt.tz_convert(None).dt.normalize()
            df_ad["__d"] = df_ad["datetime"].dt.tz_convert(None).dt.normalize()
            left  = df_un[[c for c in (["datetime","__d"] + base_cols) if c in df_un.columns]].copy()
            right = df_ad[[c for c in (["__d"] + adj_cols) if c in df_ad.columns]].copy()
            df = _pd.merge(left, right, on="__d", how="left").drop(columns="__d", errors="ignore")
        else:
            # minute: merge on exact timestamp
            left  = df_un[[c for c in base_cols if c in df_un.columns]].copy()
            right = df_ad[[c for c in (["datetime"] + adj_cols) if c in df_ad.columns]].copy()
            df = _pd.merge(left, right, on="datetime", how="left")

        # optional bounds (make them UTC-aware to match)
        if start or end:
            start_ts = _pd.to_datetime(start, utc=True) if start else None
            end_ts   = _pd.to_datetime(end,   utc=True) if end   else None
            if start_ts is not None:
                df = df[df["datetime"] >= start_ts]
            if end_ts is not None:
                df = df[df["datetime"] <= end_ts]

        return df.sort_values("datetime").reset_index(drop=True)


    def load_events(refdir: _Path, ticker: str):
        sp = _Path(refdir) / "stock_splits.parquet"
        dv = _Path(refdir) / "cash_dividends.parquet"

        splits = _pd.read_parquet(sp) if sp.exists() else _pd.DataFrame()
        divs   = _pd.read_parquet(dv) if dv.exists() else _pd.DataFrame()

        # --- standardize ticker casing if present
        if "ticker" in splits: splits["ticker"] = splits["ticker"].astype(str).str.upper()
        if "ticker" in divs:   divs["ticker"]   = divs["ticker"].astype(str).str.upper()

        # --- helper to normalize a date column under a canonical name
        def _ensure_dt(df: _pd.DataFrame, candidates: list[str], outcol: str):
            for c in candidates:
                if c in df.columns:
                    df[outcol] = _pd.to_datetime(df[c], utc=True, errors="coerce")
                    return
            df[outcol] = _pd.NaT  # if none present, create empty

        # splits: execution_date (aliases sometimes: effective_date, split_date, date)
        if not splits.empty:
            _ensure_dt(splits, ["execution_date","effective_date","split_date","date"], "execution_date")
            # ratio if missing
            if "ratio" not in splits.columns and {"split_to","split_from"} <= set(splits.columns):
                sf = _pd.to_numeric(splits["split_from"], errors="coerce")
                st = _pd.to_numeric(splits["split_to"],   errors="coerce")
                with _pd.option_context("mode.use_inf_as_na", True):
                    splits["ratio"] = _pd.Series(st / sf).where((sf > 0) & (st.notna()), _pd.NA)

        # dividends: ex_date (aliases sometimes: ex_dividend_date, exDividendDate, exDate, date)
        if not divs.empty:
            _ensure_dt(divs, ["ex_date","ex_dividend_date","exDividendDate","exDate","date"], "ex_date")
            # keep only relevant fields if present
            keep = ["ticker","ex_date","cash_amount","pay_date","declaration_date","record_date","frequency"]
            divs = divs[[c for c in keep if c in divs.columns]]

        # filter by ticker if the column exists
        tkey = str(ticker).upper()
        if "ticker" in splits:
            splits = splits[splits["ticker"] == tkey].reset_index(drop=True)
        if "ticker" in divs:
            divs   = divs[  divs["ticker"]   == tkey].reset_index(drop=True)

        return splits, divs

# ---------------------------------------------------------------------------

