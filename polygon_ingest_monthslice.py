#!/usr/bin/env python
"""
polygon_ingest_monthslice.py
────────────────────────────
Batch‐mode ingest: each worker gets *all* the CSV.GZ files for the
year–month slices it owns, so monthly Parquet files accumulate every
day’s data correctly and are atomically renamed once fully written.

Usage (Linux/macOS):
    # (optional) raise FD limit
    ulimit -n 16384

    python polygon_ingest_monthslice.py \
        --src minute_aggs_v1 \
        --out parquet_lake \
        --workers 16 \
        --chunk 5000000 \
        --watch ticker_lists/nasdaq100.json
"""

from __future__ import annotations
import os
import glob
import json
import gzip
import argparse
import threading
import time
from multiprocessing import Manager
from pathlib import Path
from typing import Optional, Sequence
from collections import OrderedDict
from concurrent.futures import ProcessPoolExecutor

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

# ── configuration ─────────────────────────────────────────────────────────────
MAX_OPEN      = 512         # max open ParquetWriter instances per worker
CHUNK_DEFAULT = 5_000_000   # rows per pandas.read_csv chunk
TS_CANDS      = ["window_start", "timestamp", "ts", "t", "epoch", "start_time"]

# ── helper functions ──────────────────────────────────────────────────────────
def detect_ts_column(path: str) -> str:
    with gzip.open(path, "rt") as f:
        header = f.readline().split(",")
    for c in TS_CANDS:
        if c in header:
            return c
    raise ValueError(f"No timestamp column in {path}")

def infer_unit(series: pd.Series) -> str:
    x = int(series.dropna().iloc[0])
    if x > 1e18: return "ns"
    if x > 1e15: return "us"
    if x > 1e12: return "ms"
    return "s"

def load_watch(path: Optional[str]) -> set[str] | None:
    if not path:
        return None
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(path)
    if p.suffix == ".json":
        return set(json.load(open(p)))
    return {line.strip() for line in open(p) if line.strip()}

def slice_owner(year: int, month: int, n_workers: int) -> int:
    """Deterministic mapping of (year, month) → worker ID."""
    return ((year - 2000) * 12 + (month - 1)) % n_workers


def start_progress_bar(total_files, counter):
    pbar = tqdm(total=total_files, desc="Progress", dynamic_ncols=True)
    last = 0
    while True:
        current = counter.value
        delta = current - last
        if delta > 0:
            pbar.update(delta)
            last = current
        if current >= total_files:
            break
        time.sleep(0.1)
    pbar.close()

# ── the batch worker ──────────────────────────────────────────────────────────
def worker(
    csv_list: Sequence[str],
    out_root: Path,
    watch: set[str] | None,
    n_workers: int,
    worker_id: int,
    chunk: int
, progress_counter):
    writers: OrderedDict[tuple[str,int,int], pq.ParquetWriter] = OrderedDict()
    file_map: dict[tuple[str,int,int], tuple[Path,Path]] = {}
    ts_col = None
    rows_written = 0

    # dtype hints
    dtypes = {
        "ticker": "category",
        "open": "float32",
        "high": "float32",
        "low": "float32",
        "close": "float32",
        "volume": "int32",
        "transactions": "int32",
    }

    try:
        for csv_path in csv_list:
            progress_counter.value += 1
            p = Path(csv_path)
            if ts_col is None:
                ts_col = detect_ts_column(csv_path)
                dtypes[ts_col] = "int64"
            usecols = list(dtypes)

            for df in pd.read_csv(
                csv_path,
                usecols=usecols,
                dtype=dtypes,
                compression="gzip",
                chunksize=chunk
            ):
                if watch:
                    df = df.loc[df["ticker"].isin(watch)]
                    if df.empty:
                        continue

                df = df.dropna(subset=["ticker"])
                if not hasattr(worker, "_unit"):
                    worker._unit = infer_unit(df[ts_col])

                df["datetime"] = (
                    pd.to_datetime(df[ts_col], unit=worker._unit, utc=True)
                      .dt.tz_convert("US/Eastern")
                )
                df.drop(columns=[ts_col], inplace=True)

                grp = df.groupby(
                    [df["ticker"],
                     df["datetime"].dt.year,
                     df["datetime"].dt.month],
                    observed=True
                )
                for (sym, yr, mo), sub in grp:
                    if slice_owner(yr, mo, n_workers) != worker_id:
                        continue

                    key = (sym, yr, mo)
                    odir = out_root / sym
                    odir.mkdir(parents=True, exist_ok=True)

                    tmp = odir / f"{sym}_{yr:04d}_{mo:02d}.parquet.inprogress"
                    fin = odir / f"{sym}_{yr:04d}_{mo:02d}.parquet"

                    w = writers.get(key)
                    if w is None:
                        if len(writers) >= MAX_OPEN:
                            oldk, oldw = writers.popitem(last=False)
                            oldw.close()
                        schema = pa.Table.from_pandas(
                            sub.head(0), preserve_index=False
                        ).schema
                        w = pq.ParquetWriter(
                            tmp,
                            schema,
                            compression="zstd",
                            compression_level=9,
                            use_dictionary=True,
                            write_statistics=True
                        )
                        writers[key] = w
                        file_map[key] = (tmp, fin)

                    w.write_table(
                        pa.Table.from_pandas(sub, preserve_index=False),
                        row_group_size=262_144
                    )
                    rows_written += len(sub)
    finally:
        # close & flush
        for w in writers.values():
            w.close()
        # atomic rename
        for tmp, fin in file_map.values():
            try:
                if tmp.exists():
                    tmp.rename(fin)
            except Exception:
                pass

    return worker_id, rows_written

# wrapper to unpack arguments
def run_worker(args):
    return worker(*args)

# ── CLI and main ───────────────────────────────────────────────────────────────
def cli():
    p = argparse.ArgumentParser(
        description="Batch ingest Polygon minute CSV.GZ → Parquet per ticker/year/month"
    )
    p.add_argument("--src",     required=True, help="root folder of CSV.GZ files")
    p.add_argument("--out",     required=True, help="destination Parquet lake")
    p.add_argument("--watch",   default=None,  help="JSON/TXT/CSV ticker list")
    p.add_argument("--workers", type=int, default=os.cpu_count()//2 or 1,
                   help="number of parallel workers")
    p.add_argument("--chunk",   type=int, default=CHUNK_DEFAULT,
                   help="rows per pandas chunk")
    return p.parse_args()

def main():
    args = cli()
    src   = Path(args.src)
    out   = Path(args.out); out.mkdir(exist_ok=True)
    watch = load_watch(args.watch)

    # gather all CSV.GZ and partition by year-month owner
    files = sorted(glob.glob(str(src/"**/*.csv.gz"), recursive=True))
    groups = {i: [] for i in range(args.workers)}
    for f in files:
        p = Path(f)
        name = p.name[:-7] if p.name.endswith(".csv.gz") else p.stem
        parts = name.split("-")
        if len(parts) == 3:
            yr, mo, _ = map(int, parts)
        elif len(parts) == 2:
            yr, mo = map(int, parts)
        else:
            yr = int(p.parent.name)
            mo = int(parts[0])
        owner = slice_owner(yr, mo, args.workers)
        groups[owner].append(f)

    tasks = [
        (groups[i], out, watch, args.workers, i, args.chunk)
        for i in range(args.workers)
    ]

    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        for wid, cnt in tqdm(pool.map(run_worker, tasks),
                             total=args.workers,
                             desc="Workers done"):
            tqdm.write(f"worker {wid:2d}: wrote {cnt:,} rows")


if __name__ == "__main__":
    import multiprocessing as mp

    parser = argparse.ArgumentParser()
    parser.add_argument("--src", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--watch", type=str)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--chunk", type=int, default=CHUNK_DEFAULT)
    args = parser.parse_args()

    src_root = Path(args.src)
    out_root = Path(args.out)
    watch = load_watch(args.watch)
    n_workers = args.workers
    chunk = args.chunk

    # Build the complete CSV list
    all_csvs = sorted(str(p) for p in src_root.rglob("*.csv.gz"))
    owned_csvs = [[] for _ in range(n_workers)]
    for path in all_csvs:
        name = Path(path).name
        try:
            date_part = name.split(".")[0]  # "2023-06-05"
            year, month = map(int, date_part.split("-")[:2])
        except Exception as e:
            print(f"Skipping unrecognized filename format: {name}")
            continue
        owner = slice_owner(year, month, n_workers)
        owned_csvs[owner].append(path)

    # Start shared progress counter
    manager = mp.Manager()
    progress_counter = manager.Value("i", 0)
    total_files = sum(len(lst) for lst in owned_csvs)
    thread = threading.Thread(target=start_progress_bar, args=(total_files, progress_counter))
    thread.start()

    # Launch workers
    with ProcessPoolExecutor(max_workers=n_workers) as executor:
        futures = []
        for worker_id in range(n_workers):
            futures.append(executor.submit(
                worker,
                owned_csvs[worker_id],
                out_root,
                watch,
                n_workers,
                worker_id,
                chunk,
                progress_counter
            ))
        
    # After all files are processed, force counter to total and join
    for f in futures:
        f.result()
    # ensure counter reaches total to finish progress bar
    progress_counter.value = total_files
    thread.join()
