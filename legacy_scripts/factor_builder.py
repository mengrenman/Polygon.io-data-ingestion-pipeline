#!/usr/bin/env python3
# factor_builder.py
from __future__ import annotations

import argparse
import json
import os
import shutil
from pathlib import Path
from typing import Optional, List, Iterable, Tuple, Dict, Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as _pq
from tqdm import tqdm as _tqdm_impl
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed


# ===========================
# Globals for batch workers
# ===========================
_SPLITS_TABLE: Optional[pd.DataFrame] = None
_DIVS_TABLE: Optional[pd.DataFrame] = None

def _init_worker(splits: Optional[pd.DataFrame], divs: Optional[pd.DataFrame]) -> None:
    global _SPLITS_TABLE, _DIVS_TABLE
    _SPLITS_TABLE = splits
    _DIVS_TABLE = divs

# Progress bars are silenced inside worker processes (N interleaved bars are noise); see _worker_init.
_PROGRESS = True

def _tqdm(iterable=None, **kw):
    kw["disable"] = bool(kw.get("disable", False)) or not _PROGRESS
    return _tqdm_impl(iterable, **kw)

def _worker_init(arrow_threads: int = 1) -> None:
    """Process-pool initializer: no progress bars, and a small Arrow thread pool per process so that N worker
    processes x Arrow's default pool (every core) do not oversubscribe the machine."""
    global _PROGRESS
    _PROGRESS = False
    try:
        pa.set_cpu_count(max(1, int(arrow_threads)))
        pa.set_io_thread_count(max(2, int(arrow_threads)))
    except Exception:
        pass

def _arrow_threads_per_worker(workers: int) -> int:
    return max(1, (os.cpu_count() or 2) // max(1, int(workers)))


# =============================================================
# IO helpers
# =============================================================

def _path_contains_any(parts: Iterable[str], candidates: set[str]) -> Optional[str]:
    for p in parts:
        if p in candidates:
            return p
    return None

def _detect_layout(root: Path) -> str:
    """'market' if the lake root holds <YYYY>/ directories (all tickers per file), else 'ticker'."""
    root = Path(root)
    if root.is_dir():
        for p in root.iterdir():
            if p.is_dir() and len(p.name) == 4 and p.name.isdigit():
                return "market"
    return "ticker"

def _detect_ts_unit(maxv: float) -> str:
    if maxv >= 1e17:  return "ns"
    if maxv >= 1e14:  return "us"
    if maxv >= 1e11:  return "ms"
    return "s"

LOCAL_TZ = "US/Eastern"

def _to_naive_utc(s: pd.Series) -> pd.Series:
    """Datetime series -> tz-naive UTC (tz-aware input is converted, not just stripped)."""
    s = pd.to_datetime(s, errors="coerce")
    if getattr(s.dt, "tz", None) is not None:
        s = s.dt.tz_convert("UTC").dt.tz_localize(None)
    return s

def _trading_day(dt_naive_utc: pd.Series) -> pd.Series:
    """
    US/Eastern calendar date (tz-naive midnight) for a datetime series (tz-naive = UTC).

    Splits and ex-dividend dates are dated by the US trading day, so price rows must be keyed
    the same way. Keying on the UTC date would put after-hours bars from 19:00/20:00 ET onward
    into the *next* day and give them the next day's factors.
    """
    s = pd.to_datetime(dt_naive_utc, errors="coerce")
    if getattr(s.dt, "tz", None) is None:
        s = s.dt.tz_localize("UTC")
    # as_unit("ns"): pandas>=3 may carry us-resolution dates (e.g. read back from parquet) while lake
    # timestamps are ns; merge_asof refuses mixed resolutions, so every event-day key is pinned to ns.
    return s.dt.tz_convert(LOCAL_TZ).dt.normalize().dt.tz_localize(None).dt.as_unit("ns")

def _read_prices(path: Path,
                 tickers: Optional[List[str]] = None,
                 start: Optional[str] = None,
                 end: Optional[str] = None) -> pd.DataFrame:
    """Read raw (unadjusted) prices; require columns: datetime, ticker, close, volume."""
    def _normalize_one(df: pd.DataFrame, file_path: Path, tickset: set | None) -> pd.DataFrame:
        df = df.copy()

        # Ticker
        if "ticker" not in df.columns:
            ticker_from_path = None
            if tickset:
                ticker_from_path = _path_contains_any(file_path.parts, tickset)
            if ticker_from_path is None and "T" in df.columns:
                df = df.rename(columns={"T": "ticker"})
            else:
                if ticker_from_path is None:
                    ticker_from_path = file_path.parent.name
                df["ticker"] = ticker_from_path
        df["ticker"] = _norm_ticker(df["ticker"])

        # Map Polygon shorthand
        if "close" not in df.columns and "c" in df.columns:
            df = df.rename(columns={"c": "close"})
        if "volume" not in df.columns and "v" in df.columns:
            df = df.rename(columns={"v": "volume"})

        # Datetime
        if "datetime" not in df.columns:
            time_col = None
            for cand in ("datetime", "date", "timestamp", "t", "time"):
                if cand in df.columns:
                    time_col = cand
                    break
            if time_col is None:
                raise ValueError(f"{file_path}: no datetime/date/timestamp column")

            s = pd.to_datetime(df[time_col], errors="coerce", utc=True)
            if s.isna().all():
                num = pd.to_numeric(df[time_col], errors="coerce")
                if num.notna().any():
                    unit = _detect_ts_unit(float(np.nanmax(num.values)))
                    s = pd.to_datetime(num, unit=unit, errors="coerce", utc=True)
            if s.isna().all():
                raise ValueError(f"{file_path}: could not parse time column '{time_col}'")

            df["datetime"] = s.dt.tz_convert(None)
        else:
            df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce", utc=True).dt.tz_convert(None)

        # Enforce base columns
        needed = {"datetime", "ticker", "close", "volume"}
        missing = needed - set(df.columns)
        if missing:
            raise ValueError(f"{file_path}: prices missing columns {missing}")

        # Optional filters
        if start:
            df = df[df["datetime"] >= pd.to_datetime(start)]
        if end:
            df = df[df["datetime"] <= pd.to_datetime(end)]
        if tickset:
            df = df[_wanted_mask(df["ticker"], tickset)]

        return df[["datetime", "ticker", "close", "volume",
                   *[c for c in ("open","high","low") if c in df.columns]]]

    # Build file list
    tickset = set(tickers) if tickers else None
    layout = _detect_layout(path) if path.is_dir() else "ticker"
    filters = None
    if path.is_file():
        files = [path]
    elif layout == "ticker" and tickset:
        # walk only the requested <TICKER>/ subtrees: an rglob of a 100k-file lake costs seconds per call
        files = sorted(f for t in sorted(tickset) if (path / t).is_dir() for f in (path / t).rglob("*.parquet"))
        if not files:
            raise SystemExit("No files matched the provided tickers under the directory (<LAKE>/<TICKER>/...)")
    else:
        files = sorted(path.rglob("*.parquet"))
        if not files:
            raise SystemExit(f"No parquet files found under {path}")
        if tickset:
            filters = [("ticker", "in", sorted(tickset))]   # market layout: all tickers per file, prune on read

    dfs = []
    for f in _tqdm(files, desc=f"Reading prices ({layout} layout) from {path}"):
        try:
            raw = pd.read_parquet(f, filters=filters)
            dfs.append(_normalize_one(raw, f, tickset))
        except Exception as e:
            print(f"[WARN] Skipping {f}: {e}")

    if not dfs:
        raise SystemExit("No readable price files after normalization")

    out = pd.concat(dfs, ignore_index=True)
    out["datetime"] = pd.to_datetime(out["datetime"]).dt.tz_localize(None)
    out["ticker"] = _norm_ticker(out["ticker"])
    return out


# =============================================================
# ID stitching & event-day key (batch)
# =============================================================

# =============================================================
# Holder ids: the company behind a ticker on a date (FIGI -> CIK -> ticker)
# =============================================================

def _norm_ticker(s: pd.Series) -> pd.Series:
    """Ticker column -> stripped, CASE PRESERVED.

    Polygon encodes share class in the letter case of the symbol: `AAp` is Alcoa's $3.75 preferred,
    a different security from `AAP` (Advance Auto Parts); `AANw` is a warrant. Upper-casing merges
    them into one series under one holder id, so the pipeline keeps the source spelling everywhere
    and folds case only when matching a user-supplied watchlist (see `_wanted_mask`).
    """
    return s.astype(str).str.strip()


def _wanted_mask(values: pd.Series, wanted: Optional[Iterable[str]]) -> pd.Series:
    """Case-insensitive membership test for a USER-SUPPLIED ticker list.

    Watchlists are written upper-case by convention (`data/ticker_lists/*.json`), while the lake
    preserves Polygon's spelling, so matching folds case even though storage does not.
    """
    if wanted is None:
        return pd.Series(True, index=values.index)
    want = {str(t).strip().casefold() for t in wanted}
    return values.astype(str).str.strip().str.casefold().isin(want)


def _holder_id(figi, cik, ticker) -> str:
    """
    Stable id for the *company* behind a ticker row: composite FIGI, else 'CIK__<cik>', else 'NOFIGI__<TICKER>'.
    Polygon returns no FIGI for some delisted holders (the pre-2009 General Motors Corp has only a CIK).
    """
    if isinstance(figi, str) and figi.strip():
        return figi.strip()
    if cik is not None and not (isinstance(cik, float) and np.isnan(cik)):
        c = str(cik).strip()
        if c and c.lower() not in ("nan", "none", "<na>"):
            return "CIK__" + c
    return "NOFIGI__" + str(ticker).strip()


def _naive_dates(x: pd.Series) -> pd.Series:
    """Any date-like column -> tz-naive (UTC) midnight, ns resolution."""
    d = pd.to_datetime(x, errors="coerce", utc=True)
    return d.dt.tz_convert(None).dt.normalize().dt.as_unit("ns")


def _normalize_sm(sm: pd.DataFrame) -> pd.DataFrame:
    """
    Security master -> one row per (ticker, holder_id) with the holder's window [effective_start, effective_end]
    (NaT = open/unknown) and an optional anchor_date (a date on which the holder is known to have held the ticker,
    from a `--probe-dates` lookup). Accepts the old puller output (no holder_id / FIGI columns) as well.
    """
    s = sm.copy()
    s["ticker"] = _norm_ticker(s["ticker"])
    for c in ("composite_figi", "cik", "holder_id"):
        if c not in s.columns:
            s[c] = pd.NA
    for c in ("effective_start", "effective_end", "anchor_date"):
        s[c] = _naive_dates(s[c]) if c in s.columns else pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")
    for c in ("start_confirmed", "end_confirmed"):
        s[c] = s[c].fillna(False).astype(bool) if c in s.columns else False
    hid = s["holder_id"].astype("string")
    need = (hid.isna() | (hid.str.strip() == "")).to_numpy()
    s.loc[need, "holder_id"] = [_holder_id(f, k, t) for f, k, t in
                                zip(s.loc[need, "composite_figi"], s.loc[need, "cik"], s.loc[need, "ticker"])]
    s["holder_id"] = s["holder_id"].astype(str)
    cols = ["ticker", "holder_id", "effective_start", "effective_end", "anchor_date", "start_confirmed", "end_confirmed"]
    if s.empty:
        return s[cols]
    # Same rules as polygon_pullers._dedupe_holders, vectorised (a loop over the 29k groups of the market security
    # master cost ~9 s per call): a confirmed start (real ticker change) beats an unconfirmed list_date; an open end
    # beats a stale delisted record of the same company.
    keys = ["ticker", "holder_id"]
    g = s.groupby(keys, sort=True)
    out = g.agg(all_start=("effective_start", "min"), all_end=("effective_end", "max"), anchor_date=("anchor_date", "min"),
                any_sc=("start_confirmed", "any"), any_ec=("end_confirmed", "any"))
    end_open = s["effective_end"].isna().groupby([s[k] for k in keys]).any().reindex(out.index)
    cs = s.loc[s["start_confirmed"]].groupby(keys)["effective_start"].min().reindex(out.index)
    ce = s.loc[s["end_confirmed"]].groupby(keys)["effective_end"].max().reindex(out.index)
    start = cs.where(cs.notna(), out["all_start"])
    end = ce.where(ce.notna(), out["all_end"]).where(~end_open.astype(bool), pd.NaT)
    out["effective_start"], out["effective_end"] = start, end
    out["start_confirmed"] = out["any_sc"] & start.notna()
    out["end_confirmed"] = out["any_ec"] & end.notna()
    out = out.reset_index()[cols]
    for c in ("effective_start", "effective_end", "anchor_date"):
        out[c] = pd.to_datetime(out[c]).astype("datetime64[ns]")
    return out


RECYCLE_GAP_DAYS = 60

def _segment_id(ticker: str, seg: int) -> str:
    """Id for a trading segment that predates the current holder of a recycled symbol."""
    return f"NOFIGI__{ticker}#SEG{int(seg)}"


def _is_segment_id(gid) -> bool:
    return isinstance(gid, str) and gid.startswith("NOFIGI__") and "#SEG" in gid


def _price_segments(tickers, days, gap_days: int = RECYCLE_GAP_DAYS) -> pd.DataFrame:
    """Cut each ticker's trading history where it stopped trading for `gap_days` or more.

    A symbol that goes quiet for months and comes back is almost always a **recycled ticker**: the
    exchange reassigned it to a different company. `ARM` printed bars from 2003 and Arm Holdings plc
    did not list until September 2023; `COIN` traded in 2007 and Coinbase listed in 2021. Returns one
    row per (ticker, segment) with the segment's first and last trading day.

    The same 60-day rule is used by `polygon_ingest.universe.trading_segments`, where it was chosen
    because Bear Stearns' symbol was reused after a 69-day gap.
    """
    d = pd.DataFrame({"ticker": np.asarray(tickers), "d": pd.to_datetime(np.asarray(days))}).drop_duplicates()
    if d.empty:
        return pd.DataFrame(columns=["ticker", "seg", "start", "end"])
    d = d.sort_values(["ticker", "d"])
    gap = d.groupby("ticker", sort=False)["d"].diff().dt.days
    d["seg"] = (gap.fillna(0) >= int(gap_days)).groupby(d["ticker"], sort=False).cumsum().astype(int)
    out = d.groupby(["ticker", "seg"], as_index=False)["d"].agg(start="min", end="max")
    return out.sort_values(["ticker", "seg"]).reset_index(drop=True)


def _recyclable_tickers(sm: pd.DataFrame) -> set:
    """Tickers whose whole history would otherwise be credited to one UNCONFIRMED holder.

    The security master carries a confirmed start only when a real ticker change or delisting was
    observed (`--probe-dates` / `--events`). Without one, `_assign_holder_ids` gives today's company
    every bar the symbol ever printed. Those are exactly the tickers where a trading gap is the only
    evidence of a previous owner, so segmentation is limited to them: a ticker with several known
    holders, or with a confirmed window, keeps the existing date-based logic untouched.
    """
    smn = _normalize_sm(sm)
    if smn.empty:
        return set()
    n = smn.groupby("ticker")["holder_id"].nunique()
    single = smn[smn["ticker"].map(n) == 1]
    return set(single.loc[~single["start_confirmed"].fillna(False).astype(bool), "ticker"])


def _recycled_segments(tickers, days, sm: pd.DataFrame, gap_days: int = RECYCLE_GAP_DAYS) -> pd.DataFrame:
    """Segments that need their own id: every segment except the last, for recyclable tickers only."""
    if gap_days <= 0:
        return pd.DataFrame(columns=["ticker", "seg", "start", "end", "id", "is_last"])
    cand = _recyclable_tickers(sm)
    if not cand:
        return pd.DataFrame(columns=["ticker", "seg", "start", "end", "id", "is_last"])
    t = pd.Series(np.asarray(tickers), dtype="object")
    keep = t.isin(cand).to_numpy()
    if not keep.any():
        return pd.DataFrame(columns=["ticker", "seg", "start", "end", "id", "is_last"])
    segs = _price_segments(t[keep].to_numpy(), np.asarray(days)[keep], gap_days)
    if segs.empty:
        return segs.assign(id=pd.Series(dtype=object), is_last=pd.Series(dtype=bool))
    last = segs.groupby("ticker")["seg"].transform("max")
    segs["is_last"] = segs["seg"].eq(last)
    segs = segs[segs.groupby("ticker")["seg"].transform("size") > 1]        # only genuinely split histories
    if segs.empty:
        return segs.assign(id=pd.Series(dtype=object))
    segs["id"] = pd.Series([None if r.is_last else _segment_id(r.ticker, r.seg) for r in segs.itertuples()],
                           index=segs.index, dtype=object)
    return segs.reset_index(drop=True)


def _segment_lookup(segs: pd.DataFrame):
    """(ticker -> (starts, ends, ids)) for fast containment tests."""
    out = {}
    for tk, g in segs.groupby("ticker", sort=False):
        g = g.sort_values("seg")
        out[tk] = (g["start"].to_numpy("datetime64[ns]"), g["end"].to_numpy("datetime64[ns]"),
                   g["id"].to_numpy(dtype=object))
    return out


def _apply_segment_ids(ids: pd.Series, tickers, dates, segs: pd.DataFrame) -> pd.Series:
    """Override the holder id on rows that fall in a pre-last segment of a recycled ticker."""
    if segs is None or segs.empty:
        return ids
    look = _segment_lookup(segs)
    tk = np.asarray(tickers, dtype=object)
    dt = pd.to_datetime(pd.Series(dates)).to_numpy("datetime64[ns]")
    out = ids.to_numpy(dtype=object).copy()
    for i, t in enumerate(tk):
        w = look.get(t)
        if w is None:
            continue
        starts, ends, sid = w
        j = np.searchsorted(ends, dt[i], side="left")        # first segment whose end >= this date
        if j >= len(sid):
            j = len(sid) - 1                                  # after the last segment: current holder
        elif dt[i] < starts[j] and j > 0:
            j -= 1                                            # in a gap: belongs to the segment it follows
        v = sid[j]
        if isinstance(v, str) and v:      # the last segment stores no id: it keeps the real holder
            out[i] = v
    return pd.Series(out, index=ids.index, dtype=object)


def _assign_holder_ids(df: pd.DataFrame, sm: pd.DataFrame, date_col: str) -> pd.Series:
    """
    Holder id for each row of `df` (needs 'ticker' and `date_col`), from the security master.

    - ticker unknown to the SM             -> 'NOFIGI__<TICKER>'
    - one holder (one distinct id)         -> that id for every row, whatever the date, EXCEPT rows before a
      CONFIRMED start or after a CONFIRMED end (a real ticker change / delisting from ticker events), which go
      to 'NOFIGI__<TICKER>' (unknown previous/next holder) rather than to a company that did not hold the
      symbol then. An imprecise list_date is never confirmed, so it can never split one company's history
      (each id anchors its adjustment factors to its own last day, so a spurious split breaks continuity).
    - several holders (a recycled ticker)  -> the holder whose bounded window contains the date; a holder with
      no known dates (found by probing a date) takes what bounded holders don't claim; else the nearest window.
      Ties: the later effective_start.
    """
    smn = _normalize_sm(sm)
    t = pd.DataFrame({
        "ticker": _norm_ticker(df["ticker"]).to_numpy(),
        "_d": _naive_dates(df[date_col]).to_numpy(),
        "_row": np.arange(len(df)),
    })
    out = np.array(["NOFIGI__" + x for x in t["ticker"]], dtype=object)
    if smn.empty or t.empty:
        return pd.Series(out, index=df.index, dtype=object)

    n_holders = smn.groupby("ticker")["holder_id"].nunique()
    single = smn[smn["ticker"].map(n_holders) == 1][["ticker", "holder_id", "effective_start", "effective_end",
                                                        "start_confirmed", "end_confirmed"]]
    m1 = t.merge(single, on="ticker", how="left")
    has = m1["holder_id"].notna()
    before = has & m1["start_confirmed"].fillna(False).astype(bool) & m1["effective_start"].notna() & (m1["_d"] < m1["effective_start"])
    after = has & m1["end_confirmed"].fillna(False).astype(bool) & m1["effective_end"].notna() & (m1["_d"] > m1["effective_end"])
    take = (has & ~before & ~after).to_numpy()
    out[take] = m1.loc[take, "holder_id"].to_numpy()

    multi = set(n_holders[n_holders > 1].index)
    tm = t[t["ticker"].isin(multi)]
    if len(tm):
        c = tm.merge(smn[smn["ticker"].isin(multi)], on="ticker", how="inner")
        d, st, en = c["_d"], c["effective_start"], c["effective_end"]
        bounded = st.notna() | en.notna()
        inwin = (st.isna() | (d >= st)) & (en.isna() | (d <= en))
        rank = np.where(bounded & inwin, 0, np.where(~bounded, 1, 2))
        dist_bound = pd.concat([(d - st).abs(), (d - en).abs()], axis=1).min(axis=1).dt.days
        dist_anchor = (d - c["anchor_date"]).abs().dt.days.fillna(10**6)
        c["_rank"] = rank
        c["_dist"] = np.where(rank == 2, dist_bound, np.where(rank == 1, dist_anchor, 0.0))
        c = c.sort_values(["_row", "_rank", "_dist", "effective_start"],
                          ascending=[True, True, True, False], na_position="last")
        best = c.drop_duplicates("_row", keep="first")
        out[best["_row"].to_numpy()] = best["holder_id"].to_numpy()
    return pd.Series(out, index=df.index, dtype=object)


def _assign_event_ids(events: pd.DataFrame, sm: pd.DataFrame, date_cols) -> pd.DataFrame:
    """
    Add 'holder_id' to a splits/dividends table by (ticker, event date), so corporate actions are keyed
    exactly like price rows (Polygon's splits/dividends endpoints carry a ticker but no FIGI).
    """
    e = events.copy()
    if "ticker" not in e.columns and "T" in e.columns:
        e = e.rename(columns={"T": "ticker"})
    if e.empty:
        e["holder_id"] = pd.Series(dtype=object)
        return e
    dcol = next((c for c in date_cols if c in e.columns), None)
    if dcol is None:
        raise ValueError(f"events table has none of the date columns {list(date_cols)}")
    e["ticker"] = _norm_ticker(e["ticker"])
    e["holder_id"] = _assign_holder_ids(e, sm, dcol)
    return e


def _apply_event_segments(events: pd.DataFrame, segs: pd.DataFrame, date_cols) -> pd.DataFrame:
    """Re-key corporate actions onto trading segments, so a company's splits cannot reach the bars of
    the previous owner of its symbol. SiriusXM's 2024 reverse split must not touch 2007 Sirius bars."""
    if segs is None or segs.empty or events.empty:
        return events
    dcol = next((c for c in date_cols if c in events.columns), None)
    if dcol is None:
        return events
    e = events.copy()
    # `holder_id` is what _assign_event_ids wrote and what _prep_splits/_prep_dividends turn into
    # `event_id`; update whichever is present so the re-keying survives either call order.
    for col in ("holder_id", "event_id"):
        if col in e.columns:
            e[col] = _apply_segment_ids(e[col].astype(object), _norm_ticker(e["ticker"]).to_numpy(),
                                        _naive_dates(e[dcol]), segs)
    return e


def _event_ids(df: pd.DataFrame) -> np.ndarray:
    """event_id = holder_id (from _assign_event_ids) -> composite_figi -> 'NOFIGI__<TICKER>'."""
    hid = df["holder_id"] if "holder_id" in df.columns else pd.Series(pd.NA, index=df.index)
    figi = df["composite_figi"] if "composite_figi" in df.columns else pd.Series(pd.NA, index=df.index)
    nofigi = ("NOFIGI__" + df["ticker"].astype(str)).to_numpy()
    return np.where(hid.notna().to_numpy(), hid.astype(str).to_numpy(),
                    np.where(figi.notna().to_numpy(), figi.astype(str).to_numpy(), nofigi))


class _EventIndex:
    """A splits/dividends table pre-grouped by holder id and by ticker. The streaming builders look events up once
    per (ticker, holder); scanning the full-market table (470k dividends) for each of 30k+ holders cost minutes."""
    def __init__(self, table: pd.DataFrame, date_col: str, cols: List[str]):
        self.date_col, self.cols = date_col, cols
        self.empty = table.iloc[0:0][cols]
        self.by_id = {k: g[cols].dropna().sort_values(date_col) for k, g in table.groupby("event_id", sort=False)}
        self.by_ticker = {k: g[cols].dropna().sort_values(date_col) for k, g in table.groupby("ticker", sort=False)}

    def get(self, gid: str, ticker: str) -> pd.DataFrame:
        """Same rule as _events_for_holder: only a NOFIGI__ id may fall back to ticker-keyed events."""
        ev = self.by_id.get(gid)
        if (ev is None or ev.empty) and str(gid).startswith("NOFIGI__") and not _is_segment_id(gid):
            ev = self.by_ticker.get(ticker)
        return self.empty if ev is None else ev


def _events_for_holder(table: pd.DataFrame, gid: str, ticker: str, date_col: str, cols: List[str]) -> pd.DataFrame:
    """
    Events keyed to this holder. Only an id without FIGI/CIK ('NOFIGI__') may fall back to ticker-keyed
    events: for a real holder an empty result means the company had none, and borrowing the ticker's events
    would apply another company's splits/dividends to its prices.
    """
    ev = table[table["event_id"] == gid][cols].dropna()
    # A segment id already collected its own events by date in _apply_event_segments; falling back to
    # every event under the ticker would hand an earlier company the later owner's splits.
    if ev.empty and str(gid).startswith("NOFIGI__") and not _is_segment_id(gid):
        ev = table[table["ticker"] == ticker][cols].dropna()
    return ev.sort_values(date_col)


def _attach_id(prx: pd.DataFrame, security_master: pd.DataFrame,
               segments: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """Attach the holder id per price row; never drops rows (out-of-window rows go to the nearest holder).

    `segments` (from `_recycled_segments`) moves bars printed before a recycled symbol's current
    owner onto their own id, so a 2003 `ARM` bar is not credited to a company that listed in 2023.
    """
    prx = prx.copy()
    prx["ticker"] = _norm_ticker(prx["ticker"])
    prx["datetime"] = _to_naive_utc(prx["datetime"])   # tz-naive UTC
    prx["event_day"] = _trading_day(prx["datetime"])    # US/Eastern trading date
    prx["id"] = _assign_holder_ids(prx, security_master, "event_day")
    if segments is not None and len(segments):
        prx["id"] = _apply_segment_ids(prx["id"], prx["ticker"].to_numpy(), prx["event_day"], segments)
    return prx


# =============================================================
# Prep reference tables
# =============================================================

def _prep_splits(splits: pd.DataFrame) -> pd.DataFrame:
    s = splits.copy()
    if "execution_date" not in s.columns:
        raise ValueError("splits is missing 'execution_date'")
    if "ratio" not in s.columns and {"split_from", "split_to"} <= set(s.columns):
        s["ratio"] = s["split_to"].astype(float) / s["split_from"].astype(float)
    if "ratio" not in s.columns:
        raise ValueError("splits missing 'ratio' (or split_from/split_to)")

    s["execution_date"] = pd.to_datetime(s["execution_date"]).dt.normalize().dt.as_unit("ns")
    s["ratio"] = s["ratio"].astype(float)

    if "ticker" not in s.columns and "T" in s.columns:
        s = s.rename(columns={"T": "ticker"})
    if "ticker" not in s.columns:
        raise ValueError("splits is missing 'ticker'")
    s["ticker"] = _norm_ticker(s["ticker"])

    if "composite_figi" not in s.columns:
        s["composite_figi"] = pd.NA

    s["event_id"] = _event_ids(s)
    return s[["execution_date", "ratio", "ticker", "composite_figi", "event_id"]]

def _prep_dividends(dividends: pd.DataFrame) -> pd.DataFrame:
    d = dividends.copy()
    ex_col = "ex_date" if "ex_date" in d.columns else ("ex_dividend_date" if "ex_dividend_date" in d.columns else None)
    amt_col = "amount" if "amount" in d.columns else ("cash_amount" if "cash_amount" in d.columns else None)
    if ex_col is None or amt_col is None:
        raise ValueError("dividends missing ex-date or amount")

    d = d.rename(columns={ex_col: "ex_date", amt_col: "amount"})
    d["ex_date"] = pd.to_datetime(d["ex_date"]).dt.normalize().dt.as_unit("ns")

    if "ticker" not in d.columns and "T" in d.columns:
        d = d.rename(columns={"T": "ticker"})
    if "ticker" not in d.columns:
        raise ValueError("dividends is missing 'ticker'")
    d["ticker"] = _norm_ticker(d["ticker"])

    if "composite_figi" not in d.columns:
        d["composite_figi"] = pd.NA

    d["event_id"] = _event_ids(d)
    return d[["ex_date", "amount", "ticker", "composite_figi", "event_id"]]


# =============================================================
# Per-id workers (BATCH MODE)
# =============================================================

def _split_factors_for_id_worker(payload: Tuple[str, pd.DataFrame]) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    gid, gpx = payload
    s = _SPLITS_TABLE
    if s is None:
        raise RuntimeError("Worker missing splits table")

    days = pd.DataFrame({"event_day": np.sort(gpx["event_day"].unique())})
    tick = gpx["ticker"].iloc[0]

    ev = s[s["event_id"] == gid][["execution_date", "ratio"]].dropna()
    used_fallback = False
    if ev.empty and str(gid).startswith("NOFIGI__") and not _is_segment_id(gid):
        ev = s[s["ticker"] == tick][["execution_date", "ratio"]].dropna()
        used_fallback = True
    ev = ev.sort_values("execution_date")

    if ev.empty:
        out = days.copy()
        out["split_price_factor"] = 1.0
        out["split_volume_factor"] = 1.0
        stats = {"ticker": tick, "events_aligned": 0, "cum_ratio": 1.0,
                 "last_raw_date": pd.NaT, "last_aligned_day": pd.NaT, "fallback": used_fallback}
    else:
        aligned = pd.merge_asof(
            ev.rename(columns={"execution_date": "event_anchor"}),
            days.rename(columns={"event_day": "event_anchor"}),
            on="event_anchor",
            direction="forward",
            allow_exact_matches=True
        ).rename(columns={"event_anchor": "event_day"}).dropna(subset=["event_day"])

        per_day = aligned.groupby("event_day", as_index=False)["ratio"].prod()
        e = days.merge(per_day, on="event_day", how="left")
        e["ratio"] = e["ratio"].fillna(1.0)
        e["F"] = e["ratio"].cumprod()
        F_last = float(e["F"].iloc[-1])
        out = days.copy()
        out["split_price_factor"]  = e["F"] / F_last
        out["split_volume_factor"] = F_last / e["F"]

        stats = {
            "ticker": tick,
            "events_aligned": int((per_day["ratio"].fillna(1.0) != 1.0).sum()),
            "cum_ratio": float(per_day["ratio"].prod()) if len(per_day) else 1.0,
            "last_raw_date": ev["execution_date"].max(),
            "last_aligned_day": per_day["event_day"].max() if len(per_day) else pd.NaT,
            "fallback": used_fallback,
        }

    out["id"] = gid
    return out, stats


def _dividend_factors_for_id_worker(payload: Tuple[str, pd.DataFrame, bool]) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    gid, gpx, use_split_base = payload
    d = _DIVS_TABLE
    if d is None:
        raise RuntimeError("Worker missing dividends table")

    gpx = gpx.sort_values("datetime").copy()
    use_split = bool(use_split_base and "close_split" in gpx.columns)
    base_series = gpx["close_split"] if use_split else gpx["close"]
    gpx["prior_base"] = base_series.shift(1)
    # Split factor in force on each day. Polygon reports cash dividends in raw (as-declared)
    # dollars per share; when the base is split-adjusted the amount must be scaled the same
    # way, or a pre-split dividend looks split_ratio times too large.
    if use_split and "split_price_factor" in gpx.columns:
        gpx["spf"] = gpx["split_price_factor"].astype(float).fillna(1.0)
    else:
        gpx["spf"] = 1.0

    cal = (gpx[["event_day", "prior_base", "spf"]]
           .drop_duplicates("event_day")
           .sort_values("event_day"))

    tick = gpx["ticker"].iloc[0]

    ev = d[d["event_id"] == gid][["ex_date", "amount"]].dropna()
    used_fallback = False
    if ev.empty and str(gid).startswith("NOFIGI__") and not _is_segment_id(gid):
        ev = d[d["ticker"] == tick][["ex_date", "amount"]].dropna()
        used_fallback = True
    ev = ev.sort_values("ex_date")

    if ev.empty:
        T = cal[["event_day"]].copy()
        T["tr_price_factor"] = 1.0
        stats = {"ticker": tick, "event_days": 0, "total_cash": 0.0, "base": "split" if use_split_base else "raw",
                 "last_raw_date": pd.NaT, "last_aligned_day": pd.NaT, "fallback": used_fallback}
    else:
        aligned = pd.merge_asof(
            ev.rename(columns={"ex_date": "event_anchor"}),
            cal.rename(columns={"event_day": "event_anchor"}),
            on="event_anchor",
            direction="forward",
            allow_exact_matches=True
        ).rename(columns={"event_anchor": "event_day"}).dropna(subset=["event_day"])

        per_day_amt = aligned.groupby("event_day", as_index=False)["amount"].sum()
        T = cal.merge(per_day_amt, on="event_day", how="left", validate="one_to_one")

        # g_t = fraction of prior-day value retained after the ex-date cash payout (< 1).
        # The backward total-return factor on day t reinvests every dividend that goes ex
        # AFTER t:  prod_{s>t} g_s = G_last / G_t.  (G_t / G_last would charge the holder.)
        T["g"] = 1.0
        mask = T["amount"].notna() & T["prior_base"].notna() & (T["prior_base"] > 0)
        amt_adj = T.loc[mask, "amount"] * T.loc[mask, "spf"]
        T.loc[mask, "g"] = (T.loc[mask, "prior_base"] - amt_adj) / T.loc[mask, "prior_base"]
        T["G"] = T["g"].cumprod()
        G_last = float(T["G"].iloc[-1])
        T["tr_price_factor"] = G_last / T["G"]

        stats = {
            "ticker": tick,
            "event_days": int((per_day_amt["amount"] > 0).sum()),
            "total_cash": float(per_day_amt["amount"].sum()),
            "base": "split" if use_split_base else "raw",
            "last_raw_date": ev["ex_date"].max(),
            "last_aligned_day": per_day_amt["event_day"].max() if len(per_day_amt) else pd.NaT,
            "fallback": used_fallback,
        }

    return T[["event_day", "tr_price_factor"]].assign(id=gid), stats


# =============================================================
# Batch builders (invoke workers)
# =============================================================

def _build_split_factors(px_df: pd.DataFrame,
                         splits: pd.DataFrame,
                         stats: dict,
                         workers: int = 1) -> pd.DataFrame:
    global _SPLITS_TABLE   # inline (workers<=1) path must set the module global the worker reads
    S = _prep_splits(splits)
    out_parts = []
    split_stats: Dict[str, Dict[str, Any]] = {}

    groups = [(gid, g[["ticker","event_day"]].copy()) for gid, g in px_df.groupby("id")]
    if workers <= 1:
        for gid, g in _tqdm(groups, desc="Split factors per id"):
            _SPLITS_TABLE = S
            res, st = _split_factors_for_id_worker((gid, g))
            out_parts.append(res)
            split_stats[gid] = st
    else:
        with ProcessPoolExecutor(max_workers=workers,
                                 initializer=_init_worker,
                                 initargs=(S, None)) as ex:
            futs = {ex.submit(_split_factors_for_id_worker, (gid, g)): gid for gid, g in groups}
            for fut in _tqdm(as_completed(futs), total=len(futs), desc=f"Split factors per id"):
                gid = futs[fut]
                res, st = fut.result()
                out_parts.append(res)
                split_stats[gid] = st

    stats["splits"] = split_stats
    return pd.concat(out_parts, ignore_index=True)

def _build_dividend_factors(px_df: pd.DataFrame,
                            dividends: pd.DataFrame,
                            use_split_base: bool,
                            stats: dict,
                            workers: int = 1) -> pd.DataFrame:
    global _DIVS_TABLE     # inline (workers<=1) path must set the module global the worker reads
    D = _prep_dividends(dividends)
    out_parts = []
    div_stats: Dict[str, Dict[str, Any]] = {}

    groups = [(gid, g.sort_values("datetime").copy()) for gid, g in px_df.groupby("id")]
    if workers <= 1:
        for gid, g in _tqdm(groups, desc="Dividend factors per id"):
            _DIVS_TABLE = D
            res, st = _dividend_factors_for_id_worker((gid, g, use_split_base))
            out_parts.append(res)
            div_stats[gid] = st
    else:
        with ProcessPoolExecutor(max_workers=workers,
                                 initializer=_init_worker,
                                 initargs=(None, D)) as ex:
            futs = {ex.submit(_dividend_factors_for_id_worker, (gid, g, use_split_base)): gid for gid, g in groups}
            for fut in _tqdm(as_completed(futs), total=len(futs), desc=f"Dividend factors per id"):
                gid = futs[fut]
                res, st = fut.result()
                out_parts.append(res)
                div_stats[gid] = st

    stats["dividends"] = div_stats
    return pd.concat(out_parts, ignore_index=True)


# =============================================================
# Apply factors (BATCH)
# =============================================================

def _apply_splits(px_id: pd.DataFrame, F: pd.DataFrame) -> pd.DataFrame:
    m = px_id.merge(F, on=["id", "event_day"], how="left")
    m["split_price_factor"]  = m["split_price_factor"].fillna(1.0)
    m["split_volume_factor"] = m["split_volume_factor"].fillna(1.0)
    m["close_split"]  = m["close"]  * m["split_price_factor"]
    m["volume_split"] = m["volume"] * m["split_volume_factor"]
    for col in ("open","high","low"):
        if col in m.columns:
            m[f"{col}_split"] = m[col] * m["split_price_factor"]
    return m

def _apply_dividends(px_df: pd.DataFrame, G: pd.DataFrame, use_split_base: bool) -> pd.DataFrame:
    m = px_df.merge(G, on=["id", "event_day"], how="left")
    m["tr_price_factor"] = m["tr_price_factor"].fillna(1.0)
    base_col = "close_split" if use_split_base and "close_split" in m.columns else "close"
    m["close_tr"] = m[base_col] * m["tr_price_factor"]
    for col in ("open_split","high_split","low_split"):
        if col in m.columns:
            m[col.replace("_split","_tr")] = m[col] * m["tr_price_factor"]
    return m

def _renormalize_tr_to_one(px_df: pd.DataFrame, use_split_base: bool) -> pd.DataFrame:
    base_col = "close_split" if use_split_base and "close_split" in px_df.columns else "close"
    df = px_df.sort_values(["id", "datetime"]).copy()
    last_vals = (df.groupby("id", as_index=False)[["close_tr", base_col]]
                   .last()
                   .rename(columns={"close_tr": "_last_tr", base_col: "_last_base"}))
    df = df.merge(last_vals, on="id", how="left")
    df["_renorm"] = df["_last_tr"] / df["_last_base"]
    df["_renorm"] = df["_renorm"].where(df["_renorm"].notna() & (df["_renorm"] != 0), 1.0)

    df["tr_price_factor"] = df["tr_price_factor"] / df["_renorm"]
    df["close_tr"]        = df[base_col] * df["tr_price_factor"]
    for col in ("open_split","high_split","low_split"):
        if col in df.columns:
            df[col.replace("_split","_tr")] = df[col] * df["tr_price_factor"]
    return df.drop(columns=["_last_tr", "_last_base", "_renorm"])


# =============================================================
# Writers
# =============================================================

def _write_one_parquet(outpath: Path, g: pd.DataFrame) -> None:
    outpath.parent.mkdir(parents=True, exist_ok=True)
    g.to_parquet(outpath, index=False)

def _select_columns_to_write(df: pd.DataFrame, materialize: str) -> List[str]:
    base = ["datetime","ticker","id","close","volume","close_split","volume_split","close_tr"]
    if materialize == "minimal":
        return [c for c in base if c in df.columns]
    if materialize == "close":
        extra = ["split_price_factor","tr_price_factor"]
        return [c for c in base + extra if c in df.columns]
    ohlc = ["open_split","high_split","low_split","open_tr","high_tr","low_tr"]
    extra = ["split_price_factor","tr_price_factor"]
    cols = base + extra + [c for c in ohlc if c in df.columns]
    return [c for c in cols if c in df.columns]

def _write_partitioned_lake(df: pd.DataFrame, outdir: Path, granularity: str, write_workers: int, materialize: str,
                            layout: str = "ticker") -> None:
    df = df.copy()
    df["datetime"] = pd.to_datetime(df["datetime"]).dt.tz_localize(None)
    df["YYYY"] = df["datetime"].dt.year
    df["MM"]   = df["datetime"].dt.month
    if granularity == "minute":
        df["DD"] = df["datetime"].dt.day

    if layout == "market":
        # all tickers per period file, ticker-major: <out>/<YYYY>/<MM>.parquet (day) or /<DD>.parquet (minute)
        df = df.sort_values(["ticker", "datetime"]).reset_index(drop=True)
        cols_to_write = _select_columns_to_write(df, materialize)
        key_cols = ["YYYY", "MM"] + (["DD"] if granularity == "minute" else [])

        def _mpath(k) -> Path:
            y, m, *d = (int(x) for x in k)
            return (outdir / f"{y:04d}" / f"{m:02d}" / f"{d[0]:02d}.parquet") if d else (outdir / f"{y:04d}" / f"{m:02d}.parquet")

        groups = list(df.groupby(key_cols))
        desc = f"Writing {granularity} lake (market layout" + (f", parallel x{write_workers})" if write_workers > 1 else ")")
        if write_workers <= 1:
            for k, g in _tqdm(groups, desc=desc):
                _write_one_parquet(_mpath(k), g[cols_to_write])
        else:
            with ThreadPoolExecutor(max_workers=write_workers) as ex:
                futs = [ex.submit(_write_one_parquet, _mpath(k), g[cols_to_write]) for k, g in groups]
                for _ in _tqdm(as_completed(futs), total=len(futs), desc=desc):
                    _.result()
        return

    df = df.sort_values(["ticker", "datetime"]).reset_index(drop=True)
    cols_to_write = _select_columns_to_write(df, materialize)

    if granularity == "day":
        key_cols = ["ticker", "YYYY", "MM"]
        n_tasks = int(df[key_cols].drop_duplicates().shape[0])
        desc = "Writing day lake" if write_workers <= 1 else f"Writing day lake (parallel x{write_workers})"
        if write_workers <= 1:
            for (t, y, m), g in _tqdm(df.groupby(key_cols), desc=desc):
                outpath = outdir / t / f"{int(y):04d}" / f"{int(m):02d}.parquet"
                _write_one_parquet(outpath, g[cols_to_write].drop(columns=["YYYY","MM"], errors="ignore"))
        else:
            with ThreadPoolExecutor(max_workers=write_workers) as ex:
                futures = []
                for (t, y, m), g in df.groupby(key_cols):
                    outpath = outdir / t / f"{int(y):04d}" / f"{int(m):02d}.parquet"
                    futures.append(ex.submit(_write_one_parquet, outpath, g[cols_to_write].drop(columns=["YYYY","MM"], errors="ignore")))
                for _ in _tqdm(as_completed(futures), total=n_tasks, desc=desc):
                    _.result()

    elif granularity == "minute":
        key_cols = ["ticker", "YYYY", "MM", "DD"]
        n_tasks = int(df[key_cols].drop_duplicates().shape[0])
        desc = "Writing minute lake" if write_workers <= 1 else f"Writing minute lake (parallel x{write_workers})"
        if write_workers <= 1:
            for (t, y, m, d), g in _tqdm(df.groupby(key_cols), desc=desc):
                outpath = outdir / t / f"{int(y):04d}" / f"{int(m):02d}" / f"{int(d):02d}.parquet"
                _write_one_parquet(outpath, g[cols_to_write].drop(columns=["YYYY","MM","DD"], errors="ignore"))
        else:
            with ThreadPoolExecutor(max_workers=write_workers) as ex:
                futures = []
                for (t, y, m, d), g in df.groupby(key_cols):
                    outpath = outdir / t / f"{int(y):04d}" / f"{int(m):02d}" / f"{int(d):02d}.parquet"
                    futures.append(ex.submit(_write_one_parquet, outpath, g[cols_to_write].drop(columns=["YYYY","MM","DD"], errors="ignore")))
                for _ in _tqdm(as_completed(futures), total=n_tasks, desc=desc):
                    _.result()
    else:
        raise ValueError("granularity must be 'day' or 'minute'")


# =============================================================
# Manifest copy
# =============================================================

def _find_manifest_near_prices(prices: Path) -> Optional[Path]:
    base = prices if prices.is_dir() else prices.parent
    for p in [base, base.parent, base.parent.parent]:
        if not p or str(p) == str(p.parent):
            continue
        cand = p / "manifest.json"
        if cand.exists():
            return cand
    return None

def _copy_manifest(prices: Path, outdir: Path, manifest_src: Optional[Path], skip: bool = False) -> Optional[Path]:
    if skip:
        return None
    src = None
    if manifest_src is not None:
        src = Path(manifest_src)
        if not src.exists():
            print(f"[WARN] --manifest-src provided but not found: {src}")
            src = None
    if src is None:
        src = _find_manifest_near_prices(prices)
    if src is None:
        print("[INFO] No manifest.json found near --prices; skipping copy.")
        return None
    dst = outdir / "manifest.json"
    try:
        shutil.copy2(src, dst)
        print(f"[INFO] Copied manifest.json from {src} -> {dst}")
        return dst
    except Exception as e:
        print(f"[WARN] Failed to copy manifest.json: {e}")
        return None


# =============================================================
# Summary output
# =============================================================

_SUMMARY_COLUMNS = ["id", "ticker", "adjust_mode", "tr_base", "split_events_aligned", "split_cum_ratio",
                    "last_split_raw_date", "last_split_aligned_day", "dividend_event_days", "dividend_total_cash",
                    "last_dividend_raw_date", "last_dividend_aligned_day", "last_datetime", "used_fallback"]

def _write_summary_rows(rows: List[dict], outdir: Path) -> Path:
    summary = pd.DataFrame(rows, columns=_SUMMARY_COLUMNS).sort_values(["ticker", "id"]).reset_index(drop=True)
    outpath = outdir / "_event_summary.csv"
    summary.to_csv(outpath, index=False)
    return outpath

def _write_summary_csv(stats: dict, outdir: Path, px_df: pd.DataFrame, adjust_mode: str, use_split_base: bool) -> Path:
    return _write_summary_rows(_summary_rows(stats, px_df, adjust_mode, use_split_base), outdir)

def _summary_rows(stats: dict, px_df: pd.DataFrame, adjust_mode: str, use_split_base: bool) -> List[dict]:
    rows = []
    ids = sorted(px_df["id"].unique())
    last_dt_per_id = px_df.groupby("id", as_index=False)["datetime"].max().rename(columns={"datetime": "last_datetime"})
    last_dt_map = dict(zip(last_dt_per_id["id"], last_dt_per_id["last_datetime"]))

    for gid in ids:
        split_rec = (stats.get("splits", {}) or {}).get(gid, {})
        div_rec   = (stats.get("dividends", {}) or {}).get(gid, {})

        ticker = (split_rec.get("ticker") or
                  div_rec.get("ticker") or
                  px_df.loc[px_df["id"] == gid, "ticker"].iloc[0])

        rows.append({
            "id": gid,
            "ticker": ticker,
            "adjust_mode": adjust_mode,
            "tr_base": "split" if (use_split_base and adjust_mode in ("both", "dividends")) else "-",
            "split_events_aligned": int(split_rec.get("events_aligned", 0)),
            "split_cum_ratio": float(split_rec.get("cum_ratio", 1.0)),
            "last_split_raw_date": split_rec.get("last_raw_date"),
            "last_split_aligned_day": split_rec.get("last_aligned_day"),
            "dividend_event_days": int(div_rec.get("event_days", 0)),
            "dividend_total_cash": float(div_rec.get("total_cash", 0.0)),
            "last_dividend_raw_date": div_rec.get("last_raw_date"),
            "last_dividend_aligned_day": div_rec.get("last_aligned_day"),
            "last_datetime": last_dt_map.get(gid),
            "used_fallback": bool(split_rec.get("fallback")) or bool(div_rec.get("fallback")),
        })
    return rows

def _print_aligned_summary(summary: pd.DataFrame) -> None:
    dt_cols = ["last_split_raw_date", "last_split_aligned_day",
               "last_dividend_raw_date", "last_dividend_aligned_day", "last_datetime"]
    for c in dt_cols:
        if c in summary.columns and not np.issubdtype(summary[c].dtype, np.datetime64):
            summary[c] = pd.to_datetime(summary[c], errors="coerce")

    def fdate(x):
        return x.date().isoformat() if pd.notna(x) else "—"

    rows = []
    for _, r in summary.iterrows():
        tkr = str(r["ticker"])
        splits = int(r["split_events_aligned"])
        cumr = float(r["split_cum_ratio"])
        ls_raw = fdate(r.get("last_split_raw_date"))
        ls_align = fdate(r.get("last_split_aligned_day"))
        divd = int(r["dividend_event_days"])
        cash = float(r["dividend_total_cash"])
        ld_raw = fdate(r.get("last_dividend_raw_date"))
        ld_align = fdate(r.get("last_dividend_aligned_day"))
        trb = str(r["tr_base"])

        col_split = f"splits={splits:>3d} (cum_ratio={cumr:g}, last={ls_raw}→{ls_align})"
        col_div   = f"div_days={divd:>3d} (cash=${cash:.2f}, last={ld_raw}→{ld_align})"
        col_tr    = f"TR base={trb}"
        rows.append((tkr, col_split, col_div, col_tr))

    w_t = max(len(t) for t, _, _, _ in rows + [("TICKER", "", "", "")])
    w_s = max(len(s) for _, s, _, _ in rows + [("", "SPLITS", "", "")])
    w_d = max(len(d) for _, _, d, _ in rows + [("", "", "DIVIDENDS", "")])
    w_tr = max(len(tr) for _, _, _, tr in rows + [("", "", "", "TR")])

    header = f"{'TICKER'.ljust(w_t)} | {'SPLITS'.ljust(w_s)} | {'DIVIDENDS'.ljust(w_d)} | {'TR'.ljust(w_tr)}"
    sep = "-" * len(header)
    print("\n===== Event Alignment Summary =====")
    print(header)
    print(sep)
    for t, s, d, tr in rows:
        print(f"{t.ljust(w_t)} | {s.ljust(w_s)} | {d.ljust(w_d)} | {tr.ljust(w_tr)}")


# =============================================================
# Streaming helpers (minute mode)
# =============================================================

def _iter_minute_day_files(root: Path, tickers: Optional[List[str]]) -> List[Tuple[str, Path, pd.Timestamp]]:
    """Return list of (ticker, file_path, event_day) for minute lake: <root>/<TICKER>/<YYYY>/<MM>/<DD>.parquet"""
    root = Path(root)
    tset = {t.strip().casefold() for t in tickers} if tickers else None
    out: List[Tuple[str, Path, pd.Timestamp]] = []

    for tdir in sorted([p for p in root.iterdir() if p.is_dir()]):
        # FIX: do NOT use pandas .str accessor on a Python string
        tkr = str(tdir.name).strip()
        if tset and tkr.casefold() not in tset:
            continue

        for ydir in sorted([p for p in tdir.iterdir() if p.is_dir()]):
            for mdir in sorted([p for p in ydir.iterdir() if p.is_dir()]):
                for f in sorted(mdir.iterdir()):
                    if f.suffix != ".parquet":
                        continue
                    try:
                        day = pd.Timestamp(int(ydir.name), int(mdir.name), int(f.stem)).normalize().as_unit("ns")
                    except Exception:
                        continue
                    out.append((tkr, f, day))
    return out


def _attach_id_days(days: pd.DataFrame, sm: pd.DataFrame,
                    segments: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """Attach the holder id per (ticker, event_day, path) minute day-file; never drops days."""
    d = days.copy()
    d["ticker"] = _norm_ticker(d["ticker"])
    d["id"] = _assign_holder_ids(d, sm, "event_day")
    if segments is not None and len(segments):
        d["id"] = _apply_segment_ids(d["id"], d["ticker"].to_numpy(), d["event_day"], segments)
    return d[["ticker", "event_day", "id", "path"]]


def _read_first_last_close(path: Path, ticker: Optional[str]) -> Tuple[float, float]:
    """Return (first_close, last_close) for the specific ticker in a minute day-file; supports multi-ticker files."""
    try:
        df = pd.read_parquet(path, columns=["datetime","close","ticker"])
    except Exception:
        try:
            df = pd.read_parquet(path, columns=["datetime","close","T"]).rename(columns={"T":"ticker"})
        except Exception:
            df = pd.read_parquet(path, columns=["datetime","close"])
            ticker = None

    if ticker is not None and "ticker" in df.columns:
        df["ticker"] = _norm_ticker(df["ticker"])
        df = df[df["ticker"] == ticker]

    if df.empty:
        return (np.nan, np.nan)
    df = df.sort_values("datetime")
    return float(df["close"].iloc[0]), float(df["close"].iloc[-1])

def _read_first_last_close_task(task: Tuple[str, Any, Any]) -> Tuple[str, Any, float, float]:
    t, d, p = task
    try:
        f, l = _read_first_last_close(Path(p), t)
    except Exception:
        f, l = (np.nan, np.nan)
    return (t, d, f, l)

def _scan_day_edges(days_df: pd.DataFrame, threads: int = 4) -> pd.DataFrame:
    """For each (ticker, event_day, path), read first/last close; compute raw gap vs prior day last.
    threads > 1 spreads the reads over that many processes (one small file per task; the pandas work is GIL-bound)."""
    tasks = [(r.ticker, r.event_day, r.path) for r in days_df.itertuples()]
    desc = f"Scanning minute day edges (x{threads})"
    if threads <= 1:
        rows = [_read_first_last_close_task(t) for t in _tqdm(tasks, desc=desc)]
    else:
        with ProcessPoolExecutor(max_workers=threads, initializer=_worker_init,
                                 initargs=(_arrow_threads_per_worker(threads),)) as ex:
            rows = list(_tqdm(ex.map(_read_first_last_close_task, tasks, chunksize=256), total=len(tasks), desc=desc))
    edges = pd.DataFrame(rows, columns=["ticker","event_day","first_close","last_close"])
    edges = edges.sort_values(["ticker","event_day"])
    edges["prev_last"] = edges.groupby("ticker")["last_close"].shift(1)
    edges["raw_gap"] = edges["first_close"] / edges["prev_last"]
    return edges

def _guess_split_ratio_from_gap(gap: float) -> Optional[float]:
    if not np.isfinite(gap) or gap <= 0:
        return None
    inv = gap if gap > 1 else 1.0 / gap
    candidates = np.array([2, 3, 4, 5, 10, 20])
    idx = int(np.argmin(np.abs(candidates - inv)))
    r = float(candidates[idx])
    return r if abs(inv - r) / r <= 0.15 else None

def _build_split_factors_from_days(id_days: pd.DataFrame,
                                   spl: pd.DataFrame,
                                   edges: Optional[pd.DataFrame],
                                   detect_gaps: bool) -> pd.DataFrame:
    s = _prep_splits(spl)
    s_idx = _EventIndex(s, "execution_date", ["execution_date", "ratio"])
    out = []

    E = None
    if edges is not None:
        E = edges.copy()
        E["ticker"] = _norm_ticker(E["ticker"])

    # One holder (company) at a time: a recycled ticker's previous company must not receive the
    # current company's splits, and each holder anchors its own factors.
    for (tkr, gid), g in _tqdm(id_days.groupby(["ticker", "id"]), desc="Split factors per holder (days)"):
        # IMPORTANT: reset_index to avoid index misalignment later
        days = (g[["event_day"]]
                .drop_duplicates()
                .sort_values("event_day")
                .reset_index(drop=True))

        ev = s_idx.get(gid, tkr)

        if ev.empty:
            per_day = pd.DataFrame({"event_day": [], "ratio": []})
        else:
            aligned = pd.merge_asof(
                ev.rename(columns={"execution_date":"event_anchor"}),
                days.rename(columns={"event_day":"event_anchor"}),
                on="event_anchor", direction="forward", allow_exact_matches=True
            ).rename(columns={"event_anchor":"event_day"}).dropna(subset=["event_day"])
            per_day = aligned.groupby("event_day", as_index=False)["ratio"].prod()

        # Optional raw-gap detection/override
        if detect_gaps and E is not None:
            e_t = E[(E["ticker"] == tkr) & E["event_day"].isin(days["event_day"])].dropna(subset=["raw_gap"])
            if not e_t.empty:
                def _guess(gap: float) -> Optional[float]:
                    if not np.isfinite(gap) or gap <= 0: return None
                    inv = gap if gap > 1 else 1.0 / gap
                    cands = np.array([2,3,4,5,10,20])
                    r = float(cands[np.argmin(np.abs(cands - inv))])
                    return r if abs(inv - r) / r <= 0.15 else None

                e_t = e_t.copy()
                e_t["ratio_guess"] = e_t["raw_gap"].apply(_guess)
                cand = e_t.dropna(subset=["ratio_guess"])[["event_day","ratio_guess"]]
                if not cand.empty:
                    per_day = per_day.set_index("event_day")
                    for d, r in cand.itertuples(index=False):
                        window = per_day.loc[per_day.index.isin([d - pd.Timedelta(days=1), d, d + pd.Timedelta(days=1)])]
                        similar = (window["ratio"] / r).abs().between(0.85, 1.15).any() if not window.empty else False
                        if not similar:
                            per_day.loc[d, "ratio"] = r
                        else:
                            d1 = d + pd.Timedelta(days=1)
                            if d1 in per_day.index and abs(per_day.loc[d1, "ratio"]/r - 1) <= 0.15:
                                per_day = per_day.drop(index=d1)
                                per_day.loc[d, "ratio"] = r
                    per_day = per_day.reset_index()

        e = days.merge(per_day, on="event_day", how="left")
        e["ratio"] = pd.to_numeric(e["ratio"], errors="coerce").fillna(1.0)
        e["F"] = e["ratio"].cumprod()
        F_last = float(e["F"].iloc[-1]) if len(e) else 1.0

        tmp = days.copy()
        # Use to_numpy() to ignore index labels and keep row-wise alignment
        tmp["split_price_factor"]  = (e["F"] / F_last).to_numpy()
        tmp["split_volume_factor"] = (F_last / e["F"]).to_numpy()
        tmp["ticker"] = tkr
        out.append(tmp)

    return pd.concat(out, ignore_index=True)


def _build_daily_prior_base(id_days: pd.DataFrame,
                            use_split_base: bool,
                            F: pd.DataFrame,
                            edges: Optional[pd.DataFrame]) -> pd.DataFrame:
    if edges is None:
        raise RuntimeError("edges must be provided in streaming mode to avoid rescanning the lake.")
    base = id_days.merge(
        edges[["ticker","event_day","last_close"]].rename(columns={"last_close":"close_eod"}),
        on=["ticker","event_day"], how="left"
    )[["id","event_day","ticker","close_eod"]]

    if use_split_base:
        base = base.merge(F[["ticker","event_day","split_price_factor"]],
                          on=["ticker","event_day"], how="left")
        base["spf"]  = base["split_price_factor"].fillna(1.0)
        base["base"] = base["close_eod"] * base["spf"]
        base = base.drop(columns=["split_price_factor"])
    else:
        base["spf"]  = 1.0
        base["base"] = base["close_eod"]

    # 'spf' (split factor in force that day) travels with the base so raw dividend amounts can
    # be expressed in the same split-adjusted units as the base they are divided by.
    base = base[["id","ticker","event_day","base","spf"]].sort_values(["id","ticker","event_day"])
    return base

def _prep_divs_for_stream(div: pd.DataFrame) -> pd.DataFrame:
    d = div.copy()
    ex = "ex_date" if "ex_date" in d.columns else "ex_dividend_date"
    amt = "amount" if "amount" in d.columns else "cash_amount"
    if "ticker" not in d.columns and "T" in d.columns:
        d = d.rename(columns={"T":"ticker"})
    if "composite_figi" not in d.columns:
        d["composite_figi"] = pd.NA
    d = d.rename(columns={ex:"ex_date", amt:"amount"})
    d["ex_date"] = pd.to_datetime(d["ex_date"]).dt.normalize().dt.as_unit("ns")
    d["ticker"] = _norm_ticker(d["ticker"])
    d["event_id"] = _event_ids(d)
    return d[["ex_date","amount","ticker","event_id"]]

def _build_dividend_factors_from_days(id_days: pd.DataFrame, div: pd.DataFrame, base: pd.DataFrame) -> pd.DataFrame:
    d = _prep_divs_for_stream(div)
    d_idx = _EventIndex(d, "ex_date", ["ex_date", "amount"])
    out = []
    cal = id_days[["ticker","id","event_day"]].drop_duplicates().sort_values(["ticker","id","event_day"])

    keys = ["ticker", "id", "event_day"] if "ticker" in base.columns else ["id", "event_day"]
    b = base.merge(id_days[["ticker","id","event_day"]].drop_duplicates(), on=keys, how="left")
    b = b.sort_values(["id","ticker","event_day"]).reset_index(drop=True)
    if "spf" not in b.columns:
        b["spf"] = 1.0
    # Prior-day base per holder AND ticker. A holder whose several tickers trade on the same days (units, warrants
    # and common under one CIK id in the market refdata) must not take another ticker's close as its prior base;
    # keyed on the holder alone, those rows multiplied in the join below and crashed the full-market minute build.
    # A ticker's first day still inherits the holder's previous day under its former ticker (FB -> META), so a
    # rename never breaks the chain, and a base is never carried across companies.
    b["prior_base"] = b.groupby(["id","ticker"])["base"].shift(1)
    first = b["prior_base"].isna().to_numpy()
    if first.any():
        per_day = b.groupby(["id","event_day"], as_index=False)["base"].last()
        per_day["_prev"] = per_day.groupby("id")["base"].shift(1)
        b = b.merge(per_day[["id","event_day","_prev"]], on=["id","event_day"], how="left")
        b.loc[first, "prior_base"] = b.loc[first, "_prev"]
        b = b.drop(columns=["_prev"])

    # prior bases pre-grouped per (ticker, holder): a boolean filter of the whole frame per holder is O(holders x rows),
    # hours on a 22-year full-market minute lake
    prb_cols = ["event_day", "prior_base", "spf"]
    prb_by = {k: g[prb_cols] for k, g in b.groupby(["ticker", "id"], sort=False)}
    prb_empty = b.iloc[0:0][prb_cols]

    for (tkr, gid), g in _tqdm(cal.groupby(["ticker", "id"]), desc="Dividend factors per holder (days)"):
        # IMPORTANT: reset_index here too
        days = g[["event_day"]].copy().reset_index(drop=True)
        prb  = prb_by.get((tkr, gid), prb_empty)
        ev = d_idx.get(gid, tkr)

        if ev.empty:
            tmp = days.copy()
            tmp["tr_price_factor"] = 1.0
        else:
            aligned = pd.merge_asof(
                ev.rename(columns={"ex_date":"event_anchor"}),
                days.rename(columns={"event_day":"event_anchor"}),
                on="event_anchor", direction="forward", allow_exact_matches=True
            ).rename(columns={"event_anchor":"event_day"}).dropna(subset=["event_day"])
            per_day_amt = aligned.groupby("event_day", as_index=False)["amount"].sum()

            T = (days.merge(prb, on="event_day", how="left")
                      .merge(per_day_amt, on="event_day", how="left"))
            # Same math as the batch worker: scale raw cash by the day's split factor, then
            # backward factor = prod of retained fractions for ex-dates AFTER t = G_last / G_t.
            T["spf"] = T["spf"].fillna(1.0)
            T["g"] = 1.0
            m = T["amount"].notna() & T["prior_base"].notna() & (T["prior_base"] > 0)
            amt_adj = T.loc[m, "amount"] * T.loc[m, "spf"]
            T.loc[m, "g"] = (T.loc[m, "prior_base"] - amt_adj) / T.loc[m, "prior_base"]
            T["G"] = T["g"].cumprod()
            G_last = float(T["G"].iloc[-1]) if len(T) else 1.0

            tmp = days.copy()
            tmp["tr_price_factor"] = (G_last / T["G"]).to_numpy()

        tmp["ticker"] = tkr
        out.append(tmp)

    return pd.concat(out, ignore_index=True)


# =============================================================
# Streaming helpers (minute mode, MARKET layout: <root>/<YYYY>/<MM>/<DD>.parquet, all tickers per file)
# =============================================================
ROW_GROUP_SIZE_MARKET = 131_072

def _day_index(df: pd.DataFrame) -> pd.DataFrame:
    """Per-ticker index of a ticker-major minute day file: first/last close, row count and row positions.
    Mirrors polygon_ingest.ingest.day_index (kept local so this script stays standalone)."""
    pos = np.arange(len(df))
    g = pd.DataFrame({"ticker": df["ticker"].to_numpy(), "close": df["close"].to_numpy(), "_pos": pos}).groupby("ticker", sort=True)
    idx = g.agg(first_close=("close", "first"), last_close=("close", "last"), n_rows=("close", "size"),
                row_start=("_pos", "min"), row_end=("_pos", "max")).reset_index()
    return idx

def _index_path(day_file: Path) -> Path:
    return day_file.with_name(day_file.stem + ".idx.parquet")

def _iter_minute_market_files(root: Path, start: Optional[str] = None, end: Optional[str] = None) -> List[Tuple[Path, pd.Timestamp]]:
    """(path, event_day) for every <root>/<YYYY>/<MM>/<DD>.parquet, filtered to [start, end]."""
    root = Path(root); out = []
    s = pd.Timestamp(start) if start else None; e = pd.Timestamp(end) if end else None
    for f in sorted(root.glob("*/*/*.parquet")):
        if f.name.endswith(".idx.parquet"):
            continue
        try:
            day = pd.Timestamp(int(f.parent.parent.name), int(f.parent.name), int(f.stem)).normalize().as_unit("ns")
        except Exception:
            continue
        if (s is not None and day < s) or (e is not None and day > e):
            continue
        out.append((f, day))
    return out

def _read_day_index(path: Path, tickers: Optional[set] = None) -> pd.DataFrame:
    """Per-ticker first/last close for one market minute day file: the .idx.parquet sidecar written by the
    ingester when present, else computed from the file (ticker + close columns; ~0.2 s for 1.4 M rows)."""
    ip = _index_path(Path(path))
    if ip.exists():
        idx = pd.read_parquet(ip)
    else:
        cols = ["ticker", "close"] + (["datetime"] if "datetime" in _pq.ParquetFile(path).schema_arrow.names else [])
        df = pd.read_parquet(path, columns=cols)
        df["ticker"] = _norm_ticker(df["ticker"])
        if "datetime" in df.columns:
            df = df.sort_values(["ticker", "datetime"])
        idx = _day_index(df)
    idx["ticker"] = _norm_ticker(idx["ticker"])
    if tickers is not None:
        idx = idx[idx["ticker"].astype(str).str.casefold().isin({str(t).casefold() for t in tickers})]
    return idx

def _scan_day_edges_market(files: List[Tuple[Path, pd.Timestamp]], tickers: Optional[List[str]] = None,
                           threads: int = 8) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    One read per day file (not per ticker): returns
      days_df (ticker, path, event_day)  - which tickers traded on which day, for _attach_id_days
      edges   (ticker, event_day, first_close, last_close, prev_last, raw_gap) - for split-gap detection and the
              dividend prior-day base, same columns as _scan_day_edges.
    """
    tset = {t.strip().casefold() for t in tickers} if tickers else None
    parts = []
    def one(item):
        f, day = item
        idx = _read_day_index(f, tset)
        return idx.assign(event_day=day, path=str(f))
    with ThreadPoolExecutor(max_workers=max(1, threads)) as ex:
        futs = [ex.submit(one, it) for it in files]
        for fut in _tqdm(as_completed(futs), total=len(futs), desc=f"Scanning market day files (threads x{threads})"):
            parts.append(fut.result())
    all_ = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=["ticker", "first_close", "last_close", "event_day", "path"])
    days_df = all_[["ticker", "path", "event_day"]].sort_values(["ticker", "event_day"]).reset_index(drop=True)
    edges = all_[["ticker", "event_day", "first_close", "last_close"]].sort_values(["ticker", "event_day"]).reset_index(drop=True)
    edges["prev_last"] = edges.groupby("ticker")["last_close"].shift(1)
    edges["raw_gap"] = edges["first_close"] / edges["prev_last"]
    return days_df, edges

def _stream_write_market_one(task: Tuple) -> int:
    """Adjust and write one market-layout minute day file: read once, join that day's factors and holder ids by
    ticker, write the same layout (ticker-major, row groups, .idx.parquet sidecar)."""
    path, day, fg, ids, outdir, materialize, tlist = task
    tpush = sorted({v for t in tlist for v in (t, t.upper())}) if tlist else None
    df = pd.read_parquet(path, filters=[("ticker", "in", tpush)] if tpush else None)
    if "T" in df.columns and "ticker" not in df.columns:
        df = df.rename(columns={"T": "ticker"})
    df["ticker"] = _norm_ticker(df["ticker"])
    if tlist:
        df = df[_wanted_mask(df["ticker"], tlist)]
    if df.empty:
        return 0
    for a, b in (("c", "close"), ("v", "volume"), ("o", "open"), ("h", "high"), ("l", "low")):
        if b not in df.columns and a in df.columns:
            df = df.rename(columns={a: b})
    df["datetime"] = _to_naive_utc(df["datetime"]) if "datetime" in df.columns else pd.Timestamp(day)
    df = df.merge(fg, on="ticker", how="left") if fg is not None else df.assign(split_price_factor=1.0, split_volume_factor=1.0, tr_price_factor=1.0)
    for c in ("split_price_factor", "split_volume_factor", "tr_price_factor"):
        df[c] = df[c].fillna(1.0)
    df = df.merge(ids, on="ticker", how="left") if ids is not None else df.assign(id=pd.NA)
    df["id"] = df["id"].where(df["id"].notna(), "NOFIGI__" + df["ticker"])
    sp, sv, tr = df["split_price_factor"], df["split_volume_factor"], df["tr_price_factor"]
    df["close_split"] = df["close"] * sp
    df["volume_split"] = df["volume"] * sv
    if materialize == "ohlc":
        for col in ("open", "high", "low"):
            if col in df.columns:
                df[f"{col}_split"] = df[col] * sp
    df["close_tr"] = df["close_split"] * tr
    if materialize == "ohlc":
        for col in ("open_split", "high_split", "low_split"):
            if col in df.columns:
                df[col.replace("_split", "_tr")] = df[col] * tr
    df = df.sort_values(["ticker", "datetime"]).reset_index(drop=True)
    cols = _select_columns_to_write(df, materialize)
    d = pd.Timestamp(day)
    outpath = Path(outdir) / f"{d.year:04d}" / f"{d.month:02d}" / f"{d.day:02d}.parquet"
    outpath.parent.mkdir(parents=True, exist_ok=True)
    tmp = outpath.with_suffix(".parquet.inprogress")
    _pq.write_table(pa.Table.from_pandas(df[cols], preserve_index=False), tmp, compression="zstd", row_group_size=ROW_GROUP_SIZE_MARKET)
    tmp.replace(outpath)
    _day_index(df).to_parquet(_index_path(outpath), index=False)
    return len(df)

def _stream_write_minutes_market(id_days: pd.DataFrame, F: pd.DataFrame, G: pd.DataFrame, outdir: Path, write_workers: int,
                                 materialize: str, tickers: Optional[List[str]] = None, debug_dump: Optional[Path] = None) -> None:
    """Per day file (one read each, not one per ticker): write the adjusted day file in the same market layout.
    write_workers > 1 runs the day files in that many processes; each task carries just its day's factor and
    holder-id slices, so a full-market factor table is never copied into every worker."""
    FG = F.merge(G, on=["ticker", "event_day"], how="outer")
    for c in ("split_price_factor", "split_volume_factor", "tr_price_factor"):
        FG[c] = FG[c].fillna(1.0)
    if debug_dump is not None:
        debug_dump.mkdir(parents=True, exist_ok=True)
        FG.to_csv(debug_dump / "_factormap.csv", index=False)
    fg_by_day = {d: g[["ticker", "split_price_factor", "split_volume_factor", "tr_price_factor"]] for d, g in FG.groupby("event_day")}
    id_by_day = {d: g[["ticker", "id"]].drop_duplicates("ticker") for d, g in id_days.groupby("event_day")}
    files = id_days[["path", "event_day"]].drop_duplicates("path").itertuples(index=False)
    tlist = sorted({t.strip() for t in tickers}) if tickers else None

    tasks = [(it.path, it.event_day, fg_by_day.get(it.event_day), id_by_day.get(it.event_day), str(outdir), materialize, tlist)
             for it in files]
    if write_workers <= 1:
        for t in _tqdm(tasks, desc="Writing minute lake (market layout)"):
            _stream_write_market_one(t)
    else:
        with ProcessPoolExecutor(max_workers=write_workers, initializer=_worker_init,
                                 initargs=(_arrow_threads_per_worker(write_workers),)) as ex:
            for _ in _tqdm(ex.map(_stream_write_market_one, tasks), total=len(tasks),
                           desc=f"Writing minute lake (market layout x{write_workers} processes)"):
                pass

def adjust_minute_market(prices: Path, sm: pd.DataFrame, spl: pd.DataFrame, div: pd.DataFrame, outdir: Path, *,
                         tickers: Optional[List[str]] = None, start: Optional[str] = None, end: Optional[str] = None,
                         adjust: str = "both", materialize: str = "ohlc", write_workers: int = 4, read_workers: int = 8,
                         detect_gaps: bool = True, debug_dump: Optional[Path] = None,
                         gap_days: int = RECYCLE_GAP_DAYS) -> Dict[str, Any]:
    """Streaming adjustment of a MARKET-layout minute lake (all tickers per day file) into the same layout.
    `spl`/`div` must already carry holder ids (see _assign_event_ids)."""
    files = _iter_minute_market_files(prices, start, end)
    if not files:
        raise SystemExit(f"No <YYYY>/<MM>/<DD>.parquet minute day files under {prices} for the selection.")
    days_df, edges = _scan_day_edges_market(files, tickers=tickers, threads=read_workers)
    if days_df.empty:
        raise SystemExit("No rows for the requested tickers in the selected day files.")
    segs = _recycled_segments(days_df["ticker"].to_numpy(), days_df["event_day"], sm, gap_days)
    if len(segs):
        spl = _apply_event_segments(spl, segs, ["execution_date"])
        div = _apply_event_segments(div, segs, ["ex_date", "ex_dividend_date"])
    id_days = _attach_id_days(days_df, sm, segments=segs)
    if debug_dump is not None:
        debug_dump.mkdir(parents=True, exist_ok=True)
        id_days.to_csv(debug_dump / "_id_days.csv", index=False); edges.to_csv(debug_dump / "_edges.csv", index=False)
    F = (_build_split_factors_from_days(id_days, spl, edges=edges, detect_gaps=detect_gaps) if adjust in ("splits", "both")
         else id_days[["ticker", "event_day"]].assign(split_price_factor=1.0, split_volume_factor=1.0))
    use_split_base = adjust == "both"
    if adjust in ("dividends", "both"):
        base = _build_daily_prior_base(id_days, use_split_base=use_split_base, F=F, edges=edges)
        G = _build_dividend_factors_from_days(id_days, div, base)
    else:
        G = id_days[["ticker", "event_day"]].assign(tr_price_factor=1.0)
    if debug_dump is not None:
        F.to_csv(debug_dump / "_split_F.csv", index=False); G.to_csv(debug_dump / "_div_G.csv", index=False)
    outdir = Path(outdir); outdir.mkdir(parents=True, exist_ok=True)
    _stream_write_minutes_market(id_days, F, G, outdir, write_workers, materialize, tickers=tickers, debug_dump=debug_dump)
    return {"files": len(files), "ticker_days": len(id_days), "tickers": int(id_days["ticker"].nunique())}


def _stream_write_one(task: Tuple) -> int:
    """Adjust and write one <TICKER>/<YYYY>/<MM>/<DD>.parquet minute day file (ticker layout) with the day's factors."""
    tkr, path, day, gid, sp, sv, tr, outdir, materialize = task
    day = pd.Timestamp(day)
    df = pd.read_parquet(path)
    # Normalize & filter to the target ticker
    if "ticker" in df.columns or "T" in df.columns:
        if "T" in df.columns and "ticker" not in df.columns:
            df = df.rename(columns={"T":"ticker"})
        df["ticker"] = _norm_ticker(df["ticker"])
        df = df[df["ticker"] == tkr]
    else:
        df["ticker"] = tkr
    if df.empty:
        return 0
    if "close" not in df.columns and "c" in df.columns:
        df = df.rename(columns={"c":"close"})
    if "volume" not in df.columns and "v" in df.columns:
        df = df.rename(columns={"v":"volume"})
    if "datetime" in df.columns:
        df["datetime"] = _to_naive_utc(df["datetime"])   # tz-naive UTC, same as the batch path
    else:
        df["datetime"] = day
    df["id"] = gid
    # split-adjust
    df["close_split"]  = df["close"]  * sp
    df["volume_split"] = df["volume"] * sv
    if materialize == "ohlc":
        for col in ("open","high","low"):
            if col in df.columns:
                df[f"{col}_split"] = df[col] * sp
    # TR (also carry split_price_factor so the ticker layout writes the same columns as the batch/market paths)
    df["split_price_factor"] = sp
    df["tr_price_factor"] = tr
    df["close_tr"] = df["close_split"] * tr
    if materialize == "ohlc":
        for col in ("open_split","high_split","low_split"):
            if col in df.columns:
                df[col.replace("_split","_tr")] = df[col] * tr
    outpath = Path(outdir) / tkr / f"{day.year:04d}" / f"{day.month:02d}" / f"{day.day:02d}.parquet"
    outpath.parent.mkdir(parents=True, exist_ok=True)
    cols = _select_columns_to_write(df, materialize)
    df[cols].to_parquet(outpath, index=False)
    return len(df)

def _stream_write_minutes(id_days: pd.DataFrame, F: pd.DataFrame, G: pd.DataFrame, outdir: Path, write_workers: int, materialize: str, debug_dump: Optional[Path]=None):
    FG = F.merge(G, on=["ticker","event_day"], how="outer")
    FG["split_price_factor"]  = FG["split_price_factor"].fillna(1.0)
    FG["split_volume_factor"] = FG["split_volume_factor"].fillna(1.0)
    FG["tr_price_factor"]     = FG["tr_price_factor"].fillna(1.0)

    # Robust string-keyed factor map
    FG["day_key"] = pd.to_datetime(FG["event_day"]).dt.strftime("%Y-%m-%d")
    factormap: Dict[Tuple[str, str], Tuple[float, float, float]] = {
        (r.ticker, r.day_key): (
            float(r.split_price_factor),
            float(r.split_volume_factor),
            float(r.tr_price_factor),
        )
        for r in FG.itertuples()
    }

    if debug_dump is not None:
        debug_dump.mkdir(parents=True, exist_ok=True)
        FG[["ticker","event_day","day_key","split_price_factor","split_volume_factor","tr_price_factor"]].to_csv(debug_dump/"_factormap.csv", index=False)

    # Exact-day lookup only. The former +-1 day fallback (for lakes partitioned on the UTC date) misfired
    # whenever a day's factors were legitimately all 1.0 - e.g. the day after an ex-date - and copied the
    # previous day's total-return factor onto it. Lake files and factors are both keyed on the ET trading
    # date now, so the fallback is unnecessary.
    tasks = []
    for r in id_days.itertuples():
        day = pd.Timestamp(r.event_day)
        sp, sv, tr = factormap.get((r.ticker, day.date().isoformat()), (1.0, 1.0, 1.0))
        tasks.append((r.ticker, str(r.path), day, r.id, sp, sv, tr, str(outdir), materialize))

    if write_workers <= 1:
        for t in _tqdm(tasks, desc="Writing minute lake (stream)"):
            _stream_write_one(t)
    else:
        # processes, not threads: each task is a small read-multiply-write in pandas, which holds the GIL
        with ProcessPoolExecutor(max_workers=write_workers, initializer=_worker_init,
                                 initargs=(_arrow_threads_per_worker(write_workers),)) as ex:
            for _ in _tqdm(ex.map(_stream_write_one, tasks, chunksize=128), total=len(tasks),
                           desc=f"Writing minute lake (stream x{write_workers} processes)"):
                pass


# =============================================================
# Sharded batch build: each process runs read -> holder ids -> factors -> write on its own slice of holders
# =============================================================

def _adjust_frame(px: pd.DataFrame, sm: pd.DataFrame, spl: pd.DataFrame, div: pd.DataFrame, adjust: str,
                  workers: int = 1, gap_days: int = RECYCLE_GAP_DAYS) -> Tuple[pd.DataFrame, dict, bool]:
    """Batch adjustment of a price frame: holder ids, split factors, dividend factors, renormalisation.
    Returns (adjusted frame, per-id stats, use_split_base)."""
    segs = _recycled_segments(_norm_ticker(px["ticker"]).to_numpy(), _trading_day(_to_naive_utc(px["datetime"])),
                              sm, gap_days)
    if len(segs):
        spl = _apply_event_segments(spl, segs, ["execution_date"])
        div = _apply_event_segments(div, segs, ["ex_date", "ex_dividend_date"])
    px_id = _attach_id(px, sm, segments=segs)
    px_id["close_split"]  = px_id["close"]
    px_id["volume_split"] = px_id["volume"]
    stats: dict = {}
    if adjust in ("splits", "both"):
        F      = _build_split_factors(px_id, spl, stats=stats, workers=workers)
        px_spl = _apply_splits(px_id, F)
    else:
        px_spl = px_id.copy()
    if adjust in ("dividends", "both"):
        use_split_base = (adjust == "both")
        G     = _build_dividend_factors(px_spl, div, use_split_base=use_split_base, stats=stats, workers=workers)
        px_tr = _apply_dividends(px_spl, G, use_split_base=use_split_base)
        px_tr = _renormalize_tr_to_one(px_tr, use_split_base=use_split_base)
    else:
        px_tr = px_spl.copy()
        px_tr["tr_price_factor"] = 1.0
        px_tr["close_tr"] = px_tr["close_split"]
        use_split_base = False
    return px_tr, stats, use_split_base


def _holder_groups(tickers: List[str], sm: pd.DataFrame) -> List[List[str]]:
    """Tickers that share a holder id (a renamed company: FB -> META) form one group, so a single shard sees every
    row of that id: its factors chain across the rename and anchor on the id's last bar. Sorted groups of sorted tickers."""
    parent = {t: t for t in tickers}
    def find(x: str) -> str:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x
    smn = _normalize_sm(sm) if len(sm) else pd.DataFrame(columns=["ticker", "holder_id"])
    smn = smn[smn["ticker"].isin(parent)]
    for _, g in smn.groupby("holder_id"):
        ts = sorted(set(g["ticker"]))
        for t in ts[1:]:
            ra, rb = find(ts[0]), find(t)
            if ra != rb:
                parent[rb] = ra
    groups: Dict[str, List[str]] = {}
    for t in tickers:
        groups.setdefault(find(t), []).append(t)
    return sorted((sorted(g) for g in groups.values()), key=lambda g: g[0])


def _plan_shards(tickers: List[str], sm: pd.DataFrame, weights: Dict[str, int], n_shards: int) -> List[List[str]]:
    """Cut the tickers into at most n_shards alphabetical slices of about equal weight without separating the
    tickers of one holder. Alphabetical slices matter for the market layout: files are ticker-major with row-group
    statistics, so a slice's `ticker in [...]` read filter skips whole row groups instead of decoding every file."""
    groups = _holder_groups(sorted(set(tickers)), sm)
    if not groups:
        return []
    n_shards = max(1, min(int(n_shards), len(groups)))
    wt = lambda g: sum(max(1, int(weights.get(t, 1))) for t in g)
    total = sum(wt(g) for g in groups)
    shards: List[List[str]] = [[]]
    acc = 0
    for g in groups:
        w = wt(g)
        if shards[-1] and len(shards) < n_shards and acc + w > total * len(shards) / n_shards:
            shards.append([])
        shards[-1].extend(g)
        acc += w
    return [sh for sh in shards if sh]


def _ticker_weights(prices: Path, layout: str, tickers: Optional[List[str]]) -> Dict[str, int]:
    """Tickers present in the lake (restricted to `tickers` when given) with a cost weight each: files per
    <TICKER>/ directory (ticker layout) or rows per ticker from the parquet ticker column (market layout)."""
    prices = Path(prices)
    want = {t.strip().casefold() for t in tickers} if tickers else None
    w: Dict[str, int] = {}
    if layout == "ticker":
        for d in prices.iterdir():
            t = d.name.strip()
            if d.is_dir() and (want is None or t.casefold() in want):
                w[t] = sum(1 for _ in d.rglob("*.parquet"))
        return {t: n for t, n in w.items() if n}
    import pyarrow.compute as pc
    files = [f for f in sorted(prices.rglob("*.parquet")) if not f.name.endswith(".idx.parquet")]
    def one(f: Path) -> Dict[str, int]:
        vc = pc.value_counts(_pq.read_table(f, columns=["ticker"]).column("ticker"))
        return {str(r["values"]).strip(): int(r["counts"]) for r in vc.to_pylist()}
    with ThreadPoolExecutor(max_workers=8) as ex:
        for part in ex.map(one, files):
            for t, n in part.items():
                if want is None or t.casefold() in want:
                    w[t] = w.get(t, 0) + n
    return w


def _shard_worker(p: Dict[str, Any]) -> Dict[str, Any]:
    """One shard: read its tickers, adjust, write. Ticker layout -> final <TICKER>/... files; market layout ->
    per-period part files under the shard's parts directory (stitched by _merge_market_parts)."""
    try:
        px = _read_prices(Path(p["prices"]), tickers=p["tickers"], start=p["start"], end=p["end"])
    except SystemExit:
        px = pd.DataFrame()
    if px.empty:                                   # e.g. a --start/--end window with none of this shard's rows
        return {"shard": p["shard"], "rows": 0, "summary": []}
    px_tr, stats, use_split_base = _adjust_frame(px, p["sm"], p["spl"], p["div"], p["adjust"], workers=1,
                                                 gap_days=p.get("gap_days", RECYCLE_GAP_DAYS))
    _write_partitioned_lake(px_tr, Path(p["outdir"]), p["granularity"], p["write_threads"], p["materialize"], layout=p["layout"])
    return {"shard": p["shard"], "rows": int(len(px_tr)), "summary": _summary_rows(stats, px_tr, p["adjust"], use_split_base)}


def _merge_one_period(task: Tuple[List[str], str]) -> int:
    parts, outpath = task
    df = pd.concat([pd.read_parquet(f) for f in parts], ignore_index=True)
    df = df.sort_values(["ticker", "datetime"], kind="stable").reset_index(drop=True)
    _write_one_parquet(Path(outpath), df)
    return len(df)


def _merge_market_parts(parts_root: Path, outdir: Path, workers: int) -> int:
    """Market layout: each shard wrote <parts_root>/<shard>/<YYYY>/<MM>[/<DD>].parquet for its tickers; stitch each
    period's parts into <outdir>/<YYYY>/<MM>[/<DD>].parquet (ticker-major, like the single-process writer) and drop the parts."""
    shards = sorted(d for d in parts_root.iterdir() if d.is_dir())
    rels = sorted(set(str(f.relative_to(sh)) for sh in shards for f in sh.rglob("*.parquet")))
    tasks = [([str(sh / rel) for sh in shards if (sh / rel).exists()], str(outdir / rel)) for rel in rels]
    n = 0
    with ProcessPoolExecutor(max_workers=max(1, int(workers)), initializer=_worker_init,
                             initargs=(_arrow_threads_per_worker(workers),)) as ex:
        for k in _tqdm(ex.map(_merge_one_period, tasks), total=len(tasks), desc=f"Merging period files (x{workers} processes)"):
            n += k
    shutil.rmtree(parts_root, ignore_errors=True)
    return n


def _run_batch_sharded(args, sm: pd.DataFrame, spl: pd.DataFrame, div: pd.DataFrame, layout: str,
                       tickers: Optional[List[str]]) -> Path:
    """Batch build over `args.workers` processes. Each process owns a slice of holders end to end (read, holder ids,
    split and dividend factors, renormalisation, write), so reading and writing scale with cores as well as the
    per-id maths. Output is identical to the single-process path (--workers 1)."""
    weights = _ticker_weights(args.prices, layout, tickers)
    if not weights:
        raise SystemExit(f"No tickers found under {args.prices}" + (" for the requested list" if tickers else ""))
    shards = _plan_shards(sorted(weights), sm, weights, args.workers)
    parts_root = args.outdir / "_parts"
    if layout == "market":
        shutil.rmtree(parts_root, ignore_errors=True)
    write_threads = max(1, int(args.write_workers) // len(shards))
    def _slice(events: pd.DataFrame, tset: set) -> pd.DataFrame:   # only this shard's events (the per-id workers scan the table)
        return events[events["ticker"].isin(tset)] if "ticker" in events.columns else events
    payloads = []
    for k, tk in enumerate(shards):
        tset = set(tk)
        payloads.append({"shard": k, "tickers": tk, "prices": str(args.prices), "start": args.start, "end": args.end,
                         "sm": sm, "spl": _slice(spl, tset), "div": _slice(div, tset),
                         "adjust": args.adjust, "materialize": args.materialize, "granularity": args.granularity,
                         "gap_days": args.recycle_gap_days,
                         "layout": layout, "write_threads": write_threads,
                         "outdir": str(parts_root / f"shard{k:03d}") if layout == "market" else str(args.outdir)})
    print(f"Sharded build: {len(weights)} tickers in {len(shards)} shards ({layout} layout, {len(shards)} processes)")
    results = []
    with ProcessPoolExecutor(max_workers=len(shards), initializer=_worker_init,
                             initargs=(_arrow_threads_per_worker(len(shards)),)) as ex:
        futs = [ex.submit(_shard_worker, p) for p in payloads]
        for fut in _tqdm(as_completed(futs), total=len(futs), desc=f"Shards (x{len(shards)} processes)"):
            results.append(fut.result())
    if layout == "market":
        _merge_market_parts(parts_root, args.outdir, args.workers)
    print(f"Adjusted rows: {sum(r['rows'] for r in results):,}")
    return _write_summary_rows([row for r in results for row in r["summary"]], args.outdir)


# =============================================================
# Defaults
# =============================================================

def _default_workers() -> int:
    try:
        return max(1, (os.cpu_count() or 2) - 1)
    except Exception:
        return 1

def _default_write_workers() -> int:
    try:
        return min(8, max(1, (os.cpu_count() or 2)))
    except Exception:
        return 1


# =============================================================
# Main
# =============================================================

def main():
    ap = argparse.ArgumentParser(
        description="Build split/dividend adjustments and write a partitioned parquet lake with 'datetime'."
    )
    ap.add_argument("--prices", type=Path, required=True,
                    help="CSV/Parquet FILE or DIRECTORY (recursively read *.parquet)")
    ap.add_argument("--refdir", type=Path, required=True,
                    help="Directory with security_master.parquet, stock_splits.parquet, cash_dividends.parquet")
    ap.add_argument("--tickers", type=Path, default=None,
                    help="Optional JSON list of tickers to prefilter files and rows")
    ap.add_argument("--start", type=str, default=None, help="Optional start date YYYY-MM-DD")
    ap.add_argument("--end", type=str, default=None, help="Optional end date YYYY-MM-DD")
    ap.add_argument("--granularity", choices=["day", "minute"], required=True,
                    help="Granularity of the parquet lake layout to write")
    ap.add_argument("--layout", choices=["auto", "ticker", "market"], default="auto",
                    help="Lake layout of --prices and --outdir: ticker (<root>/<TICKER>/<YYYY>/...), market "
                         "(<root>/<YYYY>/<MM>[/<DD>].parquet, all tickers). auto = detect from --prices.")
    ap.add_argument("--outdir", type=Path, required=True,
                    help="Output directory for adjusted parquet lake")
    ap.add_argument("--workers", type=int, default=_default_workers(),
                    help="Day batch mode: processes, each building one slice of holders end to end (read, factors, "
                         "write). Set 1 for the single-process reference path.")
    ap.add_argument("--write-workers", type=int, default=_default_write_workers(),
                    help="Minute streaming: processes writing day files. Day batch: writer threads, shared out over "
                         "the --workers processes. Set 1 to disable.")
    ap.add_argument("--recycle-gap-days", type=int, default=RECYCLE_GAP_DAYS,
                    help="Split a ticker's history where it stopped trading this many days or more, and give the "
                         "earlier segments their own holder id, so a recycled symbol's old bars are not credited "
                         "to today's company (ARM traded from 2003; Arm Holdings listed in 2023). Applies only to "
                         "tickers whose security-master start is unconfirmed. 0 disables.")
    ap.add_argument("--adjust", choices=["splits", "dividends", "both"], default="both",
                    help="Which adjustments to apply (default: both)")
    ap.add_argument("--materialize", choices=["minimal","close","ohlc"], default="minimal",
                    help="Columns to persist.")
    ap.add_argument("--verbose", action="store_true",
                    help="Print per-ticker summary of aligned split/dividend events")
    ap.add_argument("--manifest-src", type=Path, default=None,
                    help="Optional explicit path to manifest.json to copy to OUTDIR")
    ap.add_argument("--no-copy-manifest", action="store_true",
                    help="Skip copying manifest.json to OUTDIR")

    # Streaming (OOM-safe) for minute lakes
    ap.add_argument("--minute-stream", action="store_true",
                    help="Use streaming minute mode (<TICKER>/<YYYY>/<MM>/<DD>.parquet files one-by-one).")
    ap.add_argument("--stream-read-workers", type=int, default=min(8, _default_write_workers()),
                    help="Threads for minute day-edge scan & base building.")
    ap.add_argument("--detect-split-gaps", action="store_true",
                    help="Minute streaming: infer splits from overnight price gaps for tickers with no splits-table entry. "
                         "A fallback for lakes without market-wide splits refdata; on the full market it fires on warrants and "
                         "penny stocks that gap 2x overnight, so it is OFF by default (Step 4 bulk refdata covers every ticker).")
    ap.add_argument("--no-detect-split-gaps", action="store_true", help="(deprecated no-op: detection is off unless --detect-split-gaps)")

    # NEW: debug dump
    ap.add_argument("--debug-dump", type=Path, default=None,
                    help="Directory to dump CSVs: _id_days, _edges, _split_F, _div_G, _factormap.")

    args = ap.parse_args()

    tickers = None
    if args.tickers is not None:
        try:
            tickers = sorted({t.strip() for t in json.loads(Path(args.tickers).read_text())})
        except Exception:
            tickers = sorted({t.strip() for t in Path(args.tickers).read_text().splitlines() if t.strip()})

    # Load reference tables
    sm  = pd.read_parquet(args.refdir / "security_master.parquet")
    spl = pd.read_parquet(args.refdir / "stock_splits.parquet")
    div = pd.read_parquet(args.refdir / "cash_dividends.parquet")
    # Key corporate actions by the company holding the ticker on the event date, exactly like price rows.
    spl = _assign_event_ids(spl, sm, ["execution_date"])
    div = _assign_event_ids(div, sm, ["ex_date", "ex_dividend_date"])

    layout = args.layout if args.layout != "auto" else _detect_layout(args.prices)

    # Streaming path for huge MINUTE lakes
    if args.granularity == "minute" and args.minute_stream:
        if layout == "market":
            info = adjust_minute_market(args.prices, sm, spl, div, args.outdir, tickers=tickers, start=args.start, end=args.end,
                                        adjust=args.adjust, materialize=args.materialize, write_workers=args.write_workers,
                                        read_workers=args.stream_read_workers, detect_gaps=bool(args.detect_split_gaps),
                                        debug_dump=args.debug_dump, gap_days=args.recycle_gap_days)
            print(f"\nDone (streaming, market layout): {info['files']} day files, {info['tickers']} tickers, "
                  f"{info['ticker_days']} ticker-days -> {args.outdir.resolve()}")
            _copy_manifest(args.prices, args.outdir, args.manifest_src, skip=args.no_copy_manifest)
            return
        files = _iter_minute_day_files(args.prices, tickers)
        if args.start or args.end:
            s = pd.to_datetime(args.start) if args.start else None
            e = pd.to_datetime(args.end) if args.end else None
            files = [(t,p,d) for (t,p,d) in files if (s is None or d>=s) and (e is None or d<=e)]
        if not files:
            raise SystemExit("No minute day-files found under --prices for the selection.")
        days_df = pd.DataFrame(files, columns=["ticker","path","event_day"])
        segs = _recycled_segments(days_df["ticker"].to_numpy(), days_df["event_day"], sm, args.recycle_gap_days)
        if len(segs):
            spl = _apply_event_segments(spl, segs, ["execution_date"])
            div = _apply_event_segments(div, segs, ["ex_date", "ex_dividend_date"])
        id_days = _attach_id_days(days_df, sm, segments=segs)

        if args.debug_dump is not None:
            args.debug_dump.mkdir(parents=True, exist_ok=True)
            id_days.to_csv(args.debug_dump/"_id_days.csv", index=False)

        # Pre-scan minute files for first/last closes (split gap & TR base)
        detect_gaps = bool(args.detect_split_gaps)
        edges = _scan_day_edges(days_df, threads=args.stream_read_workers)
        if args.debug_dump is not None:
            edges.to_csv(args.debug_dump/"_edges.csv", index=False)

        # Ticker-day split factors
        F = _build_split_factors_from_days(id_days, spl, edges=edges, detect_gaps=detect_gaps) \
            if args.adjust in ("splits","both") else id_days[["ticker","event_day"]].assign(split_price_factor=1.0, split_volume_factor=1.0)
        if args.debug_dump is not None:
            F.to_csv(args.debug_dump/"_split_F.csv", index=False)

        # Dividend factors (prior-day base)
        use_split_base = args.adjust == "both"
        if args.adjust in ("dividends","both"):
            base = _build_daily_prior_base(id_days, use_split_base=use_split_base, F=F, edges=edges)
            G = _build_dividend_factors_from_days(id_days, div, base)
        else:
            G = id_days[["ticker","event_day"]].assign(tr_price_factor=1.0)
        if args.debug_dump is not None:
            G.to_csv(args.debug_dump/"_div_G.csv", index=False)

        # Write per day-file
        args.outdir.mkdir(parents=True, exist_ok=True)
        _stream_write_minutes(id_days, F, G, args.outdir, args.write_workers, args.materialize, debug_dump=args.debug_dump)

        print("\nDone (streaming). Wrote adjusted parquet lake to", args.outdir.resolve())
        _copy_manifest(args.prices, args.outdir, args.manifest_src, skip=args.no_copy_manifest)
        return

    # ===== Batch path (OK for day lakes) =====

    args.outdir.mkdir(parents=True, exist_ok=True)
    if args.workers > 1:
        csv_path = _run_batch_sharded(args, sm, spl, div, layout, tickers)
    else:
        px = _read_prices(args.prices, tickers=tickers, start=args.start, end=args.end)
        px_tr, stats, use_split_base = _adjust_frame(px, sm, spl, div, args.adjust, workers=1,
                                                     gap_days=args.recycle_gap_days)
        _write_partitioned_lake(px_tr, args.outdir, args.granularity, args.write_workers, args.materialize, layout=layout)
        csv_path = _write_summary_csv(stats, args.outdir, px_tr, args.adjust, use_split_base)

    if args.verbose:
        summary = pd.read_csv(csv_path, parse_dates=[
            "last_split_raw_date", "last_split_aligned_day",
            "last_dividend_raw_date", "last_dividend_aligned_day",
            "last_datetime"
        ])
        _print_aligned_summary(summary)

    _copy_manifest(args.prices, args.outdir, args.manifest_src, skip=args.no_copy_manifest)

    print(f"\nSummary CSV written to: {csv_path}")
    print("Done. Wrote adjusted parquet lake to", args.outdir.resolve())
    print(f"Mode --adjust={args.adjust} | Workers={args.workers} | Write-Workers={args.write_workers} | Granularity={args.granularity} | Materialize={args.materialize}")


if __name__ == "__main__":
    main()
