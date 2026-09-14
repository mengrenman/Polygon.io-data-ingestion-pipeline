# polygon_pullers.py
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from typing import Iterable, List, Optional, Dict, Any, Sequence

import pandas as pd
from tqdm import tqdm

from polygon import RESTClient
from polygon.exceptions import BadResponse


def load_api_key(api_key: Optional[str] = None, api_key_file: Optional[str | Path] = None) -> str:
    """
    Resolve API key (preferred → fallback):
      1) explicit --api-key argument
      2) environment variable POLYGON_API_KEY
      3) explicit --api-key-file IF it exists (otherwise ignore)
    """
    if api_key:
        return api_key

    env_key = os.getenv("POLYGON_API_KEY")
    if env_key:
        return env_key

    if api_key_file:
        p = Path(api_key_file)
        if p.exists():
            txt = p.read_text().strip().splitlines()
            if txt:
                return txt[0].strip()
            raise RuntimeError(f"API key file '{p}' is empty.")

    raise RuntimeError(
        "Polygon.io API key not found. Set POLYGON_API_KEY in your environment (or .env), "
        "or pass --api-key / --api-key-file."
    )



def _client(api_key: str) -> RESTClient:
    return RESTClient(api_key)


def _to_upper_list(v: Iterable[str]) -> List[str]:
    return [str(x).strip().upper() for x in v if str(x).strip()]


# Optional pacing between REST calls for rate-limited plans (Polygon Basic allows 5 requests/minute
# -> POLYGON_MIN_INTERVAL_SEC=12.5). Applies to the first request of each call; paginated iterators
# fetch further pages lazily outside this wrapper.
_MIN_INTERVAL_SEC = float(os.getenv("POLYGON_MIN_INTERVAL_SEC", "0") or 0)
# A 429 means the per-minute window is exhausted; sub-second backoffs just burn the retries.
_RATE_LIMIT_MIN_SLEEP_SEC = 12.0
_last_call_ts = 0.0


def _throttle() -> None:
    global _last_call_ts
    if _MIN_INTERVAL_SEC > 0:
        wait = _last_call_ts + _MIN_INTERVAL_SEC - time.monotonic()
        if wait > 0:
            time.sleep(wait)
    _last_call_ts = time.monotonic()


def _is_rate_limited(msg: str) -> bool:
    return any(s in msg for s in ("429", "Too Many Requests", "rate limit"))


def _backoff(i: int, delay: float, msg: str) -> float:
    base = delay * (2 ** i)
    return max(base, _RATE_LIMIT_MIN_SLEEP_SEC) if _is_rate_limited(msg) else base


def _retrying_call(fn, *args, _retries: int = 5, _delay: float = 0.5, **kwargs):
    """
    Retry wrapper for Polygon REST calls:
    - Hard-fails on 'Ticker not found' (caller decides what to do).
    - Rate limit (429; urllib3 surfaces exhausted client retries as MaxRetryError
      'too many 429 error responses'): sleeps >= _RATE_LIMIT_MIN_SLEEP_SEC per attempt.
    - 5xx / network errors: exponential backoff.
    """
    for i in range(_retries + 1):
        _throttle()
        try:
            return fn(*args, **kwargs)
        except BadResponse as e:
            msg = str(e)
            if "Ticker not found" in msg or '"status":"NOT_FOUND"' in msg:
                raise
            transient = _is_rate_limited(msg) or any(s in msg for s in ("Internal Server Error", "503", "502"))
            if transient and i < _retries:
                time.sleep(_backoff(i, _delay, msg))
                continue
            raise
        except Exception as e:
            # Non-BadResponse: urllib3 MaxRetryError (incl. exhausted 429 retries), network errors, ...
            if i < _retries:
                time.sleep(_backoff(i, _delay, str(e)))
                continue
            raise


def _write_failures(path: str | Path, failed: List[tuple], *, what: str) -> None:
    """Record tickers whose pull failed after retries ('<ticker>\t<reason>' per line) and warn on stderr."""
    path = Path(path)
    if failed:
        path.write_text("\n".join(f"{t}\t{r}" for t, r in failed) + "\n")
        print(f"[warn] {what}: {len(failed)} ticker(s) failed after retries -> {path}", file=sys.stderr)
        for t, r in failed:
            print(f"[warn]   {t}: {r[:200]}", file=sys.stderr)
    elif path.exists():
        path.unlink()


def holder_id(figi, cik, ticker) -> str:
    """
    Stable id for the *company* behind a ticker: composite FIGI, else 'CIK__<cik>', else 'NOFIGI__<TICKER>'.
    Polygon returns no FIGI for some delisted holders (the pre-2009 General Motors Corp has only a CIK).
    Must agree with legacy_scripts/factor_builder._holder_id.
    """
    if isinstance(figi, str) and figi.strip():
        return figi.strip()
    if cik is not None:
        c = str(cik).strip()
        if c and c.lower() not in ("nan", "none", "<na>"):
            return "CIK__" + c
    return "NOFIGI__" + str(ticker).strip().upper()


def _ts(x) -> pd.Timestamp:
    """Date-like -> tz-naive (UTC) midnight Timestamp, NaT if missing."""
    if x is None or (isinstance(x, float) and pd.isna(x)) or x == "":
        return pd.NaT
    t = pd.to_datetime(x, utc=True, errors="coerce")
    return pd.NaT if pd.isna(t) else t.tz_convert(None).normalize()


SM_COLUMNS = ["ticker", "holder_id", "holder_source", "name", "active", "type",
              "composite_figi", "share_class_figi", "cik", "locale", "currency_name", "primary_exchange", "market",
              "list_date", "delisted_utc", "effective_start", "effective_end", "anchor_date", "updated",
              "start_confirmed", "end_confirmed"]
# Polygon's stock reference history starts here: a holder whose FIRST ticker event is on this date was simply
# already trading (NVDA, AAPL, MSFT all show it), so such a start is NOT a real symbol adoption.
HISTORY_START = pd.Timestamp("2003-09-10")


def _details_row(d, ticker: str, *, source: str, anchor_date=None) -> Dict[str, Any]:
    """One security-master row from a TickerDetails object. The holder window is [list_date, delisted_utc]
    (NaT = open/unknown); anchor_date is a date the holder is known to have held the ticker (probe lookups)."""
    figi = getattr(d, "composite_figi", None)
    cik = getattr(d, "cik", None)
    return {
        "ticker": str(getattr(d, "ticker", None) or ticker).strip().upper(),
        "holder_id": holder_id(figi, cik, ticker),
        "holder_source": source,
        "name": getattr(d, "name", None),
        "active": getattr(d, "active", None),
        "type": getattr(d, "type", None),
        "composite_figi": figi,
        "share_class_figi": getattr(d, "share_class_figi", None),
        "cik": cik,
        "locale": getattr(d, "locale", None),
        "currency_name": getattr(d, "currency_name", None),
        "primary_exchange": getattr(d, "primary_exchange", None),
        "market": getattr(d, "market", None),
        "list_date": _ts(getattr(d, "list_date", None)),
        "delisted_utc": _ts(getattr(d, "delisted_utc", None)),
        "effective_start": _ts(getattr(d, "list_date", None)),
        "effective_end": _ts(getattr(d, "delisted_utc", None)),
        "anchor_date": _ts(anchor_date),
        "updated": _ts(getattr(d, "updated", None)),
        # list_date is imprecise (IPO date, possibly under another symbol); only ticker events confirm a start
        "start_confirmed": False,
        "end_confirmed": bool(pd.notna(_ts(getattr(d, "delisted_utc", None)))),
    }


def _dedupe_holders(df: pd.DataFrame) -> pd.DataFrame:
    """One row per (ticker, holder_id): descriptive fields from the 'current' row when present; windows merged
    (earliest start, latest end, earliest anchor)."""
    if df.empty:
        return df
    for c in ("list_date", "delisted_utc", "effective_start", "effective_end", "anchor_date", "updated"):
        df[c] = pd.to_datetime(df[c], errors="coerce")
    for c in ("start_confirmed", "end_confirmed"):
        df[c] = df[c].fillna(False).astype(bool) if c in df.columns else False
    df = df.assign(_cur=(df["holder_source"] == "current").astype(int))
    df = df.sort_values(["ticker", "holder_id", "_cur"], ascending=[True, True, False])
    keyed = ("ticker", "holder_id", "effective_start", "effective_end", "anchor_date", "start_confirmed", "end_confirmed")
    agg = {c: "first" for c in SM_COLUMNS if c not in keyed}
    agg.update(anchor_date="min", start_confirmed="max", end_confirmed="max")
    out = df.groupby(["ticker", "holder_id"], as_index=False).agg(agg)
    # Windows: a CONFIRMED start (real ticker change) wins over an unconfirmed list_date; an open end (NaT) wins
    # over a stale delisted record of the same company (it is still trading), else the latest confirmed end.
    win = []
    for (t, h), g in df.groupby(["ticker", "holder_id"]):
        cs = g.loc[g["start_confirmed"], "effective_start"].dropna()
        start = cs.min() if len(cs) else g["effective_start"].min()
        if g["effective_end"].isna().any():
            end = pd.NaT
        else:
            ce = g.loc[g["end_confirmed"], "effective_end"].dropna()
            end = ce.max() if len(ce) else g["effective_end"].max()
        win.append({"ticker": t, "holder_id": h, "effective_start": start, "effective_end": end})
    out = out.merge(pd.DataFrame(win), on=["ticker", "holder_id"], how="left")
    out["end_confirmed"] = out["end_confirmed"] & out["effective_end"].notna()
    return out[SM_COLUMNS].sort_values(["ticker", "holder_source"]).reset_index(drop=True)


# --------------------------
# Security Master (details)
# --------------------------
def probe_previous_holders(
    tickers: Iterable[str],
    probe_dates: Sequence[str],
    *,
    api_key: Optional[str] = None,
    api_key_file: Optional[str | Path] = None,
    cli: Optional[RESTClient] = None,
) -> pd.DataFrame:
    """
    For each ticker and probe date, who held the ticker on that date (GET /v3/reference/tickers/{t}?date=).
    Rows in SM_COLUMNS with holder_source 'probe:<date>' and anchor_date = the probe date; a holder with
    neither FIGI nor CIK is dropped (it could not be told apart from the current holder and a phantom second
    holder would split the ticker's history downstream). +1 request per ticker per date.
    """
    if cli is None:
        cli = _client(load_api_key(api_key, api_key_file))
    rows: List[Dict[str, Any]] = []
    probe_dates = [str(x) for x in probe_dates]
    for t in _to_upper_list(tickers):
        for pdate in probe_dates:
            try:
                d = _retrying_call(cli.get_ticker_details, t, date=pdate)
            except BadResponse as e:
                if "Ticker not found" in str(e) or '"status":"NOT_FOUND"' in str(e):
                    continue   # nobody held this ticker on that date
                print(f"[warn] probe {t}@{pdate}: {str(e)[:200]}", file=sys.stderr)
                continue
            except Exception as e:
                print(f"[warn] probe {t}@{pdate}: {type(e).__name__}: {str(e)[:200]}", file=sys.stderr)
                continue
            row = _details_row(d, t, source=f"probe:{pdate}", anchor_date=pdate)
            if row["holder_id"].startswith("NOFIGI__"):
                print(f"[warn] probe {t}@{pdate}: holder has neither FIGI nor CIK ({row['name']!r}); ignored", file=sys.stderr)
                continue
            rows.append(row)
    return pd.DataFrame(rows, columns=SM_COLUMNS)


def pull_security_master(
    tickers: Iterable[str],
    *,
    out_parquet: str | Path,
    api_key: Optional[str] = None,
    api_key_file: Optional[str | Path] = None,
    fail_on_missing: bool = False,
    missing_out: Optional[str | Path] = None,
    probe_dates: Optional[Sequence[str]] = None,
) -> pd.DataFrame:
    """
    Security master: one row per (ticker, holder), where a holder is the company behind the ticker.

    GET /v3/reference/tickers/{ticker} gives the *current* holder (FIGI, CIK, list_date, ...). For each date in
    `probe_dates` the same endpoint is queried with ?date=, which returns whoever held the ticker on that date;
    a different company (different FIGI/CIK) is added as a previous holder with anchor_date = the probe date.
    That is how recycled tickers (old GM until 2009, new GM from 2010-11-18) get separate ids downstream.
    Costs one extra request per ticker per probe date.
    """
    key = load_api_key(api_key, api_key_file)
    cli = _client(key)
    probe_dates = [str(x) for x in (probe_dates or [])]

    rows: List[Dict[str, Any]] = []
    missing: List[str] = []
    tlist = _to_upper_list(tickers)

    for t in tqdm(tlist, desc="security master"):
        try:
            d = _retrying_call(cli.get_ticker_details, t)
            rows.append(_details_row(d, t, source="current"))
        except BadResponse as e:
            missing.append(t)
            print(f"[warn] security master {t}: {str(e)[:200]}", file=sys.stderr)
            if fail_on_missing:
                raise
        except Exception as e:
            missing.append(t)
            print(f"[warn] security master {t}: {type(e).__name__}: {str(e)[:200]}", file=sys.stderr)
            if fail_on_missing:
                raise

    probes = probe_previous_holders(tlist, probe_dates, cli=cli) if probe_dates else pd.DataFrame(columns=SM_COLUMNS)
    df = _dedupe_holders(pd.concat([pd.DataFrame(rows, columns=SM_COLUMNS), probes], ignore_index=True))
    out_parquet = Path(out_parquet)
    out_parquet.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_parquet, index=False)

    if missing_out:
        mp = Path(missing_out)
        if missing:
            mp.write_text("\n".join(missing) + "\n")
        elif mp.exists():
            mp.unlink()   # a stale list from an earlier run would misreport this pull

    return df


# --------------------------
# Dividends
# --------------------------
def pull_dividends(
    tickers: Iterable[str],
    *,
    out_parquet: str | Path,
    api_key: Optional[str] = None,
    api_key_file: Optional[str | Path] = None,
) -> pd.DataFrame:
    key = load_api_key(api_key, api_key_file)
    cli = _client(key)

    rows: List[Dict[str, Any]] = []
    failed: List[tuple] = []
    for t in tqdm(_to_upper_list(tickers), desc="dividends"):
        try:
            it = _retrying_call(cli.list_dividends, ticker=t, order="asc", sort="ex_dividend_date", limit=1000)
            for d in it:
                rows.append(
                    {
                        "ticker": t,
                        "ex_date": pd.to_datetime(getattr(d, "ex_dividend_date", None)),
                        "pay_date": pd.to_datetime(getattr(d, "pay_date", None)),
                        "cash_amount": getattr(d, "cash_amount", None),
                        "declaration_date": pd.to_datetime(getattr(d, "declaration_date", None)),
                        "record_date": pd.to_datetime(getattr(d, "record_date", None)),
                        "frequency": getattr(d, "frequency", None),
                    }
                )
        except BadResponse as e:
            if "Ticker not found" in str(e) or '"status":"NOT_FOUND"' in str(e):
                failed.append((t, "NOT_FOUND"))
                continue
            raise
        except Exception as e:
            failed.append((t, f"{type(e).__name__}: {e}"))
            continue

    out_parquet = Path(out_parquet)
    out_parquet.parent.mkdir(parents=True, exist_ok=True)
    _write_failures(out_parquet.parent / "_dividends_failed_tickers.txt", failed, what="dividends")
    df = pd.DataFrame(rows, columns=["ticker", "ex_date", "pay_date", "cash_amount",
                                     "declaration_date", "record_date", "frequency"])
    df = df.sort_values(["ticker", "ex_date"]).reset_index(drop=True)
    df.to_parquet(out_parquet, index=False)
    return df


# --------------------------
# Splits
# --------------------------
def pull_splits(
    tickers: Iterable[str],
    *,
    out_parquet: str | Path,
    api_key: Optional[str] = None,
    api_key_file: Optional[str | Path] = None,
) -> pd.DataFrame:
    key = load_api_key(api_key, api_key_file)
    cli = _client(key)

    rows: List[Dict[str, Any]] = []
    failed: List[tuple] = []
    for t in tqdm(_to_upper_list(tickers), desc="splits"):
        try:
            it = _retrying_call(cli.list_splits, ticker=t, order="asc", sort="execution_date", limit=1000)
            for s in it:
                split_from = getattr(s, "split_from", None)
                split_to = getattr(s, "split_to", None)
                ratio = (split_to or 0) / (split_from or 1) if (split_to is not None and split_from not in (None, 0)) else None
                rows.append(
                    {
                        "ticker": t,
                        "execution_date": pd.to_datetime(getattr(s, "execution_date", None)),
                        "split_from": split_from,
                        "split_to": split_to,
                        "ratio": ratio,
                    }
                )
        except BadResponse as e:
            if "Ticker not found" in str(e) or '"status":"NOT_FOUND"' in str(e):
                failed.append((t, "NOT_FOUND"))
                continue
            raise
        except Exception as e:
            # A silently dropped ticker here means an UNADJUSTED series across its splits
            # downstream, so record it loudly instead of swallowing the error.
            failed.append((t, f"{type(e).__name__}: {e}"))
            continue

    out_parquet = Path(out_parquet)
    out_parquet.parent.mkdir(parents=True, exist_ok=True)
    _write_failures(out_parquet.parent / "_splits_failed_tickers.txt", failed, what="splits")
    df = pd.DataFrame(rows, columns=["ticker", "execution_date", "split_from", "split_to", "ratio"])
    df = df.sort_values(["ticker", "execution_date"]).reset_index(drop=True)
    df.to_parquet(out_parquet, index=False)
    return df


# --------------------------
# Ticker events (symbol history per holder)
# --------------------------
EVENT_COLUMNS = ["query_ticker", "holder_id", "composite_figi", "cik", "name", "event_type", "date", "ticker"]


def _event_field(e, *path):
    cur = e
    for k in path:
        cur = cur.get(k) if isinstance(cur, dict) else getattr(cur, k, None)
        if cur is None:
            return None
    return cur


def pull_ticker_events(
    tickers: Iterable[str],
    *,
    out_parquet: str | Path,
    api_key: Optional[str] = None,
    api_key_file: Optional[str | Path] = None,
) -> pd.DataFrame:
    """
    GET /vX/reference/tickers/{ticker}/events for each ticker: the symbol history of the company currently
    behind it, e.g. META -> [FB from 2012-05-18, META from 2022-06-09]. One request per ticker.
    Rows: query_ticker, holder_id, composite_figi, cik, name, event_type, date, ticker (the symbol adopted).
    """
    key = load_api_key(api_key, api_key_file)
    cli = _client(key)

    rows: List[Dict[str, Any]] = []
    failed: List[tuple] = []
    for t in tqdm(_to_upper_list(tickers), desc="ticker events"):
        try:
            r = _retrying_call(cli.get_ticker_events, t)
        except BadResponse as e:
            if "Ticker not found" in str(e) or '"status":"NOT_FOUND"' in str(e) or "No events found" in str(e):
                failed.append((t, "NOT_FOUND"))
                continue
            raise
        except Exception as e:
            failed.append((t, f"{type(e).__name__}: {e}"))
            continue
        figi, cik, name = getattr(r, "composite_figi", None), getattr(r, "cik", None), getattr(r, "name", None)
        for e in (getattr(r, "events", None) or []):
            new_t = _event_field(e, "ticker_change", "ticker")
            rows.append({
                "query_ticker": t,
                "holder_id": holder_id(figi, cik, t),
                "composite_figi": figi, "cik": cik, "name": name,
                "event_type": _event_field(e, "type"),
                "date": _ts(_event_field(e, "date")),
                "ticker": str(new_t).strip().upper() if new_t else None,
            })

    out_parquet = Path(out_parquet)
    out_parquet.parent.mkdir(parents=True, exist_ok=True)
    _write_failures(out_parquet.parent / "_events_failed_tickers.txt", failed, what="ticker events")
    df = pd.DataFrame(rows, columns=EVENT_COLUMNS)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.sort_values(["holder_id", "date"]).reset_index(drop=True)
    df.to_parquet(out_parquet, index=False)
    return df


def symbol_history(events: pd.DataFrame) -> pd.DataFrame:
    """
    Per holder, the symbols it traded under with [start, end] windows (end = the day before the next change,
    NaT = still current). A company's rows under a former symbol (FB before META) are only stitched into its
    series if that former symbol is also in the ingest watchlist.
    """
    cols = ["holder_id", "ticker", "start", "end"]
    if events.empty:
        return pd.DataFrame(columns=cols)
    e = events[(events["event_type"] == "ticker_change")].dropna(subset=["date", "ticker"]).copy()
    if e.empty:
        return pd.DataFrame(columns=cols)
    e["date"] = pd.to_datetime(e["date"])
    e = e.sort_values(["holder_id", "date"]).drop_duplicates(["holder_id", "date", "ticker"])
    e["start"] = e["date"]
    e["end"] = e.groupby("holder_id")["date"].shift(-1) - pd.Timedelta(days=1)
    return e[cols].reset_index(drop=True)


def holders_from_history(events: pd.DataFrame, history: pd.DataFrame) -> pd.DataFrame:
    """
    Security-master rows derived from ticker-change events: one per (holder, symbol) window from
    symbol_history(). A company's rows under a FORMER symbol (Meta under FB, 2012-05-18 .. 2022-06-08) are
    thereby keyed to that company even when the tickers table has no record of it (FB is now a ProShares
    ETF). start_confirmed is True for a real adoption date (later than HISTORY_START); end_confirmed when the
    symbol was later changed away. Merge into a security master with merge_holders().
    """
    if history is None or history.empty:
        return pd.DataFrame(columns=SM_COLUMNS)
    meta = (events.dropna(subset=["holder_id"]).drop_duplicates("holder_id").set_index("holder_id")
            if events is not None and len(events) else pd.DataFrame())
    rows: List[Dict[str, Any]] = []
    for r in history.itertuples(index=False):
        start, end = pd.to_datetime(r.start), pd.to_datetime(r.end)
        m = meta.loc[r.holder_id] if (len(meta) and r.holder_id in meta.index) else None
        row = {c: None for c in SM_COLUMNS}
        row.update({
            "ticker": str(r.ticker).strip().upper(), "holder_id": r.holder_id, "holder_source": "events",
            "name": m["name"] if m is not None else None,
            "composite_figi": m["composite_figi"] if m is not None else None,
            "cik": m["cik"] if m is not None else None,
            "effective_start": start, "effective_end": end, "anchor_date": pd.NaT,
            "start_confirmed": bool(pd.notna(start) and start > HISTORY_START),
            "end_confirmed": bool(pd.notna(end)),
        })
        rows.append(row)
    df = pd.DataFrame(rows, columns=SM_COLUMNS)
    for c in ("list_date", "delisted_utc", "effective_start", "effective_end", "anchor_date", "updated"):
        df[c] = pd.to_datetime(df[c], errors="coerce")
    return df


def merge_holders(sm: pd.DataFrame, extra: pd.DataFrame) -> pd.DataFrame:
    """Union of security-master rows, one row per (ticker, holder) with windows merged by _dedupe_holders' rules."""
    if extra is None or extra.empty:
        return sm
    if sm is None or sm.empty:
        return _dedupe_holders(extra.copy())
    return _dedupe_holders(pd.concat([sm, extra[SM_COLUMNS]], ignore_index=True))


def refine_windows(sm: pd.DataFrame, history: pd.DataFrame) -> pd.DataFrame:
    """
    Tighten each security-master row's [effective_start, effective_end] to when that holder actually used
    that ticker, from the ticker-change history. Only matters for recycled tickers whose new holder came in
    via a rename (its list_date predates the symbol); single-holder tickers ignore windows downstream.
    """
    if sm.empty or history.empty:
        return sm
    h = history.copy()
    h["start"] = pd.to_datetime(h["start"]); h["end"] = pd.to_datetime(h["end"])
    agg = h.groupby(["holder_id", "ticker"], as_index=False).agg(start=("start", "min"), end=("end", "max"),
                                                                 open=("end", lambda x: bool(x.isna().any())))
    agg.loc[agg["open"], "end"] = pd.NaT
    out = sm.copy()
    out["effective_start"] = pd.to_datetime(out["effective_start"], errors="coerce")
    out["effective_end"] = pd.to_datetime(out["effective_end"], errors="coerce")
    out = out.merge(agg[["holder_id", "ticker", "start", "end"]], on=["holder_id", "ticker"], how="left")
    hs = out["start"].notna()
    out.loc[hs, "effective_start"] = out.loc[hs, ["effective_start", "start"]].max(axis=1)
    he = out["end"].notna()
    out.loc[he, "effective_end"] = out.loc[he, ["effective_end", "end"]].min(axis=1)
    if "start_confirmed" in out.columns:
        out.loc[hs & (out["start"] > HISTORY_START), "start_confirmed"] = True
    if "end_confirmed" in out.columns:
        out.loc[he, "end_confirmed"] = True
    return out.drop(columns=["start", "end"])
