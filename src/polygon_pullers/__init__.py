# polygon_pullers.py
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from typing import Iterable, List, Optional, Dict, Any

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


# --------------------------
# Security Master (details)
# --------------------------
def pull_security_master(
    tickers: Iterable[str],
    *,
    out_parquet: str | Path,
    api_key: Optional[str] = None,
    api_key_file: Optional[str | Path] = None,
    fail_on_missing: bool = False,
    missing_out: Optional[str | Path] = None,
) -> pd.DataFrame:
    """
    For each ticker call GET /v3/reference/tickers/{ticker} and persist a compact frame.
    """
    key = load_api_key(api_key, api_key_file)
    cli = _client(key)

    rows: List[Dict[str, Any]] = []
    missing: List[str] = []

    for t in tqdm(_to_upper_list(tickers), desc="security master"):
        try:
            d = _retrying_call(cli.get_ticker_details, t)
            rows.append(
                {
                    "ticker": getattr(d, "ticker", t),
                    "name": getattr(d, "name", None),
                    "active": getattr(d, "active", None),
                    "cik": getattr(d, "cik", None),
                    "locale": getattr(d, "locale", None),
                    "currency_name": getattr(d, "currency_name", None),
                    "primary_exchange": getattr(d, "primary_exchange", None),
                    "market": getattr(d, "market", None),
                    "type": getattr(d, "type", None),
                    "list_date": pd.to_datetime(getattr(d, "list_date", None)),
                    "updated": pd.to_datetime(getattr(d, "updated", None)),
                }
            )
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

    df = pd.DataFrame(rows).sort_values("ticker").reset_index(drop=True)
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
# Ticker Events (placeholder)
# --------------------------
def pull_ticker_events(
    tickers: Iterable[str],
    *,
    out_parquet: str | Path,
    api_key: Optional[str] = None,
    api_key_file: Optional[str | Path] = None,
) -> pd.DataFrame:
    """
    Placeholder for any additional per-ticker "events" you may want to pull later.
    For now, produce an empty parquet so downstream steps don't fail.
    """
    df = pd.DataFrame(columns=["ticker", "event_type", "published_utc", "title", "url"])
    out_parquet = Path(out_parquet)
    out_parquet.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_parquet, index=False)
    return df
