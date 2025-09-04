# polygon_pullers.py
from __future__ import annotations

import json
import os
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


def _retrying_call(fn, *args, _retries: int = 5, _delay: float = 0.5, **kwargs):
    """
    Simple retry wrapper for Polygon REST calls:
    - Retries on rate-limit and 5xx responses with exponential backoff.
    - Hard-fails on 'Ticker not found'.
    """
    for i in range(_retries + 1):
        try:
            return fn(*args, **kwargs)
        except BadResponse as e:
            msg = str(e)
            # Hard skip (caller will decide) for unknown ticker
            if "Ticker not found" in msg or '"status":"NOT_FOUND"' in msg:
                raise
            # Rate limits / transient errors — backoff & retry
            if any(s in msg for s in ("429", "Too Many Requests", "rate limit", "Internal Server Error", "503", "502")):
                if i < _retries:
                    time.sleep(_delay * (2 ** i))
                    continue
            # Other errors: bubble up
            raise
        except Exception:
            # Non-HTTP exceptions (network, etc.)
            if i < _retries:
                time.sleep(_delay * (2 ** i))
                continue
            raise


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
        except BadResponse:
            missing.append(t)
            if fail_on_missing:
                raise
        except Exception:
            missing.append(t)
            if fail_on_missing:
                raise

    df = pd.DataFrame(rows).sort_values("ticker").reset_index(drop=True)
    out_parquet = Path(out_parquet)
    out_parquet.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_parquet, index=False)

    if missing and missing_out:
        Path(missing_out).write_text("\n".join(missing))

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
            # Unknown ticker -> skip gracefully
            if "Ticker not found" in str(e) or '"status":"NOT_FOUND"' in str(e):
                continue
            raise
        except Exception:
            # Non-fatal: continue with next ticker
            continue

    df = pd.DataFrame(rows).sort_values(["ticker", "ex_date"]).reset_index(drop=True)
    out_parquet = Path(out_parquet)
    out_parquet.parent.mkdir(parents=True, exist_ok=True)
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
                continue
            raise
        except Exception:
            continue

    df = pd.DataFrame(rows).sort_values(["ticker", "execution_date"]).reset_index(drop=True)
    out_parquet = Path(out_parquet)
    out_parquet.parent.mkdir(parents=True, exist_ok=True)
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
