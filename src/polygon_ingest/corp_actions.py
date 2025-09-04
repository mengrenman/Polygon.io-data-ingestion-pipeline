# src/polygon_ingest/corp_actions.py
from __future__ import annotations
import os
import pandas as pd
from polygon import RESTClient

def _client() -> RESTClient:
    key = os.getenv("POLYGON_API_KEY")
    if not key:
        raise RuntimeError("POLYGON_API_KEY not set")
    return RESTClient(key)

def fetch_splits(ticker: str) -> pd.DataFrame:
    c = _client()
    rows = []
    for s in c.list_splits(ticker=ticker, order="asc", limit=1000, sort="execution_date"):
        rows.append({
            "execution_date": pd.to_datetime(s.execution_date),
            "split_from": s.split_from,
            "split_to": s.split_to,
        })
    return pd.DataFrame(rows).sort_values("execution_date").reset_index(drop=True)

def fetch_dividends(ticker: str) -> pd.DataFrame:
    c = _client()
    rows = []
    for d in c.list_dividends(ticker=ticker, order="asc", limit=1000, sort="ex_dividend_date"):
        rows.append({
            "ex_date": pd.to_datetime(d.ex_dividend_date),
            "pay_date": pd.to_datetime(getattr(d, "pay_date", None)),
            "cash_amount": d.cash_amount,
        })
    return pd.DataFrame(rows).sort_values("ex_date").reset_index(drop=True)
