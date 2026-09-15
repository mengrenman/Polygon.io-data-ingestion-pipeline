"""
Market-wide reference data in a few hundred requests instead of several per ticker.

Polygon's reference endpoints return the whole market when called without a ticker filter, 1,000 rows per
page, so a universe of any size (including the ~10k+ symbols in the flat files) costs the same:

  /v3/reference/tickers    active + delisted stocks (FIGI, CIK, delisting date)   ~50 pages
  /v3/reference/splits     every split                                           ~20 pages
  /v3/reference/dividends  every cash dividend                                    a few hundred pages

Splits and dividends carry a stable ``id``, so later runs fetch only rows since a date and merge by id.
A collection's ``security_master`` / ``stock_splits`` / ``cash_dividends`` parquet files are then *derived*
by filtering these tables, in the same schema the per-ticker pullers write, so nothing downstream changes.
"""
from __future__ import annotations

import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Sequence

import numpy as np
import pandas as pd

from . import SM_COLUMNS, _RATE_LIMIT_MIN_SLEEP_SEC, _dedupe_holders, _throttle, _to_upper_list, holder_id

BASE = "https://api.polygon.io"
Fetch = Callable[[str, Optional[dict]], dict]

MARKET_TICKERS = "market_tickers.parquet"
MARKET_SPLITS = "market_splits.parquet"
MARKET_DIVIDENDS = "market_dividends.parquet"

TICKER_COLS = ["ticker", "name", "active", "type", "market", "locale", "primary_exchange", "currency_name",
               "cik", "composite_figi", "share_class_figi", "delisted_utc", "last_updated_utc"]
SPLIT_COLS = ["id", "ticker", "execution_date", "split_from", "split_to", "ratio"]
DIVIDEND_COLS = ["id", "ticker", "ex_dividend_date", "pay_date", "record_date", "declaration_date",
                 "cash_amount", "currency", "frequency", "dividend_type"]
OVERLAP_DAYS = 30   # incremental refresh re-fetches this much history; merged by id so no duplicates


# --------------------------
# HTTP
# --------------------------
MAX_BACKOFF_SEC = 60.0
TRANSIENT_NETWORK_ERRORS = (urllib.error.URLError, TimeoutError, ConnectionError, OSError)   # URLError wraps DNS (gaierror)


def make_fetch(api_key: str, *, retries: int = 15, timeout: float = 60.0) -> Fetch:
    """
    GET -> JSON with pacing (POLYGON_MIN_INTERVAL_SEC); >= 12 s backoff per attempt on 429; exponential
    backoff capped at MAX_BACKOFF_SEC on 5xx and on network errors (DNS failures, resets, timeouts) - a
    multi-hour paged pull must outlast a several-minute network blip (15 retries ~ 9 minutes). The key
    travels in the Authorization header, never in a URL or log.
    """
    def fetch(url: str, params: Optional[dict] = None) -> dict:
        q = urllib.parse.urlencode({k: v for k, v in (params or {}).items() if v is not None})
        full = url + (("&" if "?" in url else "?") + q if q else "")
        for i in range(retries + 1):
            _throttle()
            req = urllib.request.Request(full, headers={"Authorization": f"Bearer {api_key}",
                                                        "User-Agent": "polygonio-ingestion/bulk"})
            try:
                with urllib.request.urlopen(req, timeout=timeout) as r:
                    return json.loads(r.read().decode("utf-8"))
            except urllib.error.HTTPError as e:
                if i < retries and (e.code == 429 or e.code >= 500):
                    time.sleep(max(_RATE_LIMIT_MIN_SLEEP_SEC, min(MAX_BACKOFF_SEC, 0.5 * 2 ** i)) if e.code == 429
                               else min(MAX_BACKOFF_SEC, 0.5 * 2 ** i))
                    continue
                raise
            except TRANSIENT_NETWORK_ERRORS as e:
                if i < retries:
                    wait = min(MAX_BACKOFF_SEC, 0.5 * 2 ** i)
                    print(f"[bulk] network error ({type(e).__name__}: {str(e)[:80]}); retry {i + 1}/{retries} in {wait:.0f}s", file=sys.stderr)
                    time.sleep(wait)
                    continue
                raise
        raise RuntimeError("unreachable")
    return fetch


def iter_results(path: str, params: dict, fetch: Fetch, *, label: str = "") -> Iterator[List[dict]]:
    """Yield each page's ``results``, following ``next_url`` (which already carries cursor and params)."""
    url: Optional[str] = BASE + path
    p: Optional[dict] = dict(params)
    n = 0
    while url:
        data = fetch(url, p)
        n += 1
        res = data.get("results") or []
        nxt = data.get("next_url")
        if label and (n == 1 or n % 10 == 0 or not nxt):
            print(f"[bulk] {label}: page {n} ({len(res)} rows){'' if nxt else ' - done'}", file=sys.stderr)
        yield res
        url, p = nxt, None


def _utc_naive(x) -> pd.Series:
    return pd.to_datetime(x, utc=True, errors="coerce").dt.tz_convert(None)


def _read_or_empty(path: Path, columns: List[str]) -> pd.DataFrame:
    return pd.read_parquet(path) if path.exists() else pd.DataFrame(columns=columns)


def _merge_by_id(old: pd.DataFrame, new: pd.DataFrame, sort_cols: List[str]) -> pd.DataFrame:
    """Rows from ``new`` replace rows in ``old`` with the same Polygon ``id``."""
    if old.empty:
        out = new.copy()
    else:
        out = pd.concat([old[new.columns], new], ignore_index=True).drop_duplicates("id", keep="last")
    return out.sort_values(sort_cols).reset_index(drop=True)


# --------------------------
# Market tables
# --------------------------
def pull_market_tickers(out_parquet: str | Path, fetch: Fetch, *, market: str = "stocks") -> pd.DataFrame:
    """
    Every active and delisted ticker of ``market`` with FIGI / CIK / delisting date, plus ``holder_id``.
    A recycled symbol shows up as one active record and one or more delisted records with the same ticker
    (only when the previous company kept that symbol until it was delisted; if it was renamed first, the
    delisted record is under its final symbol - use ``--probe-dates`` for those). Always a full refetch.
    """
    rows: List[Dict[str, Any]] = []
    for active in ("true", "false"):
        params = {"market": market, "active": active, "limit": 1000, "order": "asc", "sort": "ticker"}
        for page in iter_results("/v3/reference/tickers", params, fetch, label=f"tickers active={active}"):
            rows.extend({c: r.get(c) for c in TICKER_COLS} for r in page)
    df = pd.DataFrame(rows, columns=TICKER_COLS)
    df["ticker"] = df["ticker"].astype(str).str.strip().str.upper()
    for c in ("delisted_utc", "last_updated_utc"):
        df[c] = _utc_naive(df[c])
    df["holder_id"] = [holder_id(f, k, t) for f, k, t in zip(df["composite_figi"], df["cik"], df["ticker"])]
    df["pulled_at"] = pd.Timestamp.now(tz="UTC").tz_convert(None)
    df = df.sort_values(["ticker", "active"], ascending=[True, False]).reset_index(drop=True)
    out_parquet = Path(out_parquet)
    out_parquet.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_parquet, index=False)
    return df


CHECKPOINT_EVERY_PAGES = 25   # ~5 minutes of paced paging


def _write_atomic(df: pd.DataFrame, out_parquet: Path) -> None:
    out_parquet.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_parquet.with_suffix(".parquet.inprogress")
    df.to_parquet(tmp, index=False)
    tmp.replace(out_parquet)


def _pull_paged_table(path: str, params: Dict[str, Any], out_parquet: Path, fetch: Fetch, *, label: str,
                      columns: List[str], normalize, sort_cols: List[str],
                      checkpoint_every: int = CHECKPOINT_EVERY_PAGES) -> pd.DataFrame:
    """
    Page through an endpoint sorted ascending by date and CHECKPOINT: every `checkpoint_every` pages (and on
    any exception) the rows fetched so far are merged by id into the table on disk. Because pages arrive in
    date order, everything before the table's latest date is complete, so a rerun resumes from that date
    minus OVERLAP_DAYS (see _incremental_since) instead of starting over. A 5-hour pull that dies on a DNS
    blip at page 1,580 therefore costs minutes, not hours, to finish.
    """
    out_parquet = Path(out_parquet)
    existing = _read_or_empty(out_parquet, columns)
    rows: List[Dict[str, Any]] = []
    n_pages = 0

    def flush() -> pd.DataFrame:
        nonlocal existing, rows
        if not rows:
            return existing
        new = normalize(pd.DataFrame(rows, columns=[c for c in columns if c in rows[0] or True]))
        existing = _merge_by_id(existing, new[columns], sort_cols)
        _write_atomic(existing, out_parquet)
        rows = []
        return existing

    try:
        for page in iter_results(path, params, fetch, label=label):
            rows.extend({c: r.get(c) for c in columns} for r in page)
            n_pages += 1
            if checkpoint_every and n_pages % checkpoint_every == 0:
                flush()
    except BaseException:
        if rows:
            flush()
            print(f"[bulk] {label}: interrupted after {n_pages} pages; progress checkpointed to {out_parquet} "
                  f"(rerun resumes from the table's latest date - {OVERLAP_DAYS} days)", file=sys.stderr)
        raise
    return flush()


def _normalize_splits(new: pd.DataFrame) -> pd.DataFrame:
    new = new.copy()
    new["ticker"] = new["ticker"].astype(str).str.strip().str.upper()
    new["execution_date"] = pd.to_datetime(new["execution_date"], errors="coerce")
    sf = pd.to_numeric(new["split_from"], errors="coerce")
    st = pd.to_numeric(new["split_to"], errors="coerce")
    new["ratio"] = np.where((sf > 0) & st.notna(), st / sf, np.nan)
    return new[SPLIT_COLS]


def _normalize_dividends(new: pd.DataFrame) -> pd.DataFrame:
    new = new.copy()
    new["ticker"] = new["ticker"].astype(str).str.strip().str.upper()
    for c in ("ex_dividend_date", "pay_date", "record_date", "declaration_date"):
        new[c] = pd.to_datetime(new[c], errors="coerce")
    new["cash_amount"] = pd.to_numeric(new["cash_amount"], errors="coerce")
    return new[DIVIDEND_COLS]


def pull_market_splits(out_parquet: str | Path, fetch: Fetch, *, since: Optional[str] = None,
                       checkpoint_every: int = CHECKPOINT_EVERY_PAGES) -> pd.DataFrame:
    """Every split (``execution_date >= since`` if given), merged by id into an existing table; checkpointed."""
    params: Dict[str, Any] = {"limit": 1000, "order": "asc", "sort": "execution_date"}
    if since:
        params["execution_date.gte"] = str(since)[:10]
    return _pull_paged_table("/v3/reference/splits", params, Path(out_parquet), fetch, label="splits",
                             columns=SPLIT_COLS, normalize=_normalize_splits, sort_cols=["ticker", "execution_date"],
                             checkpoint_every=checkpoint_every)


def pull_market_dividends(out_parquet: str | Path, fetch: Fetch, *, since: Optional[str] = None,
                          checkpoint_every: int = CHECKPOINT_EVERY_PAGES) -> pd.DataFrame:
    """Every cash dividend (``ex_dividend_date >= since`` if given), merged by id into an existing table; checkpointed."""
    params: Dict[str, Any] = {"limit": 1000, "order": "asc", "sort": "ex_dividend_date"}
    if since:
        params["ex_dividend_date.gte"] = str(since)[:10]
    return _pull_paged_table("/v3/reference/dividends", params, Path(out_parquet), fetch, label="dividends",
                             columns=DIVIDEND_COLS, normalize=_normalize_dividends, sort_cols=["ticker", "ex_dividend_date"],
                             checkpoint_every=checkpoint_every)


def _incremental_since(path: Path, date_col: str) -> Optional[str]:
    if not path.exists():
        return None
    d = pd.to_datetime(pd.read_parquet(path, columns=[date_col])[date_col], errors="coerce").max()
    return None if pd.isna(d) else (d - pd.Timedelta(days=OVERLAP_DAYS)).strftime("%Y-%m-%d")


ALL_TABLES = ("tickers", "splits", "dividends")


def pull_market_refdata(market_dir: str | Path, fetch: Fetch, *, since: Optional[str] = None,
                        full: bool = False, tickers: bool = True,
                        tables: Sequence[str] = ALL_TABLES) -> Dict[str, pd.DataFrame]:
    """
    Pull / refresh the market tables in ``market_dir`` (``tables`` selects which; a table not selected is
    read from disk if present). Splits and dividends are incremental by default (rows since the table's
    latest date minus OVERLAP_DAYS, merged by id - which is also how an interrupted pull resumes); ``since``
    overrides that, ``full`` refetches everything. The tickers table is always refetched in full.
    """
    market_dir = Path(market_dir)
    market_dir.mkdir(parents=True, exist_ok=True)
    tables = tuple(tables)
    unknown = set(tables) - set(ALL_TABLES)
    if unknown:
        raise ValueError(f"unknown tables {sorted(unknown)}; choose from {ALL_TABLES}")
    out: Dict[str, pd.DataFrame] = {}
    if tickers and "tickers" in tables:
        out["tickers"] = pull_market_tickers(market_dir / MARKET_TICKERS, fetch)
    elif (market_dir / MARKET_TICKERS).exists():
        out["tickers"] = pd.read_parquet(market_dir / MARKET_TICKERS)
    if "splits" in tables:
        s_since = None if full else (since or _incremental_since(market_dir / MARKET_SPLITS, "execution_date"))
        print(f"[bulk] splits since {s_since or 'the beginning'}", file=sys.stderr)
        out["splits"] = pull_market_splits(market_dir / MARKET_SPLITS, fetch, since=s_since)
    elif (market_dir / MARKET_SPLITS).exists():
        out["splits"] = pd.read_parquet(market_dir / MARKET_SPLITS)
    if "dividends" in tables:
        d_since = None if full else (since or _incremental_since(market_dir / MARKET_DIVIDENDS, "ex_dividend_date"))
        print(f"[bulk] dividends since {d_since or 'the beginning'}", file=sys.stderr)
        out["dividends"] = pull_market_dividends(market_dir / MARKET_DIVIDENDS, fetch, since=d_since)
    elif (market_dir / MARKET_DIVIDENDS).exists():
        out["dividends"] = pd.read_parquet(market_dir / MARKET_DIVIDENDS)
    return out


# --------------------------
# Collection files derived from the market tables
# --------------------------
def derive_collection_refdata(market_dir: str | Path, tickers: Iterable[str], outdir: str | Path,
                              *, extra_holders: Optional[pd.DataFrame] = None) -> Dict[str, Any]:
    """
    Filter the market tables to ``tickers`` and write ``security_master.parquet``, ``stock_splits.parquet``,
    ``cash_dividends.parquet`` (+ ``_missing_tickers.txt``) in the per-ticker pullers' schema.
    ``extra_holders`` (rows in SM_COLUMNS, e.g. from probe dates) are merged into the security master.
    """
    market_dir, outdir = Path(market_dir), Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    uni = _to_upper_list(tickers)
    uset = set(uni)

    tk = pd.read_parquet(market_dir / MARKET_TICKERS)
    src = tk[tk["ticker"].isin(uset)].copy()
    active = src["active"].fillna(False).astype(bool)
    # A delisted record with neither FIGI nor CIK carries no evidence of being a *different* company (many are
    # the same listing under an older record). Splitting a ticker's history on it would re-anchor one company's
    # adjustment factors, so such records are dropped here and reported as --probe-dates candidates.
    noid = (~active) & src["holder_id"].astype(str).str.startswith("NOFIGI__")
    ambiguous = (src[noid][["ticker", "name", "delisted_utc"]].sort_values(["ticker", "delisted_utc"]).reset_index(drop=True))
    src, active = src[~noid], active[~noid]
    sm = pd.DataFrame({
        "ticker": src["ticker"],
        "holder_id": src["holder_id"],
        "holder_source": np.where(active, "market:active", "market:delisted"),
        "name": src["name"], "active": active, "type": src["type"],
        "composite_figi": src["composite_figi"], "share_class_figi": src["share_class_figi"], "cik": src["cik"],
        "locale": src["locale"], "currency_name": src["currency_name"],
        "primary_exchange": src["primary_exchange"], "market": src["market"],
        "list_date": pd.NaT,
        "delisted_utc": src["delisted_utc"],
        "effective_start": pd.NaT,                      # the list endpoint carries no list_date
        "effective_end": src["delisted_utc"],           # NaT for the active holder
        "anchor_date": pd.NaT,
        "updated": src["last_updated_utc"],
        "start_confirmed": False,                       # the list endpoint carries no adoption date
        "end_confirmed": src["delisted_utc"].notna(),   # a delisting date is a real end
    })[SM_COLUMNS]
    if extra_holders is not None and len(extra_holders):
        sm = pd.concat([sm, extra_holders[SM_COLUMNS]], ignore_index=True)
    # the 'current' source must win the descriptive fields in _dedupe_holders; market:active is current
    sm["holder_source"] = sm["holder_source"].replace({"market:active": "current"})
    sm = _dedupe_holders(sm)
    sm["holder_source"] = sm["holder_source"].replace({"current": "market:active"})
    sm.to_parquet(outdir / "security_master.parquet", index=False)

    missing = sorted(uset - set(tk["ticker"]))
    (outdir / "_missing_tickers.txt").write_text("\n".join(missing) + ("\n" if missing else ""))
    amb_path = outdir / "_ambiguous_delisted_records.csv"
    if len(ambiguous):
        ambiguous.to_csv(amb_path, index=False)
    elif amb_path.exists():
        amb_path.unlink()

    spl = pd.read_parquet(market_dir / MARKET_SPLITS)
    spl = spl[spl["ticker"].isin(uset)][["ticker", "execution_date", "split_from", "split_to", "ratio"]]
    spl = spl.sort_values(["ticker", "execution_date"]).reset_index(drop=True)
    spl.to_parquet(outdir / "stock_splits.parquet", index=False)

    div = pd.read_parquet(market_dir / MARKET_DIVIDENDS)
    div = div[div["ticker"].isin(uset)].rename(columns={"ex_dividend_date": "ex_date"})
    div = div[["ticker", "ex_date", "pay_date", "cash_amount", "declaration_date", "record_date", "frequency"]]
    div = div.sort_values(["ticker", "ex_date"]).reset_index(drop=True)
    div.to_parquet(outdir / "cash_dividends.parquet", index=False)

    n_multi = int((sm.groupby("ticker")["holder_id"].nunique() > 1).sum()) if len(sm) else 0
    return {"tickers": len(uni), "security_master_rows": len(sm), "multi_holder_tickers": n_multi,
            "missing": missing, "ambiguous_delisted": sorted(ambiguous["ticker"].unique().tolist()),
            "splits": len(spl), "dividends": len(div)}
