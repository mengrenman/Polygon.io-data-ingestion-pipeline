"""
Point-in-time universe: which tickers were investable on each rebalance date, decided only from data
available on that date, so names that were later delisted, acquired or renamed stay in for the periods
they traded. This replaces a current index constituent list (today's S&P 500 applied back to 2003), which
keeps only the companies that survived and grew into the index.

Inputs
- a market-layout DAY lake (<root>/<YYYY>/<MM>.parquet, all tickers; `poly bars --layout market`)
- the market tickers table (refdata/_market/market_tickers.parquet) for security type / exchange / name

Method
1. Trading-history segments: a ticker's rows are split at gaps of >= `gap_days`; a gap that long means the
   symbol was reused by another company (BSC: Bear Stearns until 2008, an ETN since). The tickers table's
   record describes the CURRENT holder, so its type applies to the LAST segment only; earlier segments are
   "untyped" and judged by a symbol heuristic (a lowercase class letter as Polygon writes it, dot-suffix
   classes .U/.WS/.W/.R/.RT/.P* and NASDAQ 5th-letter W/U/R are derivatives, everything else is a share).
2. Liquidity: trailing average dollar volume (close x volume) over `lookback` trading days, per ticker,
   from a dense (days x tickers) matrix; a ticker must have traded within the last `max_stale` trading days
   and have >= `min_days` observations in the window (excludes brand-new listings, a standard bias control).
3. On each rebalance date (month-end trading day by default) eligible tickers are ranked by trailing dollar
   volume and the top `top_n` are members; optional `min_price` / `min_adv` floors.

Output: membership.parquet (rebalance_date, ticker, rank, adv_usd, close, n_days, segment type/eligibility,
current name/type/active), plus segments.parquet and summary.json diagnostics. Use `expand_daily()` to get
the membership on every trading day (forward-filled between rebalances) for research joins.
"""
from __future__ import annotations

import glob
import json
import re
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence

import numpy as np
import pandas as pd

from .security_type import UNTYPED_SOURCES, fill_type
from .tickers import resolve

DERIVATIVE_SUFFIX = re.compile(r"\.(U|UN|WS|W|WT|R|RT|RTS|P[A-Z]?|PR[A-Z]?)$")
NASDAQ_5TH_LETTER_DERIVATIVE = re.compile(r"^[A-Z]{4}[WUR]$")
SHARE_TYPES_DEFAULT = ("CS",)
FUND_TYPES = {"ETF", "ETN", "ETV", "ETS", "FUND", "SP", "INDEX"}
# Exchange-listed names print every day, so a gap this long followed by trading again means the symbol was
# reused (Bear Stearns -> an ETN after 69 days; the old GM -> the new GM after 17 months).
GAP_DAYS_DEFAULT = 60
UNTYPED_POLICIES = ("admit", "exclude", "exclude-if-current-fund")


# --------------------------
# Loading
# --------------------------
def load_market_day_lake(root: str | Path, start: Optional[str] = None, end: Optional[str] = None,
                         columns: Sequence[str] = ("datetime", "ticker", "close", "volume")) -> pd.DataFrame:
    """Read a market-layout day lake into one frame with an ET `date` column (tz-naive midnight)."""
    root = Path(root)
    files = sorted(glob.glob(str(root / "*" / "*.parquet")))
    if not files:
        raise FileNotFoundError(f"no <YYYY>/<MM>.parquet files under {root} (market layout expected)")
    s = pd.Timestamp(start) if start else None
    e = pd.Timestamp(end) if end else None
    keep = []
    for f in files:
        y, m = int(Path(f).parent.name), int(Path(f).stem)
        first, last = pd.Timestamp(y, m, 1), pd.Timestamp(y, m, 1) + pd.offsets.MonthEnd(0)
        if (s is not None and last < s) or (e is not None and first > e):
            continue
        keep.append(f)
    df = pd.concat([pd.read_parquet(f, columns=list(columns)) for f in keep], ignore_index=True)
    dt = pd.to_datetime(df["datetime"], utc=True)
    df["date"] = dt.dt.tz_convert("US/Eastern").dt.normalize().dt.tz_localize(None)
    df = df.drop(columns=["datetime"])
    if s is not None:
        df = df[df["date"] >= s]
    if e is not None:
        df = df[df["date"] <= e]
    df["ticker"] = df["ticker"].astype(str).str.strip()   # the case is the share class; see polygon_ingest.tickers
    return df.reset_index(drop=True)


# --------------------------
# Segments and eligibility
# --------------------------
def trading_segments(df: pd.DataFrame, gap_days: int = GAP_DAYS_DEFAULT) -> pd.DataFrame:
    """Per ticker, contiguous trading-history segments split at calendar gaps >= gap_days.
    Columns: ticker, segment (0-based), start, end, n_days, is_last."""
    d = df[["ticker", "date"]].drop_duplicates().sort_values(["ticker", "date"])
    gap = d.groupby("ticker")["date"].diff().dt.days
    d["segment"] = (gap >= gap_days).groupby(d["ticker"]).cumsum().astype(int)
    seg = d.groupby(["ticker", "segment"], as_index=False).agg(start=("date", "min"), end=("date", "max"), n_days=("date", "size"))
    seg["is_last"] = seg["segment"] == seg.groupby("ticker")["segment"].transform("max")
    return seg


def looks_like_derivative(ticker: str) -> bool:
    """Symbol heuristic for records without a security type: preferreds, warrants, units, rights.

    Polygon writes the class code in LOWER case - `AAp` is Alcoa's $3.75 preferred, `AAGpT` preferred
    series T, `AANw` a when-issued line - so a lowercase letter is itself the marker, and it is the one
    that catches most of them: 4,041 of the flat files' 33,833 symbols carry one. The dot-suffix and
    NASDAQ fifth-letter forms cover the spellings that encode the class without case.

    A `w` line is excluded here along with the rest. It is usually a when-issued duplicate of the
    common stock rather than a separate security, which is a reason to keep it out of a universe, not
    a claim that it is a warrant - see `polygon_ingest.security_type`.
    """
    t = str(ticker).strip()
    if any(c.islower() for c in t):
        return True
    return bool(DERIVATIVE_SUFFIX.search(t)) or bool(NASDAQ_5TH_LETTER_DERIVATIVE.match(t))


def classify_segments(segments: pd.DataFrame, tickers_table: Optional[pd.DataFrame],
                      share_types: Sequence[str] = SHARE_TYPES_DEFAULT,
                      exchanges: Optional[Sequence[str]] = None,
                      include_untyped: bool = True,
                      untyped_policy: str = "admit",
                      exclude_tickers: Optional[Iterable[str]] = None,
                      use_inferred_type: bool = True) -> pd.DataFrame:
    """
    Add `type`, `exchange`, `eligible`, `reason` to segments. The tickers table describes the CURRENT holder
    of a symbol, so its type/exchange apply to the last segment only; earlier segments (previous holders)
    are untyped. `untyped_policy`:
      admit                    admit unless the symbol looks like a derivative (default). Recovers Bear Stearns
                               (BSC is an ETN today), Meta under FB (an ETF today), Wachovia, Sun, DirecTV...
                               but also admits a fund that came back to its own symbol (QQQ 2003-04, VXX).
      exclude-if-current-fund  ...except when the current record is a fund type: drops QQQ/VXX AND Bear Stearns.
      exclude                  never admit untyped segments.
    `exclude_tickers` removes symbols by hand (e.g. QQQ, VXX under the default policy). Review the admitted
    untyped segments in summary.json / segments.parquet.

    `use_inferred_type` (default True) fills a record's missing type from its name and symbol via
    `polygon_ingest.security_type`. Polygon returns no type for 28.8% of delisted records and none of the
    active ones, so without this the type filter silently selects for survival. `type_source` on the output
    says where each segment's type came from: `polygon`, `name`, `symbol`, or a reason it is still unknown.
    """
    if untyped_policy not in UNTYPED_POLICIES:
        raise ValueError(f"untyped_policy must be one of {UNTYPED_POLICIES}")
    if not include_untyped:
        untyped_policy = "exclude"
    seg = segments.copy()
    tk = None
    if tickers_table is not None and len(tickers_table):
        tk = tickers_table.copy()
        tk["ticker"] = tk["ticker"].astype(str).str.strip()
        act = tk["active"].fillna(False).astype(bool) if "active" in tk.columns else pd.Series(True, index=tk.index)
        # prefer the active record; among delisted, the most recently delisted
        order = tk.assign(_a=act.astype(int), _d=pd.to_datetime(tk["delisted_utc"], errors="coerce") if "delisted_utc" in tk.columns else pd.NaT)
        tk = order.sort_values(["ticker", "_a", "_d"], ascending=[True, False, False]).drop_duplicates("ticker")
        if use_inferred_type:
            tk = fill_type(tk)                       # Polygon's value wins; only nulls are inferred
            tk["type"] = tk["type_inferred"]
        else:
            tk["type_source"] = np.where(tk["type"].notna(), "polygon", "unmatched")
        tk = tk[["ticker", "type", "type_source", "primary_exchange", "name", "active", "holder_id"]].rename(
            columns={"type": "rec_type", "type_source": "rec_type_source", "primary_exchange": "rec_exchange",
                     "name": "rec_name", "active": "rec_active", "holder_id": "rec_holder_id"})
        seg = seg.merge(tk, on="ticker", how="left")
    else:
        for c in ("rec_type", "rec_type_source", "rec_exchange", "rec_name", "rec_active", "rec_holder_id"):
            seg[c] = None

    last = seg["is_last"].astype(bool)
    seg["type"] = np.where(last, seg["rec_type"], None)
    seg["type_source"] = np.where(last, seg["rec_type_source"], "earlier-segment")
    seg["exchange"] = np.where(last, seg["rec_exchange"], None)
    seg["name"] = np.where(last, seg["rec_name"], None)
    seg["holder_id"] = np.where(last, seg["rec_holder_id"], None)

    typed = seg["type"].notna()
    is_share = typed & seg["type"].isin(list(share_types))
    deriv = seg["ticker"].map(looks_like_derivative).astype(bool)
    current_type = seg["ticker"].map(seg[seg["is_last"]].set_index("ticker")["type"]) if len(seg) else pd.Series(dtype=object)
    current_is_fund = current_type.isin(FUND_TYPES)
    if untyped_policy == "admit":
        untyped_ok = (~typed) & (~deriv)
    elif untyped_policy == "exclude-if-current-fund":
        untyped_ok = (~typed) & (~deriv) & (~current_is_fund)
    else:
        untyped_ok = pd.Series(False, index=seg.index)
    # a hand-written exclusion list is matched exactly, then case-insensitively where that is unambiguous
    excl_map, _, _ = resolve(exclude_tickers or [], seg["ticker"].unique())
    excl = seg["ticker"].isin(set(excl_map.values()))
    eligible = (is_share | untyped_ok) & ~excl
    if exchanges:
        exch_ok = seg["exchange"].isna() | seg["exchange"].isin(list(exchanges))
        eligible = eligible & exch_ok
    reason = np.select(
        [excl, is_share & eligible, untyped_ok & eligible, typed & ~is_share, deriv & ~typed,
         (~typed) & current_is_fund & (untyped_policy == "exclude-if-current-fund"), ~eligible],
        ["excluded_by_list", "share_type", "untyped_symbol_ok", "type_excluded", "derivative_symbol",
         "untyped_current_is_fund", "exchange_excluded"], default="excluded")
    seg["eligible"] = eligible.astype(bool)
    seg["reason"] = reason
    return seg.drop(columns=["rec_type", "rec_type_source", "rec_exchange", "rec_name", "rec_active", "rec_holder_id"])


# --------------------------
# Liquidity and membership
# --------------------------
def rebalance_dates(trading_days: pd.DatetimeIndex, freq: str = "M") -> pd.DatetimeIndex:
    """Last trading day of each period ('M' month, 'Q' quarter, 'W' week, 'D' every day)."""
    s = pd.Series(trading_days, index=trading_days)
    if freq.upper() == "D":
        return trading_days
    key = {"M": s.dt.to_period("M"), "Q": s.dt.to_period("Q"), "W": s.dt.to_period("W")}[freq.upper()]
    return pd.DatetimeIndex(s.groupby(key).max().values)


def build_membership(df: pd.DataFrame, segments: pd.DataFrame, *, top_n: int = 1000, lookback: int = 63,
                     min_days: int = 40, max_stale: int = 5, min_price: Optional[float] = None,
                     min_adv: Optional[float] = None, freq: str = "M",
                     start: Optional[str] = None, end: Optional[str] = None) -> pd.DataFrame:
    """
    Membership on each rebalance date from trailing dollar volume, using only rows dated <= that date.
    Returns one row per (rebalance_date, ticker) with rank, adv_usd, close, n_days and segment attributes.
    """
    d = df[["date", "ticker", "close", "volume"]].dropna(subset=["close"]).copy()
    d["dv"] = d["close"].astype(float) * d["volume"].astype(float)
    days = pd.DatetimeIndex(np.sort(d["date"].unique()))
    tickers = np.sort(d["ticker"].unique())
    di = pd.Index(days).get_indexer(d["date"])
    ti = pd.Index(tickers).get_indexer(d["ticker"])
    T, N = len(days), len(tickers)
    dv = np.full((T, N), np.nan); px = np.full((T, N), np.nan)
    dv[di, ti] = d["dv"].to_numpy(); px[di, ti] = d["close"].to_numpy()

    # trailing sums / counts via cumsum on NaN-as-zero copies
    obs = ~np.isnan(dv)
    c_dv = np.cumsum(np.where(obs, dv, 0.0), axis=0); c_n = np.cumsum(obs, axis=0)
    # last observed close and its position (forward fill)
    pos = np.where(obs, np.arange(T)[:, None], -1); last_pos = np.maximum.accumulate(pos, axis=0)
    px_ff = pd.DataFrame(px).ffill().to_numpy()

    rdates = rebalance_dates(days, freq)
    if start: rdates = rdates[rdates >= pd.Timestamp(start)]
    if end: rdates = rdates[rdates <= pd.Timestamp(end)]
    seg = segments[["ticker", "segment", "start", "end", "eligible", "reason", "type", "name", "holder_id"]]

    out = []
    for rd in rdates:
        t = days.get_loc(rd)
        lo = t - lookback   # window (lo, t]
        s_dv = c_dv[t] - (c_dv[lo] if lo >= 0 else 0.0)
        n = c_n[t] - (c_n[lo] if lo >= 0 else 0)
        adv = np.where(n > 0, s_dv / np.maximum(n, 1), np.nan)
        stale = (t - last_pos[t]) > max_stale
        ok = (n >= min_days) & ~stale & np.isfinite(adv) & (adv > 0)
        if min_price is not None: ok &= px_ff[t] >= min_price
        if min_adv is not None: ok &= adv >= min_adv
        idx = np.where(ok)[0]
        if len(idx) == 0:
            continue
        cand = pd.DataFrame({"rebalance_date": rd, "ticker": tickers[idx], "adv_usd": adv[idx], "close": px_ff[t][idx], "n_days": n[idx]})
        # segment containing the rebalance date -> eligibility
        m = cand.merge(seg, on="ticker", how="left")
        m = m[(m["start"] <= m["rebalance_date"]) & (m["rebalance_date"] <= m["end"] + pd.Timedelta(days=max_stale * 2))]
        m = m.sort_values(["ticker", "start"]).drop_duplicates("ticker", keep="last")
        m = m[m["eligible"].fillna(False).astype(bool)].sort_values("adv_usd", ascending=False).head(top_n)
        m["rank"] = np.arange(1, len(m) + 1)
        out.append(m[["rebalance_date", "ticker", "rank", "adv_usd", "close", "n_days", "segment", "type", "name", "holder_id", "reason"]])
    if not out:
        return pd.DataFrame(columns=["rebalance_date", "ticker", "rank", "adv_usd", "close", "n_days", "segment", "type", "name", "holder_id", "reason"])
    return pd.concat(out, ignore_index=True)


def expand_daily(membership: pd.DataFrame, trading_days: Iterable[pd.Timestamp]) -> pd.DataFrame:
    """Membership on every trading day: the latest rebalance on or before that day applies (forward fill)."""
    days = pd.DatetimeIndex(sorted(pd.to_datetime(list(trading_days))))
    rds = pd.DatetimeIndex(sorted(membership["rebalance_date"].unique()))
    if len(rds) == 0:
        return pd.DataFrame(columns=["date", "ticker", "rebalance_date"])
    pos = rds.searchsorted(days, side="right") - 1
    map_df = pd.DataFrame({"date": days[pos >= 0], "rebalance_date": rds[pos[pos >= 0]]})
    return map_df.merge(membership[["rebalance_date", "ticker"]], on="rebalance_date")[["date", "ticker", "rebalance_date"]]


# --------------------------
# Summary
# --------------------------
def summarize(membership: pd.DataFrame, tickers_table: Optional[pd.DataFrame], static_list: Optional[Iterable[str]] = None) -> Dict:
    """Members per year, survivorship (share of members whose symbol is not an active share today) and,
    if a static list is given, how much of each year's membership that list would miss."""
    if membership.empty:
        return {"rebalances": 0}
    mem = membership.copy()
    mem["year"] = mem["rebalance_date"].dt.year
    per_year = mem.groupby("year")["ticker"].nunique().to_dict()
    out: Dict = {"rebalances": int(mem["rebalance_date"].nunique()), "unique_tickers": int(mem["ticker"].nunique()),
                 "members_per_year": {int(k): int(v) for k, v in per_year.items()}}
    if tickers_table is not None and len(tickers_table):
        tk = tickers_table.copy(); tk["ticker"] = tk["ticker"].astype(str).str.strip()
        active_share = set(tk.loc[tk["active"].fillna(False).astype(bool) & tk["type"].isin(["CS"]), "ticker"])
        gone = mem.groupby("year")["ticker"].apply(lambda s: 1 - len(set(s) & active_share) / len(set(s)))
        out["share_of_members_not_an_active_common_stock_today"] = {int(k): round(float(v), 3) for k, v in gone.items()}
    if static_list is not None:
        sl = set(resolve(static_list, mem["ticker"].unique())[0].values())
        miss = mem.groupby("year")["ticker"].apply(lambda s: 1 - len(set(s) & sl) / len(set(s)))
        out["share_of_members_missing_from_static_list"] = {int(k): round(float(v), 3) for k, v in miss.items()}
    # earlier segments of recycled symbols admitted without a type: review these (a fund that came back to its
    # own symbol - QQQ 2003-04, VXX - is admitted like Bear Stearns is; exclude by hand with exclude_tickers)
    unt = mem[mem["reason"] == "untyped_symbol_ok"]
    if len(unt):
        top = (unt.groupby("ticker").agg(months=("rebalance_date", "nunique"), best_rank=("rank", "min"),
                                         first=("rebalance_date", "min"), last=("rebalance_date", "max"))
               .sort_values(["months", "best_rank"], ascending=[False, True]).head(25))
        out["untyped_segments_admitted_top"] = [{"ticker": t, "months": int(r.months), "best_rank": int(r.best_rank),
                                                 "from": str(r["first"].date()), "to": str(r["last"].date())} for t, r in top.iterrows()]
        out["untyped_segments_admitted_count"] = int(unt["ticker"].nunique())
    return out
