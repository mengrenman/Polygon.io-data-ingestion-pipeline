"""
Point-in-time holders: which company traded under a symbol on each date, and which company each corporate
action belongs to.

The market tickers table has one record per symbol, for the company that holds it TODAY. A symbol that stopped
trading inside the lake and was reused after the lake ends therefore credits every old bar to the new company:
ABX printed Barrick Gold's bars until 2018-12-31 and is Abacus Global Management today. The lake shows one
trading segment, so the recycled-symbol segmentation (a 60-day gap) never fires. The same happens inside the
lake when the new holder takes the symbol within 60 days (GOLD: Randgold until 2018-12-28, Barrick from
2019-01-02; T: AT&T Corp until 2005-11-18, the former SBC from 2005-12-01).

Massive keeps a point-in-time view. `GET /v3/reference/tickers?date=D` returns every symbol with the company
that held it on D: name and CIK almost always, a FIGI for about 40% of historical rows. One snapshot a year
names the holder of nearly every trading segment in the lake; the switch day comes from the lake itself.

A *line* is one security followed across symbols: Barrick is ABX (2003-2018), GOLD (2019 to 2025-05-08), then
B. Pieces are linked by identity (CIK or FIGI) AND price continuity, never by CIK alone: Schering-Plough's
CIK became the new Merck's in the 2009 reverse merger, but MRK's price series is old Merck's, so a CIK link
would hand Schering-Plough old Merck's dividends.

Massive files a company's dividends and splits under one of the symbols it held: the symbol at the event date
or a later one, never an earlier one. Barrick's 2003-2018 dividends (paid as ABX) are filed under GOLD, its
symbol from 2019 to 2025; BB&T's 2011-2019 dividends under TFC. `refile_actions` moves each record to the
symbol its company held on the event date, which is how factor_builder keys it.
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

import numpy as np
import pandas as pd

from . import SM_COLUMNS, holder_id

ASOF_COLS = ["asof", "ticker", "name", "cik", "composite_figi", "share_class_figi", "type", "primary_exchange",
             "market", "locale", "currency_name"]
LINE_COLS = ["ticker", "start", "end", "line", "holder_id", "name", "cik", "composite_figi", "type",
             "open_end", "n_obs", "evidence"]
GAP_DAYS = 60            # same rule as factor_builder.RECYCLE_GAP_DAYS and universe.GAP_DAYS_DEFAULT
LINK_DAYS = 5            # a renamed security trades under its new symbol within this many sessions
CONTINUITY = 0.10        # |log(open / previous close)| below this is one security trading on
NO_GAP_HANDOFF = 0.40    # without a missed session, only a jump this large (~1.5x) separates two companies
TIGHT_CONTINUITY = 0.02  # a link on price alone (no ids to compare) needs this close a match
SNAP_DAYS = 7            # a snapshot date this close to a segment's edge still observes that segment


# --------------------------
# Pull
# --------------------------
def annual_dates(first_year: int, last_year: int, month_day: str = "06-30", extra: Sequence[str] = ()) -> List[str]:
    """One snapshot date a year, plus any `extra` (e.g. the lake's first and last days)."""
    d = [f"{y}-{month_day}" for y in range(int(first_year), int(last_year) + 1)]
    return sorted({str(pd.Timestamp(x).date()) for x in list(d) + list(extra)})


def pull_tickers_asof(out_parquet: str | Path, fetch, dates: Iterable[str], *, market: str = "stocks") -> pd.DataFrame:
    """
    Market-wide snapshot per date: GET /v3/reference/tickers?date=D, about 11 pages of 1,000 per date.
    Each date is written as soon as it completes and dates already in the table are skipped, so an
    interrupted pull resumes where it stopped.
    """
    from .bulk import _write_atomic, iter_results

    out_parquet = Path(out_parquet)
    have = pd.read_parquet(out_parquet) if out_parquet.exists() else pd.DataFrame(columns=ASOF_COLS)
    done = set(pd.to_datetime(have["asof"]).dt.strftime("%Y-%m-%d")) if len(have) else set()
    for d in dates:
        d = str(pd.Timestamp(d).date())
        if d in done:
            continue
        rows: List[Dict[str, Any]] = []
        params = {"market": market, "date": d, "limit": 1000, "order": "asc", "sort": "ticker"}
        for page in iter_results("/v3/reference/tickers", params, fetch, label=f"tickers as of {d}"):
            rows.extend({c: r.get(c) for c in ASOF_COLS[1:]} for r in page)
        df = pd.DataFrame(rows, columns=ASOF_COLS[1:])
        df["ticker"] = df["ticker"].astype(str).str.strip()     # the case is the share class: never upper-case
        df.insert(0, "asof", pd.Timestamp(d))
        have = pd.concat([have, df], ignore_index=True) if len(have) else df
        _write_atomic(have, out_parquet)
        done.add(d)
    return have


# --------------------------
# Identity
# --------------------------
_NAME_STOP = {"INC", "CORP", "CORPORATION", "CO", "COMPANY", "LTD", "LIMITED", "PLC", "SA", "NV", "AG", "LLC",
              "LP", "HOLDINGS", "HLDGS", "GROUP", "THE", "NEW", "COM", "STK", "COMMON", "STOCK", "CLASS", "CL",
              "ADS", "ADR", "SHS", "ORD", "DE", "SHARES"}


def _norm_name(name) -> str:
    if not isinstance(name, str):
        return ""
    toks = [t for t in re.sub(r"[^A-Z0-9 ]", " ", name.upper()).split() if t not in _NAME_STOP]
    return " ".join(toks[:2])


def _clean(x) -> Optional[str]:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return None
    s = str(x).strip()
    return s if s and s.lower() not in ("nan", "none", "<na>") else None


def _same_holder(a: dict, b: dict) -> bool:
    """Same company: a shared CIK or FIGI. Names decide only when one side carries neither (Polygon leaves both
    empty on some delisted rows), and a CIK/FIGI that disagrees always wins over a matching name."""
    ids_compared = False
    for k in ("cik", "composite_figi"):
        if a.get(k) and b.get(k):
            if a[k] == b[k]:
                return True
            ids_compared = True
    if ids_compared:
        return False
    na, nb = _norm_name(a.get("name")), _norm_name(b.get("name"))
    return bool(na) and na == nb


# --------------------------
# Lines
# --------------------------
def _segments(days: pd.DataFrame, gap_days: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Bars with session index `k` and segment number, and one row per (ticker, seg)."""
    d = days[["ticker", "date", "open", "close"]].copy()
    d["ticker"] = d["ticker"].astype(str)
    d["date"] = pd.to_datetime(d["date"]).dt.normalize().astype("datetime64[ns]")
    d = d.sort_values(["ticker", "date"]).drop_duplicates(["ticker", "date"]).reset_index(drop=True)
    cal = np.sort(d["date"].unique())
    d["k"] = np.searchsorted(cal, d["date"].to_numpy())
    gap = d.groupby("ticker")["date"].diff().dt.days
    d["seg"] = (gap.fillna(0) >= int(gap_days)).groupby(d["ticker"]).cumsum().astype(int)
    d["_pos"] = np.arange(len(d))
    segs = d.groupby(["ticker", "seg"], as_index=False, sort=False).agg(
        start=("date", "min"), end=("date", "max"), lo=("_pos", "min"), hi=("_pos", "max"))
    segs["hi"] += 1
    return d.drop(columns="_pos"), segs


def _observe(asof: pd.DataFrame, segs: pd.DataFrame) -> pd.DataFrame:
    """Each snapshot row -> the lake segment of its ticker that it falls in (or within SNAP_DAYS of)."""
    a = asof.copy()
    a["ticker"] = a["ticker"].astype(str).str.strip()
    a["asof"] = pd.to_datetime(a["asof"]).dt.normalize().astype("datetime64[ns]")
    m = a.merge(segs, on="ticker", how="inner")
    slack = pd.Timedelta(days=SNAP_DAYS)
    m = m[(m["asof"] >= m["start"] - slack) & (m["asof"] <= m["end"] + slack)]
    m = m.sort_values(["ticker", "seg", "asof"]).drop_duplicates(["ticker", "seg", "asof"]).reset_index(drop=True)
    # cleaned after the merge, which turns None into NaN (truthy, and `isin({nan})` matches every blank FIGI)
    for c in ("cik", "composite_figi", "name", "type"):
        m[c] = pd.Series([_clean(x) for x in m[c]] if c in m.columns else [None] * len(m), index=m.index, dtype=object)
    return m


def _switch_bar(bars: pd.DataFrame, after, upto) -> dict:
    """The session in (after, upto] where one holder handed the symbol to the next: the longest run of missed
    sessions, then the largest overnight jump."""
    b = bars.assign(missed=bars["k"].diff().fillna(1) - 1,
                    jump=np.log(bars["open"] / bars["close"].shift()).abs())
    w = b[(b["date"] > after) & (b["date"] <= upto)]
    if w.empty:
        w = b[b["date"] > after].head(1)
    w = w.fillna({"jump": 0.0}).sort_values(["missed", "jump"], ascending=False)
    r = w.iloc[0]
    return {"date": r["date"], "missed": int(r["missed"]), "jump": float(r["jump"])}


def _pieces(d: pd.DataFrame, obs: pd.DataFrame, continuity: float) -> List[dict]:
    """Cut every observed segment into pieces with one holder each. Two holders inside one segment are split at
    the switch session only if the price breaks there; a new CIK on an unbroken price series (old Merck to new
    Merck) is the same security re-papered and stays one piece, carrying the later identity."""
    out: List[dict] = []
    for (tk, seg), o in obs.groupby(["ticker", "seg"], sort=False):
        bars = d.iloc[int(o["lo"].iloc[0]):int(o["hi"].iloc[0])]
        recs = o.to_dict("records")
        runs = [[recs[0]]]
        for r in recs[1:]:
            (runs[-1].append(r) if _same_holder(runs[-1][-1], r) else runs.append([r]))
        cuts, evidence = [], []
        for prev, nxt in zip(runs[:-1], runs[1:]):
            sw = _switch_bar(bars, prev[-1]["asof"], nxt[0]["asof"])
            handoff = sw["jump"] >= continuity and (sw["missed"] >= 1 or sw["jump"] >= NO_GAP_HANDOFF)
            cuts.append(sw["date"] if handoff else None)
            evidence.append(f"{'handoff' if handoff else 'repapered'}@{sw['date'].date()}"
                            f"(missed={sw['missed']},jump={sw['jump']:.2f})")
        start, run_obs = bars["date"].iloc[0], list(runs[0])
        for i, nxt in enumerate(runs[1:]):
            if cuts[i] is None:
                run_obs += nxt
                continue
            end = bars.loc[bars["date"] < cuts[i], "date"].iloc[-1]
            out.append(_piece(tk, seg, bars, start, end, run_obs, evidence[i]))
            start, run_obs = cuts[i], list(nxt)
        out.append(_piece(tk, seg, bars, start, bars["date"].iloc[-1], run_obs, ";".join(evidence)))
    return out


def _piece(tk, seg, bars, start, end, run_obs, evidence) -> dict:
    w = bars[(bars["date"] >= start) & (bars["date"] <= end)]
    last = run_obs[-1]
    return {"ticker": tk, "seg": seg, "start": start, "end": end, "k0": int(w["k"].iloc[0]), "k1": int(w["k"].iloc[-1]),
            "first_open": float(w["open"].iloc[0]), "last_close": float(w["close"].iloc[-1]),
            "ciks": {r["cik"] for r in run_obs if r["cik"]}, "figis": {r["composite_figi"] for r in run_obs if r["composite_figi"]},
            "name": last["name"], "cik": last["cik"], "composite_figi": last["composite_figi"], "type": last.get("type"),
            "n_obs": len(run_obs), "evidence": evidence, "_ident": last,
            "after_handoff": bool(start != bars["date"].iloc[0])}


def _no_ids(ident: dict) -> bool:
    return not (ident.get("cik") or ident.get("composite_figi"))


def _link(pieces: List[dict], gap_days: int, link_days: int, continuity: float) -> List[int]:
    """Union pieces into lines. Same ticker: same holder. Across tickers: same holder AND either the price
    carries on within `link_days` sessions (a rename: ABX -> GOLD) or the symbol is a fresh listing after an
    absence of `gap_days` or more (Millicom: MICC until 2011, TIGO from 2019). When one side carries no CIK or
    FIGI to compare (Polygon has none for SBC in 2004), a piece that took over an existing symbol at a
    handoff may still link on a tight price match (SBC 24.91 -> T 25.15 on 2005-12-01): a company does not
    take another's symbol mid-history unless it was already trading somewhere."""
    parent = list(range(len(pieces)))

    def find(i):
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    def union(i, j):
        parent[find(i)] = find(j)

    by_ticker: Dict[str, List[int]] = {}
    for i, p in enumerate(pieces):
        by_ticker.setdefault(p["ticker"], []).append(i)
    first_of_ticker = {i for ix in by_ticker.values() for i in [min(ix, key=lambda j: pieces[j]["k0"])]}
    first_by_cik: Dict[str, List[int]] = {}
    for j in first_of_ticker:
        if pieces[j].get("cik"):
            first_by_cik.setdefault(pieces[j]["cik"], []).append(j)
    for ix in by_ticker.values():
        for a in ix:
            for b in ix:
                if a < b and _same_holder(pieces[a]["_ident"], pieces[b]["_ident"]):
                    union(a, b)
    starts = sorted(range(len(pieces)), key=lambda i: pieces[i]["k0"])
    k0 = np.array([pieces[i]["k0"] for i in starts])
    has_pred = set()
    for i in sorted(range(len(pieces)), key=lambda i: pieces[i]["k1"]):
        p = pieces[i]
        lo = np.searchsorted(k0, p["k1"] + 1)
        hi = np.searchsorted(k0, p["k1"] + link_days, side="right")
        for j in [starts[x] for x in range(lo, hi)]:
            q = pieces[j]
            if q["ticker"] == p["ticker"] or j in has_pred:
                continue
            gap = abs(np.log(q["first_open"] / p["last_close"]))
            same = _same_holder(p["_ident"], q["_ident"])
            blind = (_no_ids(p["_ident"]) or _no_ids(q["_ident"])) and q.get("after_handoff") and gap < TIGHT_CONTINUITY
            if (same and gap < continuity) or blind:
                union(i, j); has_pred.add(j)
                break
        else:
            # relisting: same company back on a fresh symbol after a long absence, nobody else's series on it
            for j in sorted(first_by_cik.get(p.get("cik"), []), key=lambda j: pieces[j]["k0"]):
                q = pieces[j]
                if q["ticker"] != p["ticker"] and j not in has_pred and (q["start"] - p["end"]).days >= gap_days:
                    union(i, j); has_pred.add(j)
                    break
    return [find(i) for i in range(len(pieces))]


def holder_lines(days: pd.DataFrame, asof: pd.DataFrame, market_tickers: pd.DataFrame, *,
                 gap_days: int = GAP_DAYS, link_days: int = LINK_DAYS, continuity: float = CONTINUITY,
                 lake_end=None) -> pd.DataFrame:
    """
    Every observed trading piece in the lake with the company that held it and a holder id shared by all the
    pieces of one security (one line).

    `days`: (ticker, date, open, close), one row per session. `asof`: snapshot rows in ASOF_COLS (per-ticker
    point-in-time lookups have the same shape and can be appended). `market_tickers`: today's table, which
    supplies the holder id a line keeps when it is still trading or has a record there.

    A ticker no snapshot falls on is not returned and keeps today's behavior; on an observed ticker, an
    unobserved earlier segment is returned as `NOFIGI__<T>#SEG<n>` and an unobserved last one is left to
    today's holder. Ids resolve in this
    order: today's record for the symbol the line ends on, if the line runs to the lake's end and the record is
    the same company; a record carrying one of the line's FIGIs; a record with the line's CIK on one of the
    line's own symbols (a CIK on another symbol is not used: see the module docstring on Schering-Plough);
    else a fresh id from the line's latest FIGI / CIK.
    """
    d, segs = _segments(days, gap_days)
    if asof is None or asof.empty or d.empty:
        return pd.DataFrame(columns=LINE_COLS)
    lake_end = pd.Timestamp(lake_end) if lake_end is not None else d["date"].max()
    pieces = _pieces(d, _observe(asof, segs), continuity)
    if not pieces:
        return pd.DataFrame(columns=LINE_COLS)
    roots = _link(pieces, gap_days, link_days, continuity)

    mt = market_tickers.copy()
    mt["ticker"] = mt["ticker"].astype(str).str.strip()
    for c in ("cik", "composite_figi"):
        mt[c] = [_clean(x) for x in mt[c]]
    if "holder_id" not in mt.columns:
        mt["holder_id"] = [holder_id(f, k, t) for f, k, t in zip(mt["composite_figi"], mt["cik"], mt["ticker"])]
    active = mt[mt["active"].fillna(False).astype(bool)].drop_duplicates("ticker").set_index("ticker")

    lines: Dict[int, List[int]] = {}
    for i, r in enumerate(roots):
        lines.setdefault(r, []).append(i)
    ids: Dict[int, str] = {}
    opened: Dict[int, bool] = {}
    for r, ix in lines.items():
        ix = sorted(ix, key=lambda i: pieces[i]["k0"])
        tickers = {pieces[i]["ticker"] for i in ix}
        figis = set().union(*(pieces[i]["figis"] for i in ix))
        ciks = set().union(*(pieces[i]["ciks"] for i in ix))
        last = pieces[ix[-1]]
        hid, is_open = None, False
        if (lake_end - last["end"]).days <= SNAP_DAYS and last["ticker"] in active.index:
            cur = active.loc[last["ticker"]]
            if _same_holder(last["_ident"], {"cik": cur["cik"], "composite_figi": cur["composite_figi"], "name": cur.get("name")}):
                hid, is_open = cur["holder_id"], True
        if hid is None and figis:
            hit = mt[mt["composite_figi"].isin(figis)]
            if len(hit):
                hid = hit.sort_values("ticker", key=lambda s: ~s.isin(tickers)).iloc[0]["holder_id"]
        if hid is None and ciks:
            hit = mt[mt["cik"].isin(ciks) & mt["ticker"].isin(tickers)]
            if len(hit):
                hid = hit.iloc[0]["holder_id"]
        if hid is None:
            src = max(ix, key=lambda i: (bool(pieces[i]["composite_figi"]), pieces[i]["k1"]))
            hid = holder_id(pieces[src]["composite_figi"], pieces[src]["cik"] or last["cik"], pieces[ix[0]]["ticker"])
        ids[r], opened[r] = hid, is_open
    # two different securities must never share an id (two share classes known only by one CIK)
    seen: Dict[str, int] = {}
    for r in sorted(lines, key=lambda r: min(pieces[i]["k0"] for i in lines[r])):
        if ids[r] in seen and seen[ids[r]] != r:
            ids[r] = f"{ids[r]}#{pieces[min(lines[r], key=lambda i: pieces[i]['k0'])]['ticker']}"
        seen[ids[r]] = r

    rows = []
    for i, p in enumerate(pieces):
        r = roots[i]
        last_piece = max(lines[r], key=lambda j: pieces[j]["k1"]) == i
        rows.append({"ticker": p["ticker"], "start": p["start"], "end": p["end"], "line": int(r), "holder_id": ids[r],
                     "name": p["name"], "cik": p["cik"], "composite_figi": p["composite_figi"], "type": p["type"],
                     "open_end": bool(opened[r] and last_piece), "n_obs": p["n_obs"], "evidence": p["evidence"]})
    # An unobserved segment of an observed ticker would otherwise fall to today's open-ended holder once the
    # ticker has windows (factor_builder's #SEG cut only runs on single-holder tickers): name it as the
    # unknown earlier holder, exactly as factor_builder does. An unobserved LAST segment stays with today's
    # holder, as before.
    seen_segs = {(p["ticker"], p["seg"]) for p in pieces}
    last_seg = segs.groupby("ticker")["seg"].max()
    for s in segs[segs["ticker"].isin({p["ticker"] for p in pieces})].itertuples():
        if (s.ticker, s.seg) in seen_segs or s.seg == last_seg[s.ticker]:
            continue
        rows.append({"ticker": s.ticker, "start": s.start, "end": s.end, "line": -1 - len(rows),
                     "holder_id": f"NOFIGI__{s.ticker}#SEG{int(s.seg)}", "name": None, "cik": None,
                     "composite_figi": None, "type": None, "open_end": False, "n_obs": 0, "evidence": "unobserved"})
    out = pd.DataFrame(rows, columns=LINE_COLS)
    out["line"] = out.groupby("line", sort=False).ngroup()      # stable small integers
    return out.sort_values(["ticker", "start"]).reset_index(drop=True)


def lines_to_security_master(lines: pd.DataFrame) -> pd.DataFrame:
    """Security-master rows (SM_COLUMNS) with CONFIRMED windows: one per (ticker, piece). Merge them into a
    derived security master with polygon_pullers.merge_holders; factor_builder then keys each bar and each
    corporate action by these windows. An open end (a line still trading under today's record) stays open."""
    if lines is None or lines.empty:
        return pd.DataFrame(columns=SM_COLUMNS)
    r = lines
    has_type = r["type"].notna()
    sm = pd.DataFrame({
        "ticker": r["ticker"], "holder_id": r["holder_id"], "holder_source": "asof", "name": r["name"],
        "active": r["open_end"], "type": r["type"], "type_inferred": r["type"],
        "type_source": np.where(has_type, "polygon", "unmatched"),
        "composite_figi": r["composite_figi"], "share_class_figi": None, "cik": r["cik"], "locale": None,
        "currency_name": None, "primary_exchange": None, "market": "stocks",
        "list_date": pd.NaT, "delisted_utc": pd.NaT,
        "effective_start": pd.to_datetime(r["start"]),
        "effective_end": pd.to_datetime(r["end"]).where(~r["open_end"], pd.NaT),
        "anchor_date": pd.NaT, "updated": pd.NaT,
        "start_confirmed": True, "end_confirmed": ~r["open_end"],
    })
    return sm[SM_COLUMNS].reset_index(drop=True)




# --------------------------
# Corporate actions
# --------------------------
REFILE_RULES = ("as_filed", "later_symbol", "copy_dropped", "price_test", "frequency_mismatch", "override",
                "unresolved", "unplaced")
_KIND_RANK = {"own": 0, "prior": 1, "later": 2}
OVERWHELMING = 4          # a single record overrules a decisive stream only beyond this multiple of min_llr


def _line_bars(days: pd.DataFrame, bounds: Dict[str, tuple], lines: pd.DataFrame, line: int) -> pd.DataFrame:
    """One line's sessions across all its symbols, in date order (Barrick: ABX, then GOLD, then B). `days` is
    sorted by (ticker, date) and `bounds` maps a ticker to its row range, so this is a few slices."""
    parts = []
    for p in lines[lines["line"] == line].itertuples():
        if p.ticker not in bounds:
            continue
        g = days.iloc[bounds[p.ticker][0]:bounds[p.ticker][1]]
        parts.append(g[(g["date"] >= p.start) & (g["date"] <= p.end)])
    return pd.concat(parts).sort_values("date") if parts else days.iloc[0:0]


def _overnight(bars: pd.DataFrame, date) -> Optional[tuple]:
    """(log overnight return into the line's first session on/after `date`, the close before it, robust sigma of
    the line's overnight returns over the prior year), or None when the line has no session on both sides."""
    i = int(np.searchsorted(bars["date"].to_numpy(), np.datetime64(pd.Timestamp(date), "ns")))
    if i <= 0 or i >= len(bars):
        return None
    prev_close, open_ = float(bars["close"].iloc[i - 1]), float(bars["open"].iloc[i])
    if not (prev_close > 0 and open_ > 0):
        return None
    lo = max(1, i - 250)
    on = np.log(bars["open"].to_numpy()[lo:i] / bars["close"].to_numpy()[lo - 1:i - 1])
    on = on[np.isfinite(on)]
    sigma = 1.4826 * float(np.median(np.abs(on - np.median(on)))) if len(on) >= 20 else 0.02
    return float(np.log(open_ / prev_close)), prev_close, max(sigma, 0.003)


def _evidence(r: float, e: float, sigma: float) -> float:
    """log N(r; e, s) - log N(r; 0, s): positive when the overnight move looks like the event happened."""
    return e * (2 * r - e) / (2 * sigma * sigma)


def refile_actions(actions: pd.DataFrame, lines: pd.DataFrame, *, date_col: str, kind: str,
                   days: Optional[pd.DataFrame] = None, overrides: Optional[pd.DataFrame] = None,
                   min_llr: float = 3.0) -> pd.DataFrame:
    """
    File each split or dividend under the symbol its company held on the event date.

    `actions`: a market table (`id`, `ticker`, `date_col`; `cash_amount` for dividends, `ratio` for splits;
    `frequency` groups dividend streams). `kind`: "dividend" or "split". `lines`: holder_lines(). `days`: the
    (ticker, date, open, close) sessions, for the price test. `overrides`: reviewed (id, ticker) rows that win
    over every rule; an empty ticker drops the record.

    Candidates for a record under symbol Y on date d: the line holding Y at d (`own`); the line whose last
    session on Y was just before d (`prior`: Randgold's final dividend went ex on 2019-01-02, Barrick's first
    day on GOLD); a line that takes Y later and traded under another symbol X at d (`later`: Barrick's 2010
    dividend, filed under GOLD, paid on ABX). One candidate gets the record. Between two:
      1. a record with the same date and value under X is the company's own copy: this one is dropped;
      2. the line whose ex-date overnight moves match the payout, by log-likelihood ratio, per record when one
         record is decisive, else per stream (filed symbol, the two lines, dividend frequency);
      3. a dividend whose frequency differs from every stream rule 2 gave the holder of Y goes to the later
         line (`frequency_mismatch`): under GOLD, Randgold's annual dividends are decided by price, so the
         semiannual and quarterly ones are Barrick's even where their yields are too small to test;
      4. otherwise the record stays as filed and is reported `unresolved`, for an override.
    Weaker fallbacks were tried and rejected: "the later company keeps paying this stream under Y" and "the
    later company has nothing filed under its own symbol at d" both hand Barnes Group's quarterly dividends to
    Barrick (which pays quarterly under B too, and has nothing under ABX) whenever the price test is unsure.
    Returns the table with `ticker` set to the symbol on the event date, `holder_id`, `filed_ticker` and
    `refile_rule`; dropped copies are removed, and one dividend filed under two of its company's symbols is kept
    once.
    """
    a = actions.copy().reset_index(drop=True)
    a["filed_ticker"] = a["ticker"].astype(str).str.strip()
    a["_d"] = pd.to_datetime(a[date_col]).dt.normalize().astype("datetime64[ns]")
    a["holder_id"] = pd.Series([None] * len(a), dtype=object)
    a["refile_rule"] = "unplaced"
    if lines is None or lines.empty or a.empty:
        return a.drop(columns=["_d"])
    vcol = "cash_amount" if kind == "dividend" else "ratio"
    pc = lines[["ticker", "start", "end", "line", "holder_id"]].copy()
    pc["ticker"] = pc["ticker"].astype(str)
    pc["start"] = pd.to_datetime(pc["start"]).astype("datetime64[ns]")
    pc["end"] = pd.to_datetime(pc["end"]).astype("datetime64[ns]")
    a["_row"] = np.arange(len(a))

    on = a[["_row", "filed_ticker", "_d"]].merge(pc.rename(columns={"ticker": "filed_ticker"}), on="filed_ticker")
    own = on[(on["_d"] >= on["start"]) & (on["_d"] <= on["end"])].assign(kind="own", x=lambda f: f["filed_ticker"])
    prior = on[(on["_d"] > on["end"]) & (on["_d"] <= on["end"] + pd.Timedelta(days=LINK_DAYS + 2))]
    prior = prior.assign(kind="prior", x=lambda f: f["filed_ticker"])
    later = on[on["start"] > on["_d"]][["_row", "_d", "filed_ticker", "line"]].drop_duplicates(["_row", "line"])
    later = later.merge(pc.rename(columns={"ticker": "x"}), on="line")
    later = later[(later["_d"] >= later["start"]) & (later["_d"] <= later["end"]) & (later["x"] != later["filed_ticker"])]
    later = later.assign(kind="later")
    cols = ["_row", "line", "holder_id", "x", "kind"]
    cand = pd.concat([own[cols], prior[cols], later[cols]], ignore_index=True)
    cand["_rank"] = cand["kind"].map(_KIND_RANK)
    cand = cand.sort_values(["_row", "_rank"]).drop_duplicates(["_row", "line"])
    multi = cand[cand.groupby("_row")["_row"].transform("size") > 1]
    by_row = {r: g.to_dict("records") for r, g in multi.groupby("_row")}

    filed = set(zip(a["filed_ticker"], a["_d"], a[vcol]))
    if "id" in a.columns and overrides is not None and len(overrides):
        ov = dict(zip(overrides["id"].astype(str), overrides["ticker"].fillna("").astype(str)))
    else:
        ov = {}
    d = None
    if days is not None:
        d = days[["ticker", "date", "open", "close"]].copy()
        d["ticker"] = d["ticker"].astype(str)
        d["date"] = pd.to_datetime(d["date"]).dt.normalize().astype("datetime64[ns]")
        d = d.sort_values(["ticker", "date"]).drop_duplicates(["ticker", "date"]).reset_index(drop=True)
        tk, first = np.unique(d["ticker"].to_numpy(), return_index=True)
        bounds = dict(zip(tk, zip(first, np.r_[first[1:], len(d)])))
    bars_cache: Dict[int, pd.DataFrame] = {}

    def bars(line: int) -> pd.DataFrame:
        if line not in bars_cache:
            bars_cache[line] = _line_bars(d, bounds, pc, line)
        return bars_cache[line]

    def evidence(c: dict, i: int) -> float:
        if d is None:
            return 0.0
        o = _overnight(bars(c["line"]), a.at[i, "_d"])
        if o is None:
            return 0.0
        v = float(a.at[i, vcol])
        if kind == "dividend":
            if not (0 < v < o[1]):
                return -50.0                       # a payout at or above the share price cannot be this line's
            e = float(np.log1p(-v / o[1]))
        else:
            if not v > 0:
                return 0.0
            e = float(-np.log(v))
        return _evidence(o[0], e, o[2])

    def put(i: int, c: Optional[dict], rule: str):
        a.at[i, "refile_rule"] = rule
        if c is not None:
            a.at[i, "ticker"], a.at[i, "holder_id"] = c["x"], c["holder_id"]

    n_cand = cand.groupby("_row").size()
    one = cand[cand["_row"].map(n_cand) == 1]
    if ov:
        one = one[~a.loc[one["_row"], "id"].astype(str).isin(list(ov)).to_numpy()]
    rows_one = one["_row"].to_numpy()
    a.loc[rows_one, "ticker"] = one["x"].to_numpy()
    a.loc[rows_one, "holder_id"] = one["holder_id"].to_numpy()
    a.loc[rows_one, "refile_rule"] = np.where(one["kind"].to_numpy() == "later", "later_symbol", "as_filed")
    done = set(rows_one.tolist())
    todo = sorted((set(by_row) | ({i for i, v in enumerate(a["id"].astype(str)) if v in ov} if ov else set())) - done)

    pending: List[dict] = []
    for i in todo:
        cs = by_row.get(i, [])
        if ov and str(a.at[i, "id"]) in ov:
            t = ov[str(a.at[i, "id"])]
            hit = [c for c in cs if c["x"] == t] if t else []
            if not t:
                put(i, None, "copy_dropped")
            else:
                a.at[i, "ticker"] = t
                put(i, hit[0] if hit else None, "override")
            continue
        if not cs:
            continue
        if len(cs) == 1:
            put(i, cs[0], "as_filed" if cs[0]["kind"] != "later" else "later_symbol")
            continue
        if len(cs) > 2:
            put(i, next(c for c in cs if c["kind"] == "own") if any(c["kind"] == "own" for c in cs) else None, "unresolved")
            continue
        first, second = sorted(cs, key=lambda c: c["_rank"])
        if second["kind"] == "later" and (second["x"], a.at[i, "_d"], a.at[i, vcol]) in filed:
            put(i, None, "copy_dropped")
            continue
        pending.append({"i": i, "first": first, "second": second,
                        "llr": evidence(second, i) - evidence(first, i)})

    streams: Dict[tuple, List[dict]] = {}
    for p in pending:
        f = a.at[p["i"], "frequency"] if "frequency" in a.columns else None
        key = (a.at[p["i"], "filed_ticker"], p["first"]["line"], p["second"]["line"], None if pd.isna(f) else f)
        streams.setdefault(key, []).append(p)
    undecided: List[dict] = []
    held_freqs: Dict[tuple, set] = {}          # (filed symbol, holder line) -> frequencies decided for it by price
    for ps in streams.values():
        total = sum(p["llr"] for p in ps)
        stream_decides = len(ps) > 1 and abs(total) >= min_llr
        for p in ps:
            # Ex-date moves are noisy: one record in a long stream can look decisive by chance (2 of Barnes'
            # 83 dividends under B matched Barrick's moves on ABX at min_llr). A decisive stream wins unless
            # the record's own evidence is overwhelming (Randgold's $2.69 final dividend would be a 20% drop
            # on Barrick's line: llr about -64).
            if stream_decides and not (abs(p["llr"]) >= OVERWHELMING * min_llr and np.sign(p["llr"]) != np.sign(total)):
                to = p["second"] if total > 0 else p["first"]
            elif abs(p["llr"]) >= min_llr:
                to = p["second"] if p["llr"] > 0 else p["first"]
            else:
                undecided.append(p)
                continue
            put(p["i"], to, "price_test")
            if to["kind"] != "later" and "frequency" in a.columns:
                held_freqs.setdefault((a.at[p["i"], "filed_ticker"], to["line"]), set()).add(a.at[p["i"], "frequency"])
    for p in undecided:
        f = a.at[p["i"], "frequency"] if "frequency" in a.columns else None
        held = held_freqs.get((a.at[p["i"], "filed_ticker"], p["first"]["line"]))
        if (p["second"]["kind"] == "later" and held and f is not None and pd.notna(f) and f != 0 and f not in held):
            put(p["i"], p["second"], "frequency_mismatch")
        else:
            put(p["i"], p["first"] if p["first"]["kind"] == "own" else p["second"] if p["second"]["kind"] == "own"
                else p["first"], "unresolved")

    out = a[a["refile_rule"] != "copy_dropped"]
    if kind == "dividend":
        key = ["holder_id", "_d", vcol] + (["dividend_type"] if "dividend_type" in out.columns else [])
        known = out["holder_id"].notna()
        out = pd.concat([out[~known], out[known].drop_duplicates(key, keep="first")]).sort_index()
    return out.drop(columns=["_d", "_row"]).reset_index(drop=True)
