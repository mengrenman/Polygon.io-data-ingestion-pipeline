#!/usr/bin/env python
"""
Audit an adjusted day lake for symbols credited to the wrong company. No requests; a minute or two on the full
market. Run it on the current build to size the defect and on a rebuild to accept it.

Writes to --out:
  flagged_segments.csv   (A) a ticker's last trading segment ends well before the lake does while today's
                         security master marks it active: the symbol was reused after its last bar, and every
                         bar carries the new company's id. Rows, the id the adjusted lake gives them, universe
                         member-months, dividends applied, and how many arrived through the holder id from
                         another symbol (BHLB's under BBT) rather than from the segment's own symbol.
  seg_dividends.csv      (C) earlier-holder segments (#SEG ids) whose applied dividends are implausible for
                         their own prices (> 15%/yr or one ex-date > 20%): usually today's holder's pre-rename
                         history, filed under the symbol, landing on an older company (TFC#SEG0 gets BB&T's).
  handoffs.csv           (B) inside one segment, a gap of 3+ missed sessions with a > 15% price break, or a
                         missed session with a > 2x break, and no split within 5 days: candidates for a second
                         company under the same id (GOLD 2019-01-02, T 2005-12-01). Many are data holes.
  --probes (optional): point-in-time lookups (ticker, date, name, cik, composite_figi) to name the company
  behind each flagged segment and say whether it is today's holder.

See docs/symbol-reuse-after-lake-end.md.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.compute as pc
import pyarrow.dataset as ds

GAP_DAYS = 60


def _days(lake: Path) -> pd.DataFrame:
    t = ds.dataset(str(lake), format="parquet").to_table(columns=["ticker", "datetime", "open", "close", "volume"]).to_pandas()
    t["date"] = t["datetime"].dt.tz_convert("US/Eastern").dt.tz_localize(None).dt.normalize()
    t = t.sort_values(["ticker", "date", "volume"], ascending=[True, True, False]).drop_duplicates(["ticker", "date"])
    t = t.drop(columns="datetime").reset_index(drop=True)
    t["ticker"] = t["ticker"].astype(str)
    gap = t.groupby("ticker")["date"].diff().dt.days
    t["seg"] = (gap.fillna(0) >= GAP_DAYS).groupby(t["ticker"]).cumsum().astype(int)
    return t


def _adj(lake_adj: Path, tickers=None, ids_like: str | None = None) -> pd.DataFrame:
    f = None
    if tickers is not None:
        f = ds.field("ticker").isin(sorted(tickers))
    if ids_like is not None:
        f = pc.match_substring(ds.field("id"), ids_like)
    a = ds.dataset(str(lake_adj), format="parquet").to_table(
        columns=["ticker", "id", "datetime", "close", "split_price_factor", "tr_price_factor"], filter=f).to_pandas()
    a["date"] = pd.to_datetime(a["datetime"]).dt.tz_localize("UTC").dt.tz_convert("US/Eastern").dt.tz_localize(None).dt.normalize()
    return a.sort_values(["ticker", "date"]).reset_index(drop=True)


def _steps(g: pd.DataFrame) -> pd.DataFrame:
    """Ex-dates applied to a series: g_t = tr_{t-1}/tr_t < 1 on the session a dividend went ex."""
    tr = g["tr_price_factor"].to_numpy()
    step = np.r_[1.0, tr[:-1] / tr[1:]]
    return g.assign(lg=np.log(step))[np.abs(step - 1) > 1e-9]


def _own_days(g: pd.DataFrame, dates: pd.Series) -> set:
    """Sessions of `g` that an event dated `dates` lands on (forward, like the adjuster)."""
    dd = g["date"].to_numpy()
    k = np.searchsorted(dd, dates.to_numpy().astype("datetime64[ns]"))
    return set(dd[k[k < len(dd)]])


def flagged(days, sm, lake_adj, members, divs, splits, cutoff) -> pd.DataFrame:
    seg = days.groupby(["ticker", "seg"]).agg(start=("date", "min"), end=("date", "max"), rows=("date", "size")).reset_index()
    last = seg[seg["seg"] == seg.groupby("ticker")["seg"].transform("max")]
    act = sm[sm["active"].fillna(False).astype(bool)].groupby("ticker").agg(master_id=("holder_id", "first"), master_name=("name", "first"))
    f = last.join(act, on="ticker", how="inner")
    f = f[f["end"] < cutoff].copy()
    mm = members.merge(f[["ticker", "seg"]].rename(columns={"seg": "segment"}), on=["ticker", "segment"])
    f = f.join(mm.groupby("ticker").size().rename("member_months"), on="ticker").fillna({"member_months": 0})
    a = _adj(lake_adj, tickers=set(f["ticker"]))
    own = divs[divs["ticker"].isin(set(f["ticker"]))]
    out = []
    for r in f.itertuples():
        g = a[(a["ticker"] == r.ticker) & (a["date"] >= r.start) & (a["date"] <= r.end)]
        ex = _steps(g) if len(g) > 1 else g.iloc[0:0].assign(lg=[])
        o = own[(own["ticker"] == r.ticker) & own["ex_dividend_date"].between(r.start, r.end)]
        fx = ex[~ex["date"].isin(_own_days(g, o["ex_dividend_date"]))]
        # a split applied with no record under the segment's own symbol came through the holder id: GOLD's
        # 2022-06-07 2:1 is A-Mark's (Gold.com's former symbol AMRK), +102% in close_tr on Barrick's bars
        sp = g["split_price_factor"].to_numpy() if len(g) > 1 else np.ones(1)
        sx = g.iloc[1:][np.abs(sp[1:] / sp[:-1] - 1) > 1e-9] if len(g) > 1 else g.iloc[0:0]
        so = splits[(splits["ticker"] == r.ticker) & pd.to_datetime(splits["execution_date"]).between(r.start, r.end)]
        sfx = sx[~sx["date"].isin(_own_days(g, pd.to_datetime(so["execution_date"])))]
        out.append({"ticker": r.ticker, "start": r.start.date(), "end": r.end.date(), "rows": r.rows,
                    "adj_rows": len(g), "adj_id": ",".join(g["id"].unique()), "master_id": r.master_id,
                    "master_name": r.master_name, "member_months": int(r.member_months),
                    "n_div_applied": len(ex), "div_applied_logpct": round(100 * ex["lg"].sum(), 2),
                    "n_div_own_symbol": len(o), "n_div_via_holder_id": len(fx),
                    "div_via_holder_id_logpct": round(100 * fx["lg"].sum(), 2),
                    "n_split_days": len(sx), "n_split_via_holder_id": len(sfx),
                    "split_via_holder_id_dates": ",".join(str(x.date()) for x in sfx["date"])})
    return pd.DataFrame(out)


def seg_dividends(lake_adj) -> pd.DataFrame:
    t = _adj(lake_adj, ids_like="#SEG").sort_values(["id", "date"])
    t["step"] = t.groupby("id")["tr_price_factor"].shift() / t["tr_price_factor"]
    ex = t[t["step"].notna() & ((t["step"] - 1).abs() > 1e-9)]
    s = t.groupby("id").agg(ticker=("ticker", "first"), start=("date", "min"), end=("date", "max"), rows=("date", "size"),
                            median_close=("close", "median"))
    e = ex.groupby("id").agg(n_ex=("step", "size"), cum_logpct=("step", lambda x: 100 * np.log(x.clip(lower=1e-12)).sum()),
                             max_single_pct=("step", lambda x: 100 * (1 - x.min())))
    s = s.join(e).fillna({"n_ex": 0, "cum_logpct": 0.0, "max_single_pct": 0.0})
    s["per_yr_logpct"] = s["cum_logpct"] / ((s["end"] - s["start"]).dt.days / 365.25).clip(lower=1 / 252)
    s["implausible"] = (s["per_yr_logpct"] < -15) | (s["max_single_pct"] > 20)
    s = s.reset_index()
    num = s.select_dtypes("number").columns
    s[num] = s[num].round(2)
    return s


def handoffs(days, splits) -> pd.DataFrame:
    d = days.copy()
    cal = np.sort(d["date"].unique())
    d["k"] = np.searchsorted(cal, d["date"].to_numpy())
    g = d.groupby("ticker")
    d["prev_date"], d["prev_close"] = g["date"].shift(), g["close"].shift()
    d["missed"] = d["k"] - g["k"].shift() - 1
    d["jump"] = np.log(d["open"] / d["prev_close"])
    c = d[d["prev_date"].notna() & ((d["date"] - d["prev_date"]).dt.days < GAP_DAYS) & (d["prev_close"] >= 1) & (d["open"] >= 1)]
    c = c[((c["missed"] >= 3) & (c["jump"].abs() > 0.15)) | ((c["missed"] >= 1) & (c["jump"].abs() > np.log(2)))]
    s = splits[["ticker", "execution_date"]].merge(c[["ticker", "date"]], on="ticker")
    near = s[(pd.to_datetime(s["execution_date"]) - s["date"]).abs().dt.days <= 5][["ticker", "date"]].drop_duplicates()
    c = c.merge(near.assign(_split=True), on=["ticker", "date"], how="left")
    c = c[c["_split"].isna()]
    c = c[["ticker", "prev_date", "date", "missed", "prev_close", "open", "jump"]].reset_index(drop=True)
    return c.round({"prev_close": 4, "open": 4, "jump": 4})


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--lake", type=Path, required=True, help="market-layout unadjusted DAY lake")
    ap.add_argument("--lake-adj", type=Path, required=True, help="market-layout adjusted DAY lake")
    ap.add_argument("--refdata", type=Path, required=True, help="derived refdata dir (security_master.parquet)")
    ap.add_argument("--market-dir", type=Path, required=True, help="refdata/_market")
    ap.add_argument("--universe", type=Path, default=None, help="universe dir with membership.parquet")
    ap.add_argument("--probes", type=Path, default=None, help="point-in-time lookups (ticker, date, name, cik, composite_figi)")
    ap.add_argument("--cutoff", default="2025-06-30", help="a last segment ending before this date is flagged")
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)

    days = _days(args.lake)
    sm = pd.read_parquet(args.refdata / "security_master.parquet")
    members = (pd.read_parquet(args.universe / "membership.parquet") if args.universe
               else pd.DataFrame(columns=["rebalance_date", "ticker", "segment"]))
    divs = pd.read_parquet(args.market_dir / "market_dividends.parquet", columns=["ticker", "ex_dividend_date", "cash_amount"])
    divs = divs[divs["cash_amount"] > 0]
    splits = pd.read_parquet(args.market_dir / "market_splits.parquet")
    f = flagged(days, sm, args.lake_adj, members, divs, splits, pd.Timestamp(args.cutoff))
    if args.probes:
        p = pd.read_parquet(args.probes).sort_values("date")
        p = p[p["found"]] if "found" in p.columns else p
        late = p.groupby("ticker").agg(true_name=("name", "last"), true_cik=("cik", "last"), true_figi=("composite_figi", "last"))
        f = f.join(late, on="ticker")
        sm_ids = sm.set_index("ticker")[["cik", "composite_figi"]]
        same = []
        for r in f.itertuples():
            m = sm_ids.loc[[r.ticker]] if r.ticker in sm_ids.index else None
            same.append(None if pd.isna(r.true_name) or m is None else
                        bool(((m["cik"] == r.true_cik) & m["cik"].notna()).any() or ((m["composite_figi"] == r.true_figi) & m["composite_figi"].notna()).any()))
        f["master_is_true_holder"] = same
    f.to_csv(args.out / "flagged_segments.csv", index=False)
    s = seg_dividends(args.lake_adj)
    s.to_csv(args.out / "seg_dividends.csv", index=False)
    h = handoffs(days, splits)
    h.to_csv(args.out / "handoffs.csv", index=False)
    summary = {
        "tickers": int(days["ticker"].nunique()), "rows": int(len(days)),
        "A_flagged_segments": int(len(f)), "A_rows": int(f["rows"].sum()), "A_adj_rows": int(f["adj_rows"].sum()),
        "A_adj_rows_with_master_id": int((f["adj_id"] == f["master_id"]).mul(f["adj_rows"]).sum()),
        "A_member_segments": int((f["member_months"] > 0).sum()), "A_member_months": int(f["member_months"].sum()),
        "A_member_months_total": int(len(members)),
        "A_segments_with_dividends_via_holder_id": int((f["n_div_via_holder_id"] > 0).sum()),
        "A_segments_with_splits_via_holder_id": int((f["n_split_via_holder_id"] > 0).sum()),
        "C_seg_ids": int(len(s)), "C_seg_rows": int(s["rows"].sum()), "C_implausible": int(s["implausible"].sum()),
        "B_handoff_candidates": int(len(h)),
    }
    (args.out / "summary.json").write_text(json.dumps(summary, indent=1))
    print(json.dumps(summary, indent=1))


if __name__ == "__main__":
    main()
