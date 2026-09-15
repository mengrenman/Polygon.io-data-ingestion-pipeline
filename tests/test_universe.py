"""
Point-in-time universe: membership per rebalance date from trailing dollar volume, using only data up to
that date. Delisted names stay in while they traded; derivatives are excluded; a recycled symbol's earlier
holder is judged separately from the current record's type.
"""
import numpy as np
import pandas as pd
import pytest

from polygon_ingest import universe as U


def _lake(tmp_path):
    """Market-layout day lake, Jan-Jun 2024. Tickers:
    BIG   liquid common stock all along
    MID   common stock, liquid
    GONE  delisted after 2024-03-15 (a survivor-biased list would never contain it)
    XYZ.WS warrant (derivative symbol, no type record)
    RECY  recycled: traded to 2020 (old company, no record), gap, back from 2024-04 as an ETF
    NEW   lists 2024-05-20 (too few observations in a 63-day window)
    """
    days = pd.bdate_range("2024-01-02", "2024-06-28")
    rows = []
    def add(t, d, close, vol):
        rows.append({"datetime": pd.Timestamp(d).tz_localize("US/Eastern"), "ticker": t, "close": close, "volume": vol})
    for i, d in enumerate(days):
        add("BIG", d, 100.0, 1_000_000)
        add("MID", d, 50.0, 200_000)
        if d <= pd.Timestamp("2024-03-15"): add("GONE", d, 20.0, 500_000)
        add("XYZ.WS", d, 2.0, 5_000_000)
        if d >= pd.Timestamp("2024-04-01"): add("RECY", d, 30.0, 400_000)
        if d >= pd.Timestamp("2024-05-20"): add("NEW", d, 10.0, 9_000_000)
    for d in pd.bdate_range("2019-06-03", "2020-06-30"):      # old holder of RECY, long before
        add("RECY", d, 5.0, 300_000)
        add("BIG", d, 80.0, 900_000)
    df = pd.DataFrame(rows)
    df["date_"] = df["datetime"].dt.tz_localize(None)
    root = tmp_path / "lake"
    for (y, m), g in df.groupby([df["date_"].dt.year, df["date_"].dt.month]):
        p = root / f"{y:04d}" / f"{m:02d}.parquet"; p.parent.mkdir(parents=True, exist_ok=True)
        g.drop(columns=["date_"]).sort_values(["ticker", "datetime"]).to_parquet(p, index=False)
    return root


TICKERS = pd.DataFrame({
    "ticker": ["BIG", "MID", "GONE", "RECY", "NEW"],
    "type": ["CS", "CS", "CS", "ETF", "CS"],
    "primary_exchange": ["XNYS", "XNAS", "XNYS", "ARCX", "XNAS"],
    "name": ["Big Co", "Mid Co", "Gone Inc", "Recycled ETF", "New Co"],
    "active": [True, True, False, True, True],
    "delisted_utc": [pd.NaT, pd.NaT, pd.Timestamp("2024-03-18"), pd.NaT, pd.NaT],
    "holder_id": ["BBGBIG", "BBGMID", "BBGGONE", "BBGRECYETF", "BBGNEW"],
})


class TestSegments:
    def test_recycled_symbol_splits_at_gap_and_only_last_segment_gets_the_record_type(self, tmp_path):
        df = U.load_market_day_lake(_lake(tmp_path))
        seg = U.classify_segments(U.trading_segments(df), TICKERS)
        r = seg[seg["ticker"] == "RECY"].sort_values("segment")
        assert len(r) == 2 and r["is_last"].tolist() == [False, True]
        assert pd.isna(r.iloc[0]["type"]) and r.iloc[0]["eligible"] and r.iloc[0]["reason"] == "untyped_symbol_ok"
        assert r.iloc[1]["type"] == "ETF" and not r.iloc[1]["eligible"] and r.iloc[1]["reason"] == "type_excluded"

    def test_derivative_symbols_excluded_without_a_type(self, tmp_path):
        df = U.load_market_day_lake(_lake(tmp_path))
        seg = U.classify_segments(U.trading_segments(df), TICKERS)
        assert not seg.loc[seg["ticker"] == "XYZ.WS", "eligible"].iloc[0]
        for t in ("AACT.U", "ACHR.WS", "AACIW", "AACIU", "BACpL".upper().replace("P", ".P")):
            assert U.looks_like_derivative(t), t
        for t in ("BRK.B", "GOOGL", "NA", "AAPL", "AGM.A"):
            assert not U.looks_like_derivative(t), t

    def test_exclude_untyped_flag(self, tmp_path):
        df = U.load_market_day_lake(_lake(tmp_path))
        seg = U.classify_segments(U.trading_segments(df), TICKERS, include_untyped=False)
        assert not seg.loc[(seg["ticker"] == "RECY") & (seg["segment"] == 0), "eligible"].iloc[0]

    def test_untyped_policies_and_exclude_list(self, tmp_path):
        df = U.load_market_day_lake(_lake(tmp_path))
        segs = U.trading_segments(df)
        early = lambda seg: seg[(seg["ticker"] == "RECY") & (seg["segment"] == 0)].iloc[0]
        # RECY's current record is an ETF: 'admit' keeps the old holder's segment, the fund policy drops it
        assert early(U.classify_segments(segs, TICKERS, untyped_policy="admit"))["eligible"]
        e = early(U.classify_segments(segs, TICKERS, untyped_policy="exclude-if-current-fund"))
        assert not e["eligible"] and e["reason"] == "untyped_current_is_fund"
        e = early(U.classify_segments(segs, TICKERS, exclude_tickers=["recy"]))
        assert not e["eligible"] and e["reason"] == "excluded_by_list"
        with pytest.raises(ValueError):
            U.classify_segments(segs, TICKERS, untyped_policy="bogus")

    def test_short_gap_splits_by_default_like_bear_stearns(self):
        # BSC: last Bear Stearns print 2008-05-30, ETN under the same symbol from 2008-08-07 (69 days)
        d1 = pd.bdate_range("2008-01-02", "2008-05-30"); d2 = pd.bdate_range("2008-08-07", "2008-12-31")
        df = pd.DataFrame({"ticker": "BSC", "date": d1.append(d2)})
        seg = U.trading_segments(df)                       # default GAP_DAYS_DEFAULT = 60
        assert len(seg) == 2 and seg["is_last"].tolist() == [False, True]
        assert len(U.trading_segments(df, gap_days=365)) == 1


class TestMembership:
    def test_point_in_time_membership(self, tmp_path):
        df = U.load_market_day_lake(_lake(tmp_path))
        seg = U.classify_segments(U.trading_segments(df), TICKERS)
        mem = U.build_membership(df, seg, top_n=10, lookback=20, min_days=15, max_stale=3, freq="M", start="2024-01-01")
        by = {d.strftime("%Y-%m"): set(g["ticker"]) for d, g in mem.groupby("rebalance_date")}
        assert by["2024-01"] == {"BIG", "MID", "GONE"}              # warrant out; RECY (ETF) not trading yet; NEW not listed
        assert by["2024-02"] == {"BIG", "MID", "GONE"}
        assert "GONE" not in by["2024-03"]                           # delisted 03-15, stale by month-end -> dropped
        assert "GONE" in by["2024-02"]                               # ...but IN while it traded: no survivorship bias
        assert "RECY" not in by["2024-05"]                           # current holder is an ETF: type excluded
        assert "NEW" not in by["2024-05"] and "NEW" in by["2024-06"]  # fresh listing needs min_days observations
        jan = mem[mem["rebalance_date"] == pd.Timestamp("2024-01-31")].sort_values("rank")
        assert jan["ticker"].tolist()[:2] == ["BIG", "GONE"]          # ranked by dollar volume: 100M > 10M > 10M... BIG then GONE(10M) vs MID(10M)
        assert (jan["n_days"] >= 15).all()

    def test_top_n_and_floors(self, tmp_path):
        df = U.load_market_day_lake(_lake(tmp_path))
        seg = U.classify_segments(U.trading_segments(df), TICKERS)
        mem = U.build_membership(df, seg, top_n=1, lookback=20, min_days=15, freq="M", start="2024-01-01")
        assert (mem.groupby("rebalance_date").size() == 1).all() and (mem["ticker"] == "BIG").all()
        mem2 = U.build_membership(df, seg, top_n=10, lookback=20, min_days=15, freq="M", start="2024-01-01", min_price=60.0)
        assert set(mem2["ticker"]) == {"BIG"}

    def test_expand_daily_forward_fills_between_rebalances(self, tmp_path):
        df = U.load_market_day_lake(_lake(tmp_path))
        seg = U.classify_segments(U.trading_segments(df), TICKERS)
        mem = U.build_membership(df, seg, top_n=10, lookback=20, min_days=15, freq="M", start="2024-01-01")
        daily = U.expand_daily(mem, df["date"].unique())
        feb14 = set(daily.loc[daily["date"] == pd.Timestamp("2024-02-14"), "ticker"])
        assert feb14 == {"BIG", "MID", "GONE"}                       # January membership applies through February
        assert daily.loc[daily["date"] == pd.Timestamp("2024-02-14"), "rebalance_date"].iloc[0] == pd.Timestamp("2024-01-31")
        assert (daily["date"] < pd.Timestamp("2024-01-31")).sum() == 0   # no membership before the first rebalance

    def test_summary_reports_survivorship(self, tmp_path):
        df = U.load_market_day_lake(_lake(tmp_path))
        seg = U.classify_segments(U.trading_segments(df), TICKERS)
        mem = U.build_membership(df, seg, top_n=10, lookback=20, min_days=15, freq="M", start="2024-01-01")
        s = U.summarize(mem, TICKERS, static_list=["BIG", "MID"])
        assert s["members_per_year"] == {2024: 4}                    # BIG, MID, GONE, NEW
        assert s["share_of_members_not_an_active_common_stock_today"][2024] == pytest.approx(0.25)   # GONE
        assert s["share_of_members_missing_from_static_list"][2024] == pytest.approx(0.5)             # GONE, NEW
