"""
Market-layout MINUTE lakes end to end: the ingester writes <YYYY>/<MM>/<DD>.parquet with all tickers plus a
per-ticker .idx.parquet sidecar; the streaming adjuster reads each day file once, joins that day's factors by
ticker and writes the same layout; the loaders read one symbol by pruning.
"""
import gzip

import numpy as np
import pandas as pd
import pytest

import factor_builder as fb
from polygon_ingest import ingest
from polygon_ingest.lake_io import load_polygonio_lake, load_series, read_day_index


def _ns(ts_utc: str) -> int:
    return pd.Timestamp(ts_utc, tz="UTC").value


HEADER = "ticker,volume,open,close,high,low,window_start,transactions\n"
DAYS = ["2024-01-16", "2024-01-17", "2024-01-18"]      # SPL splits 2:1 on 01-18; DIV pays $1 ex 01-18


def _src(tmp_path):
    src = tmp_path / "src"
    for d in DAYS:
        rows = []
        for t, base in (("DIV", 20.0), ("SPL", 100.0), ("ZZZ", 5.0)):
            price = base
            if t == "SPL" and d == "2024-01-18": price = 50.0          # post-split
            if t == "DIV" and d == "2024-01-18": price = 19.0          # drops by the $1 dividend
            for hh, mm in ((9, 30), (9, 31), (15, 59)):                # 3 bars per day, ET
                ts = _ns(pd.Timestamp(f"{d} {hh:02d}:{mm:02d}", tz="US/Eastern").tz_convert("UTC").strftime("%Y-%m-%d %H:%M"))
                rows.append(f"{t},1000,{price},{price},{price},{price},{ts},3\n")
        p = src / d[:4] / d[5:7] / f"{d}.csv.gz"; p.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(p, "wt") as f:
            f.write(HEADER + "".join(rows))
    return src


SM = pd.DataFrame({"ticker": ["DIV", "SPL", "ZZZ"], "composite_figi": ["BBGDIV", "BBGSPL", "BBGZZZ"], "cik": [None] * 3})
SPLITS = pd.DataFrame({"ticker": ["SPL"], "execution_date": pd.to_datetime(["2024-01-18"]), "ratio": [2.0]})
DIVS = pd.DataFrame({"ticker": ["DIV"], "ex_date": pd.to_datetime(["2024-01-18"]), "amount": [1.0]})


@pytest.fixture
def lake(tmp_path):
    out = tmp_path / "lake"
    ingest.run_ingest("minute", _src(tmp_path), out, workers=1, quiet_console=True, layout="market", write_manifest=True)
    return out


class TestIngestSidecar:
    def test_day_files_and_index_sidecars(self, lake):
        files = sorted(str(p.relative_to(lake)) for p in lake.rglob("*.parquet"))
        assert files == [f"2024/01/{d}.idx.parquet" if i % 2 == 0 else f"2024/01/{d}.parquet" for d in ("16", "17", "18") for i in (0, 1)]
        idx = read_day_index(lake / "2024/01/18.parquet").set_index("ticker")
        assert idx.loc["SPL", "first_close"] == 50.0 and idx.loc["SPL", "n_rows"] == 3
        assert idx.loc["DIV", "row_start"] == 0 and idx.loc["SPL", "row_start"] == 3 and idx.loc["ZZZ", "row_end"] == 8
        man = pd.read_json(lake / "manifest_minute.json")
        assert len(man["__market__"]) == 3                                # sidecars are not manifest entries


class TestMarketStreamingAdjuster:
    def _adjusted(self, lake, tmp_path, **kw):
        spl = fb._assign_event_ids(SPLITS, SM, ["execution_date"]); div = fb._assign_event_ids(DIVS, SM, ["ex_date"])
        out = tmp_path / "adj"
        info = fb.adjust_minute_market(lake, SM, spl, div, out, write_workers=1, read_workers=1, detect_gaps=False, **kw)
        return out, info

    def test_factors_applied_per_ticker_in_the_same_layout(self, lake, tmp_path):
        out, info = self._adjusted(lake, tmp_path)
        assert info == {"files": 3, "ticker_days": 9, "tickers": 3}
        assert sorted(str(p.relative_to(out)) for p in out.rglob("*.parquet") if not p.name.endswith(".idx.parquet")) == \
            ["2024/01/16.parquet", "2024/01/17.parquet", "2024/01/18.parquet"]
        d16 = pd.read_parquet(out / "2024/01/16.parquet").set_index("ticker")
        d18 = pd.read_parquet(out / "2024/01/18.parquet").set_index("ticker")
        # SPL: 2:1 split on 01-18 -> earlier prices halved, volume doubled; on/after the split unchanged
        assert d16.loc["SPL", "close_split"].tolist() == pytest.approx([50.0] * 3) and d16.loc["SPL", "volume_split"].tolist() == pytest.approx([2000.0] * 3)
        assert d18.loc["SPL", "close_split"].tolist() == pytest.approx([50.0] * 3)
        # DIV: $1 ex on 01-18, price 20 -> 19: total return flat => close_tr before = 19
        assert d16.loc["DIV", "close_tr"].tolist() == pytest.approx([19.0] * 3) and d18.loc["DIV", "close_tr"].tolist() == pytest.approx([19.0] * 3)
        # ZZZ untouched; ids are the holders; ohlc materialized; sidecar written for the adjusted file
        assert d16.loc["ZZZ", "close_split"].tolist() == pytest.approx([5.0] * 3) and d16.loc["ZZZ", "tr_price_factor"].tolist() == [1.0] * 3
        assert set(d16["id"]) == {"BBGDIV", "BBGSPL", "BBGZZZ"} and "open_tr" in d16.columns
        assert read_day_index(out / "2024/01/16.parquet") is not None
        assert d16.reset_index()["ticker"].is_monotonic_increasing                 # ticker-major

    def test_ticker_subset_and_date_window(self, lake, tmp_path):
        out, info = self._adjusted(lake, tmp_path, tickers=["SPL"], start="2024-01-17")
        assert info["files"] == 2 and info["tickers"] == 1
        d17 = pd.read_parquet(out / "2024/01/17.parquet")
        assert d17["ticker"].unique().tolist() == ["SPL"] and d17["close_split"].tolist() == pytest.approx([50.0] * 3)
        assert not (out / "2024/01/16.parquet").exists()

    def test_edge_scan_falls_back_without_sidecars(self, lake):
        for p in lake.rglob("*.idx.parquet"):
            p.unlink()
        files = fb._iter_minute_market_files(lake)
        days_df, edges = fb._scan_day_edges_market(files, threads=1)
        assert len(days_df) == 9 and set(days_df["ticker"]) == {"DIV", "SPL", "ZZZ"}
        e = edges.set_index(["ticker", "event_day"])
        assert e.loc[("SPL", pd.Timestamp("2024-01-18")), "raw_gap"] == pytest.approx(0.5)     # split gap visible to the detector
        assert e.loc[("DIV", pd.Timestamp("2024-01-18")), "prev_last"] == 20.0

    def test_cli_path_dispatches_market_layout(self, lake, tmp_path, monkeypatch):
        refdir = tmp_path / "ref"; refdir.mkdir()
        SM.to_parquet(refdir / "security_master.parquet", index=False)
        SPLITS.to_parquet(refdir / "stock_splits.parquet", index=False)
        DIVS.rename(columns={"amount": "cash_amount", "ex_date": "ex_dividend_date"}).to_parquet(refdir / "cash_dividends.parquet", index=False)
        out = tmp_path / "adj_cli"
        monkeypatch.setattr("sys.argv", ["factor_builder.py", "--prices", str(lake), "--refdir", str(refdir), "--granularity", "minute",
                                         "--outdir", str(out), "--adjust", "both", "--materialize", "ohlc", "--minute-stream",
                                         "--write-workers", "1", "--stream-read-workers", "1", "--no-detect-split-gaps"])
        fb.main()
        d16 = pd.read_parquet(out / "2024/01/16.parquet").set_index("ticker")
        assert d16.loc["SPL", "close_split"].tolist() == pytest.approx([50.0] * 3)


class TestReaders:
    def test_load_polygonio_lake_prunes_by_ticker_and_uses_sidecars(self, lake):
        df = load_polygonio_lake(["SPL"], "2024-01-16", "2024-01-17", lake, granularity="minute")
        assert df["ticker"].unique().tolist() == ["SPL"] and len(df) == 6
        assert load_polygonio_lake(["NOPE"], "2024-01-16", "2024-01-18", lake, granularity="minute").empty

    def test_load_series_minute_market_unadjusted_and_adjusted(self, lake, tmp_path):
        spl = fb._assign_event_ids(SPLITS, SM, ["execution_date"]); div = fb._assign_event_ids(DIVS, SM, ["ex_date"])
        out = tmp_path / "adj"
        fb.adjust_minute_market(lake, SM, spl, div, out, write_workers=1, read_workers=1, detect_gaps=False)
        df = load_series(lake, out, "minute", "SPL")
        assert len(df) == 9 and df["close_sa"].notna().all()
        assert df.sort_values("datetime")["close_sa"].tolist() == pytest.approx([50.0] * 9)


class TestTickerLayoutStreamingRegressions:
    def _ticker_lake(self, tmp_path):
        out = tmp_path / "lake_t"
        ingest.run_ingest("minute", _src(tmp_path), out, workers=1, quiet_console=True)   # default ticker layout
        return out

    def test_day_after_ex_date_keeps_factor_one(self, tmp_path):
        # The old +-1 day fallback copied the previous day's TR factor onto a day whose factors were all 1.0.
        lake = self._ticker_lake(tmp_path)
        refdir = tmp_path / "ref"; refdir.mkdir()
        SM.to_parquet(refdir / "security_master.parquet", index=False)
        SPLITS.to_parquet(refdir / "stock_splits.parquet", index=False)
        DIVS.to_parquet(refdir / "cash_dividends.parquet", index=False)
        out = tmp_path / "adj_t"
        import sys
        sys.argv = ["factor_builder.py", "--prices", str(lake), "--refdir", str(refdir), "--granularity", "minute", "--outdir", str(out),
                    "--adjust", "both", "--materialize", "ohlc", "--minute-stream", "--write-workers", "1", "--stream-read-workers", "1"]
        fb.main()
        d17 = pd.read_parquet(out / "DIV/2024/01/17.parquet"); d18 = pd.read_parquet(out / "DIV/2024/01/18.parquet")
        assert d17["tr_price_factor"].tolist() == pytest.approx([19.0 / 20.0] * 3)
        assert d18["tr_price_factor"].tolist() == [1.0] * 3 and d18["close_tr"].tolist() == pytest.approx([19.0] * 3)
        # and the two layouts agree row for row
        spl = fb._assign_event_ids(SPLITS, SM, ["execution_date"]); div = fb._assign_event_ids(DIVS, SM, ["ex_date"])
        mlake = tmp_path / "lake_m"; ingest.run_ingest("minute", _src(tmp_path), mlake, workers=1, quiet_console=True, layout="market")
        fb.adjust_minute_market(mlake, SM, spl, div, tmp_path / "adj_m", write_workers=1, read_workers=1, detect_gaps=False)
        A = pd.concat([pd.read_parquet(p) for p in (out).rglob("*.parquet")]).sort_values(["ticker", "datetime"]).reset_index(drop=True)
        B = pd.concat([pd.read_parquet(p) for p in (tmp_path / "adj_m").rglob("*.parquet") if not p.name.endswith(".idx.parquet")]).sort_values(["ticker", "datetime"]).reset_index(drop=True)
        for c in ("close_split", "close_tr", "split_price_factor", "tr_price_factor"):
            assert np.allclose(A[c].to_numpy(dtype=float), B[c].to_numpy(dtype=float)), c


class TestGapDetectionOptIn:
    def test_overnight_gap_is_not_a_split_unless_asked(self, lake, tmp_path):
        # ZZZ has no splits-table entry. Make it gap 2x overnight in a copy of the lake and see what each mode does.
        import shutil
        lake2 = tmp_path / "lake2"; shutil.copytree(lake, lake2)
        f = lake2 / "2024/01/18.parquet"; df = pd.read_parquet(f)
        df.loc[df["ticker"] == "ZZZ", ["open", "high", "low", "close"]] = 10.0          # 5 -> 10 overnight
        df.to_parquet(f, index=False); (lake2 / "2024/01/18.idx.parquet").unlink()      # force the fallback scan
        spl = fb._assign_event_ids(SPLITS, SM, ["execution_date"]); div = fb._assign_event_ids(DIVS, SM, ["ex_date"])
        fb.adjust_minute_market(lake2, SM, spl, div, tmp_path / "off", write_workers=1, read_workers=1, detect_gaps=False)
        fb.adjust_minute_market(lake2, SM, spl, div, tmp_path / "on", write_workers=1, read_workers=1, detect_gaps=True)
        off = pd.read_parquet(tmp_path / "off/2024/01/16.parquet").set_index("ticker").loc["ZZZ", "split_price_factor"]
        on = pd.read_parquet(tmp_path / "on/2024/01/16.parquet").set_index("ticker").loc["ZZZ", "split_price_factor"]
        assert (off == 1.0).all()                       # default: a price jump is not a split
        assert (on != 1.0).all()                        # opt-in heuristic infers one
