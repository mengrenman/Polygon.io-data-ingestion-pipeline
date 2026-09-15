"""
Multi-core builder: the day batch path shards holders across processes (each process reads, attaches ids, builds
factors and writes its own slice) and the minute streaming writers run in processes instead of GIL-bound threads.
Every parallel path must reproduce the single-process reference output file for file.
"""
import gzip
import sys

import numpy as np
import pandas as pd
import pytest

import factor_builder as fb
from polygon_ingest import ingest
from test_minute_market import DIVS as MDIVS, SM as MSM, SPLITS as MSPLITS, _src as _minute_src


def _ns(ts_et: str) -> int:
    return pd.Timestamp(ts_et, tz="US/Eastern").tz_convert("UTC").value


HEADER = "ticker,volume,open,close,high,low,window_start,transactions\n"
DAYS = [d.strftime("%Y-%m-%d") for d in pd.bdate_range("2024-01-16", "2024-02-15")]
RENAME = "2024-02-05"          # FB becomes META (same holder BBGMETA); META pays $2 ex 02-08 -> reaches FB rows


def _close(t: str, d: str) -> float:
    if t == "SPL":  return 100.0 if d < "2024-01-25" else 50.0     # 2:1 split on 01-25
    if t == "DIV":  return 20.0 if d < "2024-02-01" else 19.0      # $1 dividend ex 02-01
    if t == "NOS":  return 10.0 if d < "2024-01-30" else 9.5       # not in the security master: NOFIGI id, ticker-keyed events
    if t in ("FB", "META"): return 300.0 if d < "2024-02-08" else 298.0
    if t == "OLD":  return 7.0                                     # January only, no events: delisted in the fixture
    return 5.0                                                     # ZZZ: no events


def _day_src(tmp_path):
    src = tmp_path / "src"
    for d in DAYS:
        rows = []
        for t in ("DIV", "SPL", "ZZZ", "NOS", "FB" if d < RENAME else "META", *(["OLD"] if d < "2024-02-01" else [])):
            c = _close(t, d)
            rows.append(f"{t},1000,{c},{c},{c},{c},{_ns(d + ' 00:00')},1\n")
        p = src / d[:4] / d[5:7] / f"{d}.csv.gz"
        p.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(p, "wt") as f:
            f.write(HEADER + "".join(rows))
    return src


SM = pd.DataFrame({
    "ticker": ["DIV", "SPL", "ZZZ", "FB", "META"],
    "holder_id": ["BBGDIV", "BBGSPL", "BBGZZZ", "BBGMETA", "BBGMETA"],
    "composite_figi": ["BBGDIV", "BBGSPL", "BBGZZZ", "BBGMETA", "BBGMETA"],
    "cik": [None] * 5,
    "effective_start": pd.to_datetime([None, None, None, None, RENAME]),
    "effective_end": pd.to_datetime([None, None, None, "2024-02-02", None]),
    "start_confirmed": [False, False, False, False, True],
    "end_confirmed": [False, False, False, True, False],
})
SPLITS = pd.DataFrame({"ticker": ["SPL"], "execution_date": pd.to_datetime(["2024-01-25"]), "split_from": [1], "split_to": [2]})
DIVS = pd.DataFrame({"ticker": ["DIV", "META", "NOS"], "ex_dividend_date": pd.to_datetime(["2024-02-01", "2024-02-08", "2024-01-30"]),
                     "cash_amount": [1.0, 2.0, 0.5]})


def _refdir(tmp_path):
    ref = tmp_path / "ref"
    ref.mkdir(exist_ok=True)
    SM.to_parquet(ref / "security_master.parquet", index=False)
    SPLITS.to_parquet(ref / "stock_splits.parquet", index=False)
    DIVS.to_parquet(ref / "cash_dividends.parquet", index=False)
    return ref


def _build(prices, refdir, out, workers, monkeypatch, granularity="day", extra=()):
    argv = ["factor_builder.py", "--prices", str(prices), "--refdir", str(refdir), "--granularity", granularity,
            "--outdir", str(out), "--adjust", "both", "--materialize", "ohlc", "--workers", str(workers),
            "--write-workers", "2", "--no-copy-manifest", *extra]
    monkeypatch.setattr(sys, "argv", argv)
    fb.main()
    return out


def _lake_files(out):
    return {str(p.relative_to(out)): pd.read_parquet(p) for p in sorted(out.rglob("*.parquet"))}


def _assert_same_lake(ref, par):
    a, b = _lake_files(ref), _lake_files(par)
    assert sorted(a) == sorted(b) and a, "different file sets"
    for rel in a:
        pd.testing.assert_frame_equal(a[rel], b[rel], check_exact=True, obj=rel)
    assert (ref / "_event_summary.csv").read_text() == (par / "_event_summary.csv").read_text()
    assert not (par / "_parts").exists()


@pytest.fixture
def day_lakes(tmp_path):
    src = _day_src(tmp_path)
    t, m = tmp_path / "lake_t", tmp_path / "lake_m"
    ingest.run_ingest("day", src, t, workers=1, quiet_console=True)
    ingest.run_ingest("day", src, m, workers=1, quiet_console=True, layout="market")
    return t, m, _refdir(tmp_path)


class TestShardedDayBuild:
    def test_ticker_layout_matches_single_process(self, day_lakes, tmp_path, monkeypatch):
        lake_t, _, ref = day_lakes
        one = _build(lake_t, ref, tmp_path / "adj1", 1, monkeypatch)
        par = _build(lake_t, ref, tmp_path / "adj3", 3, monkeypatch)
        _assert_same_lake(one, par)
        assert sorted(p.name for p in par.iterdir() if p.is_dir()) == ["DIV", "FB", "META", "NOS", "OLD", "SPL", "ZZZ"]

    def test_market_layout_matches_single_process(self, day_lakes, tmp_path, monkeypatch):
        _, lake_m, ref = day_lakes
        one = _build(lake_m, ref, tmp_path / "adj1", 1, monkeypatch)
        par = _build(lake_m, ref, tmp_path / "adj3", 3, monkeypatch)
        _assert_same_lake(one, par)
        jan = pd.read_parquet(par / "2024" / "01.parquet")
        assert sorted(str(p.relative_to(par)) for p in par.rglob("*.parquet")) == ["2024/01.parquet", "2024/02.parquet"]
        assert jan["ticker"].is_monotonic_increasing and jan["ticker"].nunique() == 6     # ticker-major, stitched from 3 shards

    def test_rename_pair_chains_through_one_holder(self, day_lakes, tmp_path, monkeypatch):
        lake_t, _, ref = day_lakes
        par = _build(lake_t, ref, tmp_path / "adj3", 3, monkeypatch)
        fbr = pd.concat([pd.read_parquet(p) for p in (par / "FB").rglob("*.parquet")]).sort_values("datetime")
        meta = pd.concat([pd.read_parquet(p) for p in (par / "META").rglob("*.parquet")]).sort_values("datetime")
        assert set(fbr["id"]) == set(meta["id"]) == {"BBGMETA"}
        # META's $2 dividend (ex 02-08) reaches the FB rows through the shared holder: TR factor 298/300 before it, 1 after
        assert fbr["tr_price_factor"].tolist() == pytest.approx([298.0 / 300.0] * len(fbr))
        assert meta["tr_price_factor"].iloc[-1] == 1.0 and meta["tr_price_factor"].iloc[0] == pytest.approx(298.0 / 300.0)
        summary = pd.read_csv(par / "_event_summary.csv")
        assert summary.loc[summary["id"] == "BBGMETA", "dividend_event_days"].item() == 1
        nos = pd.concat([pd.read_parquet(p) for p in (par / "NOS").rglob("*.parquet")]).sort_values("datetime")
        assert set(nos["id"]) == {"NOFIGI__NOS"} and nos["tr_price_factor"].iloc[0] == pytest.approx(9.5 / 10.0)

    def test_workers_above_holder_count_and_ticker_filter(self, day_lakes, tmp_path, monkeypatch):
        lake_t, _, ref = day_lakes
        tl = tmp_path / "tickers.json"
        tl.write_text('["SPL", "DIV"]')
        one = _build(lake_t, ref, tmp_path / "adj1", 1, monkeypatch, extra=("--tickers", str(tl)))
        par = _build(lake_t, ref, tmp_path / "adj9", 9, monkeypatch, extra=("--tickers", str(tl)))
        _assert_same_lake(one, par)
        assert sorted(p.name for p in par.iterdir() if p.is_dir()) == ["DIV", "SPL"]


    def test_date_window_that_empties_a_shard(self, day_lakes, tmp_path, monkeypatch):
        lake_t, _, ref = day_lakes
        win = ("--start", "2024-02-01", "--end", "2024-02-15")           # OLD (January only) gets a shard of its own with no rows
        one = _build(lake_t, ref, tmp_path / "adj1", 1, monkeypatch, extra=win)
        par = _build(lake_t, ref, tmp_path / "adj9", 9, monkeypatch, extra=win)
        _assert_same_lake(one, par)
        assert not (par / "OLD").exists() and (par / "FB").exists() and (par / "META").exists()


class TestHolderWithSeveralTickers:
    def test_streaming_dividend_builder_keys_prior_base_per_ticker(self):
        # units, warrants and common of one SPAC share a CIK holder id and trade on the same days: the full-market
        # minute build crashed here ("Length of values (57) does not match length of index (19)")
        days = pd.to_datetime(["2024-06-03", "2024-06-04", "2024-06-05"]).as_unit("ns")
        rows = [(t, "CIK__1", d, f"{t}/{d.date()}") for t in ("ABCU", "ABCW", "ABC") for d in days]
        id_days = pd.DataFrame(rows, columns=["ticker", "id", "event_day", "path"])
        edges = pd.DataFrame({"ticker": [r[0] for r in rows], "event_day": [r[2] for r in rows],
                              "first_close": [10.0, 10.0, 9.0, 1.0, 1.0, 1.0, 20.0, 20.0, 18.0],
                              "last_close": [10.0, 10.0, 9.0, 1.0, 1.0, 1.0, 20.0, 20.0, 18.0]})
        F = pd.DataFrame({"ticker": edges["ticker"], "event_day": edges["event_day"], "split_price_factor": 1.0, "split_volume_factor": 1.0})
        div = pd.DataFrame({"ticker": ["ABC"], "ex_date": [days[2]], "amount": [2.0], "holder_id": ["CIK__1"]})   # as _assign_event_ids keys it
        base = fb._build_daily_prior_base(id_days, use_split_base=True, F=F, edges=edges)
        G = fb._build_dividend_factors_from_days(id_days, div, base)
        assert len(G) == 9 and not G.duplicated(["ticker", "event_day"]).any()
        g = G.set_index(["ticker", "event_day"])["tr_price_factor"]
        # the holder-keyed $2 dividend is charged against each ticker's OWN prior close, never another ticker's
        assert g[("ABC", days[0])] == pytest.approx(18.0 / 20.0) and g[("ABC", days[2])] == 1.0
        assert g[("ABCU", days[0])] == pytest.approx(8.0 / 10.0)


class TestEventIndex:
    def test_lookup_rules_match_events_for_holder(self):
        t = pd.DataFrame({"ticker": ["AAA", "AAA", "BBB"], "event_id": ["BBGAAA", "BBGAAA", "NOFIGI__BBB"],
                          "ex_date": pd.to_datetime(["2024-02-01", "2024-01-01", "2024-03-01"]), "amount": [1.0, 2.0, None]})
        idx = fb._EventIndex(t, "ex_date", ["ex_date", "amount"])
        for gid, tkr in (("BBGAAA", "AAA"), ("NOFIGI__AAA", "AAA"), ("NOFIGI__BBB", "BBB"), ("BBGZZZ", "AAA"), ("NOFIGI__ZZZ", "ZZZ")):
            pd.testing.assert_frame_equal(idx.get(gid, tkr).reset_index(drop=True),
                                          fb._events_for_holder(t, gid, tkr, "ex_date", ["ex_date", "amount"]).reset_index(drop=True))
        assert idx.get("BBGAAA", "AAA")["amount"].tolist() == [2.0, 1.0]            # sorted by date
        assert idx.get("NOFIGI__AAA", "AAA")["amount"].tolist() == [2.0, 1.0]       # NOFIGI id falls back to the ticker's events
        assert idx.get("BBGZZZ", "AAA").empty and idx.get("NOFIGI__BBB", "BBB").empty   # a real holder never borrows; NaN amounts drop


class TestShardPlanning:
    def test_holders_stay_together_and_slices_are_alphabetical(self):
        weights = {"DIV": 2, "FB": 2, "META": 1, "NOS": 2, "SPL": 2, "ZZZ": 2}
        shards = fb._plan_shards(sorted(weights), SM, weights, 3)
        assert len(shards) == 3 and sorted(t for s in shards for t in s) == sorted(weights)
        fb_shard = next(s for s in shards if "FB" in s)
        assert "META" in fb_shard
        flat = [t for s in shards for t in s]
        assert flat == sorted(flat)                                  # contiguous alphabetical slices
        assert fb._plan_shards(sorted(weights), SM, weights, 50) and len(fb._plan_shards(sorted(weights), SM, weights, 50)) <= 5
        assert fb._plan_shards([], SM, {}, 4) == []

    def test_balanced_by_weight(self):
        weights = {"AAA": 100, "BBB": 1, "CCC": 1, "DDD": 1, "EEE": 100}
        shards = fb._plan_shards(sorted(weights), pd.DataFrame(columns=["ticker", "holder_id"]), weights, 2)
        assert len(shards) == 2 and [t for s in shards for t in s] == sorted(weights)
        w = [sum(weights[t] for t in s) for s in shards]
        assert abs(w[0] - w[1]) <= max(weights.values())              # never worse than one heaviest holder apart

    def test_ticker_weights_count_files_or_rows(self, day_lakes):
        lake_t, lake_m, _ = day_lakes
        wt = fb._ticker_weights(lake_t, "ticker", None)
        assert wt["DIV"] == 2 and wt["META"] == 1 and wt["OLD"] == 1 and set(wt) == {"DIV", "SPL", "ZZZ", "NOS", "FB", "META", "OLD"}
        wm = fb._ticker_weights(lake_m, "market", ["DIV", "META"])
        assert set(wm) == {"DIV", "META"} and wm["DIV"] == len(DAYS) and wm["META"] == sum(d >= RENAME for d in DAYS)


class TestStreamingInProcesses:
    def _minute_refdir(self, tmp_path):
        ref = tmp_path / "mref"
        ref.mkdir()
        MSM.to_parquet(ref / "security_master.parquet", index=False)
        MSPLITS.to_parquet(ref / "stock_splits.parquet", index=False)
        MDIVS.to_parquet(ref / "cash_dividends.parquet", index=False)
        return ref

    def test_market_minute_writer_processes_match_serial(self, tmp_path):
        lake = tmp_path / "lake"
        ingest.run_ingest("minute", _minute_src(tmp_path), lake, workers=1, quiet_console=True, layout="market")
        spl = fb._assign_event_ids(MSPLITS, MSM, ["execution_date"]); div = fb._assign_event_ids(MDIVS, MSM, ["ex_date"])
        fb.adjust_minute_market(lake, MSM, spl, div, tmp_path / "one", write_workers=1, read_workers=1, detect_gaps=False)
        fb.adjust_minute_market(lake, MSM, spl, div, tmp_path / "par", write_workers=2, read_workers=2, detect_gaps=False)
        a, b = _lake_files(tmp_path / "one"), _lake_files(tmp_path / "par")
        assert sorted(a) == sorted(b) and any(k.endswith(".idx.parquet") for k in a)
        for rel in a:
            pd.testing.assert_frame_equal(a[rel], b[rel], check_exact=True, obj=rel)

    def test_ticker_minute_writer_processes_match_serial(self, tmp_path, monkeypatch):
        lake = tmp_path / "lake_t"
        ingest.run_ingest("minute", _minute_src(tmp_path), lake, workers=1, quiet_console=True)
        ref = self._minute_refdir(tmp_path)
        one = _build(lake, ref, tmp_path / "one", 1, monkeypatch, granularity="minute",
                     extra=("--minute-stream", "--stream-read-workers", "1"))
        # --write-workers is fixed at 2 by _build: the streaming writer and the edge scan both run in 2 processes here
        par = _build(lake, ref, tmp_path / "par", 1, monkeypatch, granularity="minute",
                     extra=("--minute-stream", "--stream-read-workers", "2"))
        a, b = _lake_files(one), _lake_files(par)
        assert sorted(a) == sorted(b) and len(a) == 9
        for rel in a:
            pd.testing.assert_frame_equal(a[rel], b[rel], check_exact=True, obj=rel)
