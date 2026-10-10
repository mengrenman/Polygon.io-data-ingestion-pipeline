"""
What a crash, a partial source tree or a symbol-less vendor row must not do to a lake.

1. Sidecar and data file are one pair. The ingester used to rename a minute day file into place and only then
   write its <DD>.idx.parquet, so a crash between the two left no sidecar or a STALE one; a reader slicing the
   new file at the old positions returned another ticker's bars under the requested name. Both files now carry
   one token, and the old sidecar is gone before the new data file appears.
2. A day month file is rewritten whole from what --src holds for that month. A refresh tree starting 2025-08-14
   would have replaced day/all/2025/08.parquet with its last few sessions. Such a run is now refused before
   anything is written unless --replace-month is passed.
3. Polygon's minute files carry 365 rows with an empty ticker field in 37 sessions (2006-08-31 to 2014-01-23):
   all-zero bars that sat after the last ticker block, outside every sidecar range. They are dropped on ingest,
   and day_index refuses to write a sidecar that does not cover its file.
4. A market-layout file holds every ticker of its period and is rewritten whole from the rows the run keeps, so
   `poly bars --layout market --watch list.json --out lake/day/all` would have left each month it touched holding
   the list alone. A --watch / --only run that would rewrite a file holding other tickers is now refused before
   anything is written unless --replace-with-subset is passed.
"""
import gzip
import json
import shutil

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import factor_builder as fb
from polygon_ingest import ingest
from polygon_ingest.lake_io import read_day_index

HEADER = "ticker,volume,open,close,high,low,window_start,transactions\n"


def _ns_et(ts):
    return pd.Timestamp(ts, tz="US/Eastern").tz_convert("UTC").value


def _write_csv(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt") as f:
        f.write(HEADER + "".join(rows))


def _minute_rows(ticker, day, n, price):
    return [f"{ticker},100,{price},{price},{price},{price},{_ns_et(f'{day} 09:{30 + i:02d}')},1\n" for i in range(n)]


def _footer_token(path):
    return (pq.read_metadata(path).metadata or {}).get(ingest.SIDECAR_PAIR_KEY)


# ---------------------------------------------------------------------------
# 1. sidecar pairs
# ---------------------------------------------------------------------------
DAY = "2024-01-16"


def _minute_lake(tmp_path, name, counts):
    """A one-day market minute lake; `counts` maps ticker -> number of bars (prices 10, 20, 30, ... by ticker)."""
    src = tmp_path / f"src_{name}"
    rows = [r for k, (t, n) in enumerate(counts.items()) for r in _minute_rows(t, DAY, n, 10.0 * (k + 1))]
    _write_csv(src / "2024/01" / f"{DAY}.csv.gz", rows)
    out = tmp_path / name
    ingest.run_ingest("minute", src, out, workers=1, quiet_console=True, layout="market")
    return out / "2024/01/16.parquet"


class TestSidecarPair:
    def test_data_file_and_sidecar_share_one_token(self, tmp_path):
        f = _minute_lake(tmp_path, "lake", {"AAA": 3, "BBB": 2, "CCC": 4})
        idx = f.with_name("16.idx.parquet")
        assert _footer_token(f) and _footer_token(f) == _footer_token(idx)
        assert read_day_index(f).set_index("ticker")["row_start"].to_dict() == {"AAA": 0, "BBB": 3, "CCC": 5}
        assert not list(f.parent.glob("*.inprogress"))

    def test_a_stale_sidecar_is_refused_by_both_readers(self, tmp_path):
        # The failure the old write order allowed: the rebuilt day file is in place (AAA grew to 5 bars) but the
        # sidecar on disk is the previous one, which puts BBB at rows 3-4 - now AAA's last two bars.
        old = _minute_lake(tmp_path, "v1", {"AAA": 3, "BBB": 2, "CCC": 4})
        new = _minute_lake(tmp_path, "v2", {"AAA": 5, "BBB": 2, "CCC": 4})
        shutil.copy(new, old)
        assert read_day_index(old) is None                           # the loaders fall back to reading the file
        edges = fb._read_day_index(old).set_index("ticker")          # the adjuster recomputes from the data
        assert edges.loc["BBB", "first_close"] == 20.0 and edges.loc["AAA", "n_rows"] == 5

    def test_a_crash_between_the_renames_leaves_no_sidecar_rather_than_a_stale_one(self, tmp_path, monkeypatch):
        f = _minute_lake(tmp_path, "lake", {"AAA": 3, "BBB": 2})
        real_replace = type(f).replace

        def crash_on_sidecar(self, target):
            if self.name.endswith(".idx.parquet.inprogress"):
                raise OSError("simulated crash")
            return real_replace(self, target)

        monkeypatch.setattr(type(f), "replace", crash_on_sidecar)
        df = pd.DataFrame({"ticker": ["AAA"] * 5, "close": [1.0] * 5})
        with pytest.raises(OSError, match="simulated crash"):
            ingest._write_with_sidecar(pa.Table.from_pandas(df), f, ingest.day_index(df), 131_072)
        assert len(pd.read_parquet(f)) == 5                           # the new data file is in place ...
        assert not f.with_name("16.idx.parquet").exists()             # ... with no sidecar, not the old one
        assert read_day_index(f) is None

    def test_a_legacy_sidecar_without_a_token_is_kept_when_it_covers_the_file(self, tmp_path):
        f = _minute_lake(tmp_path, "lake", {"AAA": 3, "BBB": 2})
        idx = f.with_name("16.idx.parquet")
        pq.write_table(pa.Table.from_pandas(pd.read_parquet(f)), f)            # both footers without a token,
        pd.read_parquet(idx).to_parquet(idx, index=False)                      # as every lake built before 2026-10
        assert read_day_index(f) is not None
        pd.read_parquet(idx).iloc[:1].to_parquet(idx, index=False)             # covers 3 of 5 rows
        assert read_day_index(f) is None

    def test_the_adjuster_writes_its_sidecar_as_a_pair_too(self, tmp_path):
        f = _minute_lake(tmp_path, "lake", {"AAA": 3, "BBB": 2})
        sm = pd.DataFrame({"ticker": ["AAA", "BBB"], "composite_figi": ["BBGA", "BBGB"], "cik": [None, None]})
        no_events = pd.DataFrame({"ticker": pd.Series(dtype=str), "execution_date": pd.Series(dtype="datetime64[ns]"),
                                  "ratio": pd.Series(dtype=float)})
        spl = fb._assign_event_ids(no_events, sm, ["execution_date"])
        div = fb._assign_event_ids(no_events.rename(columns={"execution_date": "ex_date", "ratio": "amount"}), sm, ["ex_date"])
        out = tmp_path / "adj"
        fb.adjust_minute_market(f.parents[2], sm, spl, div, out, write_workers=1, read_workers=1, detect_gaps=False)
        a = out / "2024/01/16.parquet"
        assert _footer_token(a) and _footer_token(a) == _footer_token(a.with_name("16.idx.parquet"))
        assert read_day_index(a) is not None


# ---------------------------------------------------------------------------
# 2. whole-month rewrite
# ---------------------------------------------------------------------------
AUG = ["2025-08-11", "2025-08-12", "2025-08-13"]          # the lake ends here, like the real one
REFRESH = ["2025-08-14", "2025-08-15", "2025-09-02"]


def _day_tree(root, sessions, tickers=("AAA", "BBB")):
    for d in sessions:
        _write_csv(root / d[:4] / d[5:7] / f"{d}.csv.gz",
                   [f"{t},100,1,1,1,1,{_ns_et(f'{d} 00:00')},1\n" for t in tickers])
    return root


def _sessions(path):
    return sorted(pd.read_parquet(path)["datetime"].dt.strftime("%Y-%m-%d").unique())


class TestMonthRewriteGuard:
    @pytest.mark.parametrize("layout", ["market", "ticker"])
    def test_a_refresh_tree_starting_mid_month_is_refused_and_nothing_is_written(self, tmp_path, layout):
        out = tmp_path / "lake"
        ingest.run_ingest("day", _day_tree(tmp_path / "full", AUG), out, workers=1, quiet_console=True, layout=layout)
        aug = out / ("2025/08.parquet" if layout == "market" else "AAA/2025/08.parquet")
        before = aug.read_bytes()
        with pytest.raises(SystemExit, match=r"2025-08: 3 session\(s\) missing from --src \(2025-08-11 \.\. 2025-08-13\)"):
            ingest.run_ingest("day", _day_tree(tmp_path / "refresh", REFRESH), out, workers=1, quiet_console=True,
                              layout=layout)
        assert aug.read_bytes() == before
        assert not (out / ("2025/09.parquet" if layout == "market" else "AAA/2025/09.parquet")).exists()

    def test_replace_month_rewrites_it_from_src_alone(self, tmp_path):
        out = tmp_path / "lake"
        ingest.run_ingest("day", _day_tree(tmp_path / "full", AUG), out, workers=1, quiet_console=True, layout="market")
        ingest.run_ingest("day", _day_tree(tmp_path / "refresh", REFRESH), out, workers=1, quiet_console=True,
                          layout="market", replace_month=True)
        assert _sessions(out / "2025/08.parquet") == ["2025-08-14", "2025-08-15"]

    def test_a_tree_holding_every_session_of_the_month_goes_through(self, tmp_path):
        out = tmp_path / "lake"
        ingest.run_ingest("day", _day_tree(tmp_path / "full", AUG), out, workers=1, quiet_console=True, layout="market")
        ingest.run_ingest("day", _day_tree(tmp_path / "refresh", AUG + REFRESH), out, workers=1, quiet_console=True,
                          layout="market")
        assert _sessions(out / "2025/08.parquet") == AUG + REFRESH[:2]
        assert _sessions(out / "2025/09.parquet") == ["2025-09-02"]

    def test_ticker_layout_checks_only_the_symbols_the_run_writes(self, tmp_path):
        out = tmp_path / "lake"
        ingest.run_ingest("day", _day_tree(tmp_path / "full", AUG, ("AAA",)), out, workers=1, quiet_console=True)
        watch = tmp_path / "watch.json"
        watch.write_text('["BBB"]')
        ingest.run_ingest("day", _day_tree(tmp_path / "refresh", REFRESH), out, workers=1, quiet_console=True, watch=watch)
        assert _sessions(out / "AAA/2025/08.parquet") == AUG                   # untouched, and not in the way
        assert _sessions(out / "BBB/2025/08.parquet") == REFRESH[:2]

    def test_rows_stamped_into_a_month_src_has_no_file_for_do_not_replace_it(self, tmp_path, capsys):
        # a re-ingest of July whose last file carries a row dated the next day (the 2019-08-12 file carries 29
        # stamped 08-13): that row alone would have become day/all/2025/08.parquet
        out = tmp_path / "lake"
        ingest.run_ingest("day", _day_tree(tmp_path / "full", ["2025-07-31"] + AUG), out, workers=1,
                          quiet_console=True, layout="market")
        before = (out / "2025/08.parquet").read_bytes()
        july = tmp_path / "july"
        _write_csv(july / "2025/07/2025-07-31.csv.gz", [f"AAA,100,2,2,2,2,{_ns_et('2025-07-31 00:00')},1\n",
                                                        f"ZZZ,100,2,2,2,2,{_ns_et('2025-08-01 00:00')},1\n"])
        ingest.run_ingest("day", july, out, workers=1, quiet_console=True, layout="market")
        assert (out / "2025/08.parquet").read_bytes() == before
        assert pd.read_parquet(out / "2025/07.parquet")["close"].tolist() == [2.0]
        assert "left as they were" in capsys.readouterr().out
        ingest.run_ingest("day", july, out, workers=1, quiet_console=True, layout="market", replace_month=True)
        assert pd.read_parquet(out / "2025/08.parquet")["ticker"].tolist() == ["ZZZ"]

    def test_a_stray_minute_row_does_not_replace_the_session_before(self, tmp_path):
        out = tmp_path / "lake"
        src = tmp_path / "full"
        for d in ("2024-01-12", "2024-01-16"):
            _write_csv(src / "2024/01" / f"{d}.csv.gz", _minute_rows("AAA", d, 3, 5.0))
        ingest.run_ingest("minute", src, out, workers=1, quiet_console=True, layout="market")
        before = (out / "2024/01/12.parquet").read_bytes()
        later = tmp_path / "later"
        _write_csv(later / "2024/01/2024-01-16.csv.gz",
                   _minute_rows("AAA", "2024-01-16", 3, 6.0) + [f"ZZZ,1,1,1,1,1,{_ns_et('2024-01-12 19:59')},1\n"])
        ingest.run_ingest("minute", later, out, workers=1, quiet_console=True, layout="market")
        assert (out / "2024/01/12.parquet").read_bytes() == before
        assert pd.read_parquet(out / "2024/01/16.parquet")["close"].tolist() == [6.0] * 3


# ---------------------------------------------------------------------------
# 3. rows without a ticker
# ---------------------------------------------------------------------------
def _with_blank_tickers(tmp_path):
    src = tmp_path / "src"
    day = "2006-08-31"                                        # the first of the 37 sessions
    rows = _minute_rows("AAA", day, 3, 10.0) + _minute_rows("BBB", day, 2, 20.0)
    rows += [f",0,0.0,0.0,0.0,0.0,{_ns_et(f'{day} 16:{i:02d}')},{i + 1}\n" for i in range(4)]     # empty ticker field
    _write_csv(src / "2006/08" / f"{day}.csv.gz", rows)
    return src


class TestRowsWithoutATicker:
    def test_minute_ingest_drops_them_and_the_sidecar_covers_the_file(self, tmp_path, capsys):
        out = tmp_path / "lake"
        ingest.run_ingest("minute", _with_blank_tickers(tmp_path), out, workers=1, quiet_console=True, layout="market")
        f = out / "2006/08/31.parquet"
        df = pd.read_parquet(f)
        assert len(df) == 5 and df["ticker"].notna().all() and (df["close"] > 0).all()
        assert int(read_day_index(f)["n_rows"].sum()) == len(df)
        assert "4 row(s) with an empty ticker field dropped" in capsys.readouterr().out

    def test_day_and_ticker_layouts_drop_them_too(self, tmp_path):
        ingest.run_ingest("day", _with_blank_tickers(tmp_path), tmp_path / "day", workers=1, quiet_console=True, layout="market")
        assert pd.read_parquet(tmp_path / "day/2006/08.parquet")["ticker"].notna().all()
        ingest.run_ingest("minute", _with_blank_tickers(tmp_path), tmp_path / "tk", workers=1, quiet_console=True)
        assert sorted(p.name for p in (tmp_path / "tk").iterdir()) == ["AAA", "BBB"]       # no '<NA>' directory

    @pytest.mark.parametrize("day_index", [ingest.day_index, fb._day_index], ids=["ingest", "factor_builder"])
    def test_day_index_refuses_a_sidecar_that_does_not_cover_its_file(self, day_index):
        df = pd.DataFrame({"ticker": pd.array(["AAA", "AAA", None], dtype="string"), "close": [1.0, 1.0, 0.0]})
        with pytest.raises(ValueError, match="cover 2 of 3 rows"):
            day_index(df)

    def test_the_adjuster_drops_them_from_a_lake_ingested_before_the_fix(self, tmp_path):
        # build the raw file the old ingester wrote: null-ticker rows last, a legacy sidecar that skips them
        f = tmp_path / "raw/2006/08/31.parquet"
        f.parent.mkdir(parents=True)
        raw = pd.DataFrame({"ticker": pd.array(["AAA"] * 3 + [None] * 2, dtype="string"),
                            "datetime": pd.date_range("2006-08-31 09:30", periods=5, freq="min", tz="US/Eastern"),
                            "open": [10.0] * 3 + [0.0] * 2, "high": [10.0] * 3 + [0.0] * 2,
                            "low": [10.0] * 3 + [0.0] * 2, "close": [10.0] * 3 + [0.0] * 2,
                            "volume": [100] * 3 + [0] * 2, "transactions": [1, 1, 1, 7, 9]})
        raw.to_parquet(f, index=False)
        raw.iloc[:3].pipe(lambda d: d.assign(_p=range(3))).groupby("ticker").agg(
            first_close=("close", "first"), last_close=("close", "last"), n_rows=("close", "size"),
            row_start=("_p", "min"), row_end=("_p", "max")).reset_index().to_parquet(f.with_name("31.idx.parquet"), index=False)
        sm = pd.DataFrame({"ticker": ["AAA"], "composite_figi": ["BBGA"], "cik": [None]})
        no_events = pd.DataFrame({"ticker": pd.Series(dtype=str), "ex_date": pd.Series(dtype="datetime64[ns]"),
                                  "amount": pd.Series(dtype=float)})
        div = fb._assign_event_ids(no_events, sm, ["ex_date"])
        spl = fb._assign_event_ids(no_events.rename(columns={"ex_date": "execution_date", "amount": "ratio"}), sm, ["execution_date"])
        fb.adjust_minute_market(tmp_path / "raw", sm, spl, div, tmp_path / "adj", write_workers=1, read_workers=1,
                                detect_gaps=False)
        adj = pd.read_parquet(tmp_path / "adj/2006/08/31.parquet")
        assert adj["ticker"].tolist() == ["AAA"] * 3 and adj["id"].notna().all()
        assert int(read_day_index(tmp_path / "adj/2006/08/31.parquet")["n_rows"].sum()) == 3


# ---------------------------------------------------------------------------
# 4. a ticker subset written over a market lake
# ---------------------------------------------------------------------------
def _universe_lake(tmp_path, tf):
    """A market lake of AAA, BBB and CCC (day: 2025-08-11 .. 13; minute: one session) -> (src, out, period file)."""
    if tf == "minute":
        f = _minute_lake(tmp_path, "lake", {"AAA": 3, "BBB": 2, "CCC": 4})
        return tmp_path / "src_lake", tmp_path / "lake", f
    src, out = _day_tree(tmp_path / "full", AUG, ("AAA", "BBB", "CCC")), tmp_path / "lake"
    ingest.run_ingest("day", src, out, workers=1, quiet_console=True, layout="market")
    return src, out, out / "2025/08.parquet"


def _watch(tmp_path, *symbols):
    p = tmp_path / "watch.json"
    p.write_text(json.dumps(list(symbols)))
    return p


def _snapshot(out):
    return {p: p.read_bytes() for p in sorted(out.rglob("*.parquet"))}


class TestSubsetOverAMarketLake:
    @pytest.mark.parametrize("tf", ["day", "minute"])
    @pytest.mark.parametrize("select", ["watch", "only"])
    def test_a_subset_run_over_existing_files_is_refused_and_nothing_is_written(self, tmp_path, tf, select):
        src, out, f = _universe_lake(tmp_path, tf)
        before = _snapshot(out)                      # minute: the data file and its sidecar
        kw = {"watch": _watch(tmp_path, "AAA")} if select == "watch" else {"only": "AAA"}
        with pytest.raises(SystemExit) as e:
            ingest.run_ingest(tf, src, out, workers=1, quiet_console=True, layout="market", **kw)
        msg = str(e.value)
        assert f"{f.relative_to(out)}: 2 other ticker(s) (BBB, CCC)" in msg and "--replace-with-subset" in msg
        assert _snapshot(out) == before

    def test_a_minute_file_without_its_sidecar_is_read_for_its_tickers(self, tmp_path):
        src, out, f = _universe_lake(tmp_path, "minute")
        f.with_name("16.idx.parquet").unlink()                  # what a crash between the renames leaves
        with pytest.raises(SystemExit, match=r"2024/01/16\.parquet: 2 other ticker\(s\) \(AAA, CCC\)"):
            ingest.run_ingest("minute", src, out, workers=1, quiet_console=True, layout="market", only="BBB")

    def test_replace_month_does_not_override_it(self, tmp_path):
        # --replace-month accepts losing sessions --src lacks; losing tickers needs its own flag
        src, out, f = _universe_lake(tmp_path, "day")
        before = f.read_bytes()
        with pytest.raises(SystemExit, match="replace-with-subset"):
            ingest.run_ingest("day", src, out, workers=1, quiet_console=True, layout="market",
                              watch=_watch(tmp_path, "AAA"), replace_month=True)
        assert f.read_bytes() == before

    @pytest.mark.parametrize("tf", ["day", "minute"])
    def test_replace_with_subset_rewrites_the_files_with_the_selection_alone(self, tmp_path, tf, capsys):
        src, out, f = _universe_lake(tmp_path, tf)
        ingest.run_ingest(tf, src, out, workers=1, quiet_console=True, layout="market",
                          watch=_watch(tmp_path, "AAA", "CCC"), replace_with_subset=True)
        assert sorted(pd.read_parquet(f)["ticker"].unique()) == ["AAA", "CCC"]
        if tf == "minute":
            assert read_day_index(f)["ticker"].tolist() == ["AAA", "CCC"]
        assert "rewriting 1 existing market file(s) with the selected tickers alone" in capsys.readouterr().out

    @pytest.mark.parametrize("tf", ["day", "minute"])
    def test_a_fresh_output_directory_is_written_as_before(self, tmp_path, tf):
        src, _, f = _universe_lake(tmp_path, tf)
        out = tmp_path / "subset"
        ingest.run_ingest(tf, src, out, workers=1, quiet_console=True, layout="market", watch=_watch(tmp_path, "BBB"))
        assert pd.read_parquet(out / f.relative_to(tmp_path / "lake"))["ticker"].unique().tolist() == ["BBB"]

    def test_refreshing_a_subset_lake_with_its_own_list_goes_through(self, tmp_path):
        # nothing outside the selection is in the file, so nothing is lost; a wider list adds tickers
        src = _day_tree(tmp_path / "full", AUG, ("AAA", "BBB", "CCC"))
        out = tmp_path / "subset"
        ingest.run_ingest("day", src, out, workers=1, quiet_console=True, layout="market", watch=_watch(tmp_path, "AAA"))
        ingest.run_ingest("day", src, out, workers=1, quiet_console=True, layout="market", watch=_watch(tmp_path, "AAA"))
        ingest.run_ingest("day", src, out, workers=1, quiet_console=True, layout="market",
                          watch=_watch(tmp_path, "AAA", "BBB"))
        assert sorted(pd.read_parquet(out / "2025/08.parquet")["ticker"].unique()) == ["AAA", "BBB"]
        with pytest.raises(SystemExit, match=r"2025/08\.parquet: 1 other ticker\(s\) \(BBB\)"):
            ingest.run_ingest("day", src, out, workers=1, quiet_console=True, layout="market", only="AAA")

    def test_a_new_period_beside_existing_files_is_written_and_they_are_left_alone(self, tmp_path):
        src, out, aug = _universe_lake(tmp_path, "day")
        before = aug.read_bytes()
        sep = _day_tree(tmp_path / "sep", ["2025-09-02"], ("AAA", "BBB"))
        ingest.run_ingest("day", sep, out, workers=1, quiet_console=True, layout="market", watch=_watch(tmp_path, "AAA"))
        assert aug.read_bytes() == before
        assert pd.read_parquet(out / "2025/09.parquet")["ticker"].tolist() == ["AAA"]

    def test_ignore_case_is_honored_when_deciding_what_would_be_lost(self, tmp_path):
        src = _day_tree(tmp_path / "full", AUG, ("AAP", "AAp"))
        out = tmp_path / "lake"
        ingest.run_ingest("day", src, out, workers=1, quiet_console=True, layout="market")
        with pytest.raises(SystemExit, match=r"1 other ticker\(s\) \(AAp\)"):      # AAP selects the common alone
            ingest.run_ingest("day", src, out, workers=1, quiet_console=True, layout="market", watch=_watch(tmp_path, "AAP"))
        ingest.run_ingest("day", src, out, workers=1, quiet_console=True, layout="market", watch=_watch(tmp_path, "aap"),
                          ignore_case=True)
        assert sorted(pd.read_parquet(out / "2025/08.parquet")["ticker"].unique()) == ["AAP", "AAp"]
