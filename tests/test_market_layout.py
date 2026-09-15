"""
Market layout: one file per period holding every ticker (<out>/<YYYY>/<MM>.parquet for day,
/<DD>.parquet for minute), ticker-major with row-group statistics so one symbol can be pruned on read.
Also covers the bounded-memory flushing that the layout work introduced for both layouts.
"""
import gzip
import json

import pandas as pd
import pyarrow.parquet as pq
import pytest

import factor_builder as fb
from polygon_ingest import ingest
from polygon_ingest.lake_io import detect_layout, load_polygonio_lake, load_series, select_lake_files


def _ns(ts_utc: str) -> int:
    return pd.Timestamp(ts_utc, tz="UTC").value


HEADER = "ticker,volume,open,close,high,low,window_start,transactions\n"


def _write_csv(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt") as f:
        f.write(HEADER + "".join(rows))


def _day_src(tmp_path):
    """Two source months, two tickers; day bars stamped at midnight ET (05:00 UTC)."""
    src = tmp_path / "src"
    _write_csv(src / "2024/01/2024-01-16.csv.gz", [f"TOY,100,10,11,12,9,{_ns('2024-01-16 05:00')},5\n",
                                                   f"ZZZ,200,20,21,22,19,{_ns('2024-01-16 05:00')},6\n"])
    _write_csv(src / "2024/01/2024-01-17.csv.gz", [f"TOY,110,11,12,13,10,{_ns('2024-01-17 05:00')},5\n"])
    _write_csv(src / "2024/02/2024-02-01.csv.gz", [f"TOY,120,12,13,14,11,{_ns('2024-02-01 05:00')},5\n",
                                                   f"ZZZ,210,21,22,23,20,{_ns('2024-02-01 05:00')},6\n"])
    return src


class TestFlushing:
    def test_day_months_two_behind_are_final(self):
        keys = [("AAPL", 2024, 1), ("AAPL", 2024, 2), ("AAPL", 2024, 3), (2023, 12)]
        assert sorted(map(str, ingest._flushable(keys, (2024, 3), "day"))) == sorted(map(str, [("AAPL", 2024, 1), (2023, 12)]))
        assert ingest._flushable(keys, (2024, 2), "day") == [(2023, 12)]

    def test_minute_dates_before_yesterday_are_final(self):
        keys = [(2024, 1, 12), (2024, 1, 15), (2024, 1, 16)]
        assert ingest._flushable(keys, (2024, 1, 16), "minute") == [(2024, 1, 12)]   # 15 is "yesterday": kept
        assert ingest._flushable(keys, (2024, 1, 18), "minute") == [(2024, 1, 12), (2024, 1, 15), (2024, 1, 16)]

    def test_source_period(self, tmp_path):
        p = tmp_path / "2024" / "01" / "2024-01-16.csv.gz"
        assert ingest.source_period(p, "day") == (2024, 1)
        assert ingest.source_period(p, "minute") == (2024, 1, 16)


class TestMarketIngest:
    def test_day_market_layout_files_tickers_and_manifest(self, tmp_path):
        src = _day_src(tmp_path)
        out = tmp_path / "lake"
        ingest.run_ingest("day", src, out, workers=1, quiet_console=True, layout="market", write_manifest=True)

        assert sorted(str(p.relative_to(out)) for p in out.rglob("*.parquet")) == ["2024/01.parquet", "2024/02.parquet"]
        jan = pd.read_parquet(out / "2024" / "01.parquet")
        assert jan["ticker"].tolist() == ["TOY", "TOY", "ZZZ"]           # ticker-major
        assert pq.ParquetFile(out / "2024" / "01.parquet").schema_arrow.field("close").type == "double"
        man = json.loads((out / "manifest_day.json").read_text())
        assert list(man) == ["__market__"] and len(man["__market__"]) == 2
        assert detect_layout(out) == "market"

    def test_minute_market_layout_partitions_on_et_date(self, tmp_path):
        src = tmp_path / "src"
        _write_csv(src / "2024/01/2024-01-16.csv.gz", [f"TOY,100,1,1,1,1,{_ns('2024-01-16 00:30')},5\n",    # 19:30 ET Jan 15
                                                       f"TOY,200,2,2,2,2,{_ns('2024-01-16 14:30')},6\n",    # 09:30 ET Jan 16
                                                       f"ZZZ,300,3,3,3,3,{_ns('2024-01-16 14:31')},7\n"])
        out = tmp_path / "lake"
        ingest.run_ingest("minute", src, out, workers=1, quiet_console=True, layout="market")
        data_files = [p for p in out.rglob("*.parquet") if not p.name.endswith(".idx.parquet")]   # sidecars aside
        assert sorted(str(p.relative_to(out)) for p in data_files) == ["2024/01/15.parquet", "2024/01/16.parquet"]
        d16 = pd.read_parquet(out / "2024/01/16.parquet")
        assert d16["ticker"].tolist() == ["TOY", "ZZZ"]

    def test_ticker_layout_unchanged(self, tmp_path):
        src = _day_src(tmp_path)
        out = tmp_path / "lake"
        ingest.run_ingest("day", src, out, workers=1, quiet_console=True)     # default layout
        assert sorted(str(p.relative_to(out)) for p in out.rglob("*.parquet")) == [
            "TOY/2024/01.parquet", "TOY/2024/02.parquet", "ZZZ/2024/01.parquet", "ZZZ/2024/02.parquet"]
        assert detect_layout(out) == "ticker"


class TestLoaders:
    @pytest.fixture
    def market_lake(self, tmp_path):
        out = tmp_path / "lake"
        ingest.run_ingest("day", _day_src(tmp_path), out, workers=1, quiet_console=True, layout="market")
        return out

    def test_select_and_load_prune_to_requested_tickers(self, market_lake):
        files = select_lake_files(["TOY"], "2024-01-01", "2024-01-31", market_lake, granularity="day")
        assert [f.name for f in files] == ["01.parquet"]
        df = load_polygonio_lake(["TOY"], "2024-01-01", "2024-02-29", market_lake, granularity="day")
        assert df["ticker"].unique().tolist() == ["TOY"] and len(df) == 3
        both = load_polygonio_lake(["TOY", "ZZZ"], "2024-01-01", "2024-02-29", market_lake, granularity="day")
        assert len(both) == 5

    def test_load_series_mixes_market_unadjusted_with_ticker_adjusted(self, market_lake, tmp_path):
        adj = tmp_path / "lake_adj" / "TOY" / "2024"
        adj.mkdir(parents=True)
        pd.DataFrame({"datetime": pd.to_datetime(["2024-01-16 05:00", "2024-01-17 05:00"]), "ticker": "TOY",
                      "close": [11.0, 12.0], "close_split": [11.0, 12.0], "close_tr": [10.9, 12.0]}).to_parquet(adj / "01.parquet", index=False)
        df = load_series(market_lake, tmp_path / "lake_adj", "day", "TOY")
        assert df["ticker"].unique().tolist() == ["TOY"] and len(df) == 3
        assert df["close_tr"].tolist()[:2] == pytest.approx([10.9, 12.0])


class TestFactorBuilderLayouts:
    def test_read_prices_prunes_market_layout_by_ticker(self, tmp_path):
        out = tmp_path / "lake"
        ingest.run_ingest("day", _day_src(tmp_path), out, workers=1, quiet_console=True, layout="market")
        px = fb._read_prices(out, tickers=["TOY"])
        assert px["ticker"].unique().tolist() == ["TOY"] and len(px) == 3
        assert fb._detect_layout(out) == "market"
        assert len(fb._read_prices(out)) == 5                              # no filter: everything

    def test_write_partitioned_lake_market_layout(self, tmp_path):
        df = pd.DataFrame({"datetime": pd.to_datetime(["2024-01-16", "2024-01-17", "2024-02-01", "2024-01-16"]),
                           "ticker": ["ZZZ", "TOY", "TOY", "TOY"], "id": "x", "close": [1.0, 2.0, 3.0, 4.0], "volume": [1] * 4,
                           "close_split": [1.0, 2.0, 3.0, 4.0], "volume_split": [1] * 4, "close_tr": [1.0, 2.0, 3.0, 4.0]})
        fb._write_partitioned_lake(df, tmp_path / "adj", "day", write_workers=1, materialize="minimal", layout="market")
        assert sorted(str(p.relative_to(tmp_path / "adj")) for p in (tmp_path / "adj").rglob("*.parquet")) == ["2024/01.parquet", "2024/02.parquet"]
        jan = pd.read_parquet(tmp_path / "adj" / "2024" / "01.parquet")
        assert jan["ticker"].tolist() == ["TOY", "TOY", "ZZZ"] and "YYYY" not in jan.columns


class TestNaTicker:
    def test_ticker_named_na_survives_ingest(self, tmp_path):
        # "NA" (Nano Labs) is a real symbol; pandas' default NA tokens would drop it to a null ticker
        src = tmp_path / "src"
        _write_csv(src / "2024/06/2024-06-10.csv.gz", [f"NA,100,5,5,5,5,{_ns('2024-06-10 04:00')},1\n",
                                                       f"TOY,100,1,1,1,1,{_ns('2024-06-10 04:00')},1\n"])
        out = tmp_path / "lake"
        ingest.run_ingest("day", src, out, workers=1, quiet_console=True, layout="market")
        df = pd.read_parquet(out / "2024" / "06.parquet")
        assert df["ticker"].isna().sum() == 0 and df["ticker"].tolist() == ["NA", "TOY"]
