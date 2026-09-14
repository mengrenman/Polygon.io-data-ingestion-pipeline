"""
Regression tests for the adjustment math in legacy_scripts/factor_builder.py and the
ET-trading-date partitioning / float64 storage in polygon_ingest.ingest.

The total-return cases are "known answer" tests: a $1 dividend goes ex and the price drops by
exactly $1, so the holder's total return across the ex-date must be 0%. The original code
produced -1.99% here (factor direction inverted) and -10.9% once a later 10:1 split was in
play (raw dividend divided by a split-adjusted base).
"""
from __future__ import annotations

import gzip

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import factor_builder as fb  # legacy_scripts/ is on pythonpath via pyproject
from polygon_ingest.ingest import run_ingest

# Pinned to ns: production event-day keys are ns (see factor_builder._trading_day / _prep_*), and
# pandas>=3 would otherwise build these fixtures at us resolution.
DAYS = pd.DatetimeIndex(pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"])).as_unit("ns")
EX = pd.Timestamp("2024-01-03").as_unit("ns")
GID = "NOFIGI__TOY"


def _px(close, close_split=None, spf=None) -> pd.DataFrame:
    df = pd.DataFrame({"datetime": DAYS, "event_day": DAYS, "ticker": "TOY", "close": close})
    if close_split is not None:
        df["close_split"] = close_split
    if spf is not None:
        df["split_price_factor"] = spf
    return df


def _divs(amount=1.0, ex_dates=(EX,)) -> pd.DataFrame:
    n = len(ex_dates)
    return pd.DataFrame({"event_id": [GID] * n, "ticker": ["TOY"] * n,
                         "ex_date": list(ex_dates), "amount": [amount] * n})


def _ret_across_ex(close_tr) -> float:
    return float(close_tr[1] / close_tr[0] - 1.0)


# ---------------------------------------------------------------------------
# Batch path: _dividend_factors_for_id_worker
# ---------------------------------------------------------------------------
class TestBatchDividendFactors:
    def test_zero_total_return_across_ex_date_raw_base(self):
        fb._DIVS_TABLE = _divs(1.0)
        px = _px([100.0, 99.0, 99.0])
        T, _ = fb._dividend_factors_for_id_worker((GID, px, False))
        close_tr = px["close"].to_numpy() * T["tr_price_factor"].to_numpy()

        assert _ret_across_ex(close_tr) == pytest.approx(0.0, abs=1e-12)
        # anchored to the latest price: factor is 1 after the last ex-date ...
        assert T["tr_price_factor"].iloc[-1] == pytest.approx(1.0)
        # ... and EARLIER prices are scaled DOWN (dividends reinvested make the past cheaper)
        assert T["tr_price_factor"].iloc[0] == pytest.approx(0.99)

    def test_dividend_is_split_scaled_when_base_is_split_adjusted(self):
        # A later 10:1 split makes close_split = close/10 and split_price_factor = 0.1 on these
        # days. Polygon reports the raw $1.00 dividend; in split-adjusted units it is $0.10.
        fb._DIVS_TABLE = _divs(1.0)
        px = _px([100.0, 99.0, 99.0], close_split=[10.0, 9.9, 9.9], spf=[0.1, 0.1, 0.1])
        T, stats = fb._dividend_factors_for_id_worker((GID, px, True))
        close_tr = px["close_split"].to_numpy() * T["tr_price_factor"].to_numpy()

        assert stats["base"] == "split"
        assert _ret_across_ex(close_tr) == pytest.approx(0.0, abs=1e-12)

    def test_two_dividends_compound(self):
        # $1 ex on day 2 and again on day 3; price drops $1 each time -> TR flat throughout.
        fb._DIVS_TABLE = _divs(1.0, ex_dates=(EX, pd.Timestamp("2024-01-04").as_unit("ns")))
        px = _px([100.0, 99.0, 98.0])
        T, _ = fb._dividend_factors_for_id_worker((GID, px, False))
        close_tr = px["close"].to_numpy() * T["tr_price_factor"].to_numpy()

        assert np.allclose(np.diff(close_tr), 0.0, atol=1e-12)

    def test_no_dividends_is_identity(self):
        fb._DIVS_TABLE = _divs(1.0).iloc[0:0]
        T, stats = fb._dividend_factors_for_id_worker((GID, _px([100.0, 101.0, 102.0]), False))

        assert (T["tr_price_factor"] == 1.0).all()
        assert stats["event_days"] == 0


# ---------------------------------------------------------------------------
# Streaming (minute) path: _build_daily_prior_base + _build_dividend_factors_from_days
# ---------------------------------------------------------------------------
class TestStreamingDividendFactors:
    @staticmethod
    def _inputs(last_close, spf):
        id_days = pd.DataFrame({"id": GID, "ticker": "TOY", "event_day": DAYS, "path": ["a", "b", "c"]})
        edges = pd.DataFrame({"ticker": "TOY", "event_day": DAYS,
                              "first_close": last_close, "last_close": last_close})
        F = pd.DataFrame({"ticker": "TOY", "event_day": DAYS,
                          "split_price_factor": spf, "split_volume_factor": 1.0 / spf})
        return id_days, edges, F

    def test_zero_total_return_with_split_base(self):
        id_days, edges, F = self._inputs([100.0, 99.0, 99.0], 0.1)
        base = fb._build_daily_prior_base(id_days, use_split_base=True, F=F, edges=edges)
        G = fb._build_dividend_factors_from_days(id_days, _divs(1.0), base).sort_values("event_day")
        close_split = np.array([10.0, 9.9, 9.9])
        close_tr = close_split * G["tr_price_factor"].to_numpy()

        assert _ret_across_ex(close_tr) == pytest.approx(0.0, abs=1e-12)
        assert G["tr_price_factor"].iloc[-1] == pytest.approx(1.0)

    def test_matches_batch_path(self):
        id_days, edges, F = self._inputs([100.0, 99.0, 99.0], 0.1)
        base = fb._build_daily_prior_base(id_days, use_split_base=True, F=F, edges=edges)
        G_stream = fb._build_dividend_factors_from_days(id_days, _divs(1.0), base).sort_values("event_day")

        fb._DIVS_TABLE = _divs(1.0)
        px = _px([100.0, 99.0, 99.0], close_split=[10.0, 9.9, 9.9], spf=[0.1, 0.1, 0.1])
        G_batch, _ = fb._dividend_factors_for_id_worker((GID, px, True))

        np.testing.assert_allclose(G_stream["tr_price_factor"].to_numpy(),
                                   G_batch["tr_price_factor"].to_numpy(), rtol=1e-12)


# ---------------------------------------------------------------------------
# Split factors (unchanged behaviour, guarded)
# ---------------------------------------------------------------------------
class TestSplitFactors:
    def test_two_for_one_split_halves_earlier_prices_and_doubles_volume(self):
        fb._SPLITS_TABLE = pd.DataFrame({"event_id": [GID], "ticker": ["TOY"],
                                         "execution_date": [EX], "ratio": [2.0], "composite_figi": [pd.NA]})
        days = pd.DataFrame({"ticker": "TOY", "event_day": DAYS})
        out, _ = fb._split_factors_for_id_worker((GID, days))

        assert out["split_price_factor"].tolist() == pytest.approx([0.5, 1.0, 1.0])
        assert out["split_volume_factor"].tolist() == pytest.approx([2.0, 1.0, 1.0])


# ---------------------------------------------------------------------------
# Trading-date alignment
# ---------------------------------------------------------------------------
class TestTradingDay:
    def test_evening_utc_maps_to_previous_eastern_date(self):
        # 2024-01-16 00:30 UTC = 2024-01-15 19:30 EST  -> after-hours bar of the Jan 15 session
        # 2024-01-16 14:30 UTC = 2024-01-16 09:30 EST  -> Jan 16 open
        # 2024-07-16 00:30 UTC = 2024-07-15 20:30 EDT  -> Jul 15
        s = pd.Series(pd.to_datetime(["2024-01-16 00:30", "2024-01-16 14:30", "2024-07-16 00:30"]))
        out = fb._trading_day(s)

        assert out.dt.tz is None
        assert out.tolist() == [pd.Timestamp("2024-01-15"), pd.Timestamp("2024-01-16"), pd.Timestamp("2024-07-15")]

    def test_tz_aware_input_is_converted_not_stripped(self):
        s = pd.Series(pd.to_datetime(["2024-01-15 19:30"])).dt.tz_localize("US/Eastern")
        assert fb._to_naive_utc(s).tolist() == [pd.Timestamp("2024-01-16 00:30")]
        assert fb._trading_day(s).tolist() == [pd.Timestamp("2024-01-15")]

    def test_day_bars_unaffected(self):
        # Polygon day aggregates are stamped at midnight ET (04:00/05:00 UTC): same date either way.
        s = pd.Series(pd.to_datetime(["2024-01-16 05:00", "2024-07-16 04:00"]))
        assert fb._trading_day(s).tolist() == [pd.Timestamp("2024-01-16"), pd.Timestamp("2024-07-16")]


# ---------------------------------------------------------------------------
# Ingest: partition on ET trading date, store float64
# ---------------------------------------------------------------------------
class TestIngestPartitioning:
    def test_minute_bars_partition_on_eastern_date_and_store_float64(self, tmp_path):
        src = tmp_path / "src" / "2024" / "01"
        src.mkdir(parents=True)
        ts_late = pd.Timestamp("2024-01-16 00:30", tz="UTC").value   # 19:30 ET Jan 15
        ts_open = pd.Timestamp("2024-01-16 14:30", tz="UTC").value   # 09:30 ET Jan 16
        csv = ("ticker,volume,open,close,high,low,window_start,transactions\n"
               f"TOY,100,123456.78,123456.79,123456.80,123456.77,{ts_late},5\n"
               f"TOY,200,10.0,10.1,10.2,9.9,{ts_open},6\n")
        with gzip.open(src / "2024-01-16.csv.gz", "wt") as f:
            f.write(csv)

        out = tmp_path / "lake"
        run_ingest("minute", tmp_path / "src", out, workers=1, quiet_console=True)

        f15 = out / "TOY" / "2024" / "01" / "15.parquet"
        f16 = out / "TOY" / "2024" / "01" / "16.parquet"
        assert f15.exists(), "19:30 ET bar must land in the Jan 15 file, not the UTC Jan 16 file"
        assert f16.exists()

        t = pq.read_table(f15)
        assert t.schema.field("close").type == pa.float64()
        df = t.to_pandas()
        assert len(df) == 1
        assert df["close"].iloc[0] == 123456.79          # float32 would give 123456.7890625
        assert str(df["datetime"].dt.tz) in ("US/Eastern", "America/New_York")
        assert {"yr_et", "mo_et", "day_et"} <= set(df.columns)


# ---------------------------------------------------------------------------
# Batch builders with workers=1 (inline path). Before the fix, a bare assignment inside the
# builder created a local instead of setting the module global, so the worker raised
# "Worker missing splits table" unless run through the multiprocess initializer.
# ---------------------------------------------------------------------------
class TestInlineBuilders:
    def test_single_worker_builders_set_module_tables(self):
        px = _px([100.0, 99.0, 99.0]).assign(id=GID)
        splits = pd.DataFrame({"ticker": ["TOY"], "execution_date": [EX], "ratio": [2.0]})
        divs = pd.DataFrame({"ticker": ["TOY"], "ex_date": [EX], "amount": [1.0]})
        stats: dict = {}

        F = fb._build_split_factors(px, splits, stats=stats, workers=1)
        assert F["split_price_factor"].tolist() == pytest.approx([0.5, 1.0, 1.0])

        G = fb._build_dividend_factors(px, divs, use_split_base=False, stats=stats, workers=1)
        assert G["tr_price_factor"].tolist() == pytest.approx([0.99, 1.0, 1.0])


# ---------------------------------------------------------------------------
# Robustness: mixed datetime resolutions (pandas>=3 reads parquet dates back as us while lake
# timestamps are ns; merge_asof raises MergeError on mismatched keys) and a security master
# without an effective window (the repo's own puller never writes one).
# ---------------------------------------------------------------------------
class TestRobustness:
    def test_split_and_dividend_prep_pin_event_dates_to_ns(self):
        us_dates = pd.Series([EX]).dt.as_unit("us")
        S = fb._prep_splits(pd.DataFrame({"ticker": ["TOY"], "execution_date": us_dates, "ratio": [2.0]}))
        D = fb._prep_dividends(pd.DataFrame({"ticker": ["TOY"], "ex_dividend_date": us_dates, "cash_amount": [1.0]}))
        assert S["execution_date"].dt.unit == "ns"
        assert D["ex_date"].dt.unit == "ns"

        fb._SPLITS_TABLE = S
        fb._DIVS_TABLE = D
        days_ns = pd.DataFrame({"ticker": "TOY", "event_day": pd.DatetimeIndex(DAYS).as_unit("ns")})
        out, _ = fb._split_factors_for_id_worker((GID, days_ns))          # would raise MergeError before
        assert out["split_price_factor"].tolist() == pytest.approx([0.5, 1.0, 1.0])
        T, _ = fb._dividend_factors_for_id_worker((GID, _px([100.0, 99.0, 99.0]), False))
        assert T["tr_price_factor"].tolist() == pytest.approx([0.99, 1.0, 1.0])

    def test_trading_day_returns_ns(self):
        s = pd.Series(pd.to_datetime(["2024-01-16 00:30"])).dt.as_unit("us")
        assert fb._trading_day(s).dt.unit == "ns"

    def test_attach_id_days_tolerates_missing_effective_window(self):
        days = pd.DataFrame({"ticker": ["TOY"], "path": ["p.parquet"], "event_day": [EX]})
        sm = pd.DataFrame({"ticker": ["TOY"], "name": ["Toy Co"]})   # what pull_security_master() emits
        out = fb._attach_id_days(days, sm)
        assert out["id"].tolist() == [GID]
        assert list(out.columns) == ["ticker", "event_day", "id", "path"]


# ---------------------------------------------------------------------------
# Loader used by the QA notebook: day-mode merge of unadjusted (tz-aware ET) and adjusted
# (tz-naive UTC) lakes on calendar date. Previously listed "datetime" twice and raised.
# ---------------------------------------------------------------------------
class TestLakeIoLoadSeries:
    def test_day_merge_maps_split_to_sa_and_keeps_every_row(self, tmp_path):
        from polygon_ingest.lake_io import load_series

        days_et = pd.DatetimeIndex(DAYS).tz_localize("US/Eastern")               # unadjusted lake convention
        un = pd.DataFrame({"datetime": days_et, "ticker": "TOY",
                           "open": [1.0, 2.0, 3.0], "high": [1.0, 2.0, 3.0], "low": [1.0, 2.0, 3.0],
                           "close": [100.0, 99.0, 99.0], "volume": [10, 10, 10]})
        ad = pd.DataFrame({"datetime": days_et.tz_convert("UTC").tz_localize(None),  # adjusted lake convention
                           "ticker": "TOY", "close": [100.0, 99.0, 99.0],
                           "close_split": [10.0, 9.9, 9.9], "close_tr": [9.9, 9.9, 9.9]})
        for root, df in (("lake", un), ("lake_adj", ad)):
            f = tmp_path / root / "TOY" / "2024" / "01.parquet"
            f.parent.mkdir(parents=True)
            df.to_parquet(f, index=False)

        out = load_series(tmp_path / "lake", tmp_path / "lake_adj", "day", "TOY")

        assert len(out) == 3
        assert out.columns.tolist().count("datetime") == 1
        assert out["close_sa"].tolist() == pytest.approx([10.0, 9.9, 9.9])
        assert out["close_tr"].tolist() == pytest.approx([9.9, 9.9, 9.9])
        assert out["close"].tolist() == pytest.approx([100.0, 99.0, 99.0])
