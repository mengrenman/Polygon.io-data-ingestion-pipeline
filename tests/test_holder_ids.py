"""
Holder (company) ids. Price rows and corporate actions are keyed by the company behind a ticker on a date
(composite FIGI -> CIK -> ticker), so a recycled ticker's previous company never receives the current
company's splits/dividends, and one company's history is never split into two ids by an imprecise list_date.

The GM fixture mirrors what Polygon returns: the pre-2009 General Motors Corp has a CIK but no FIGI and no
dates (only the probe date it was found on); the current company has a FIGI and list_date 2010-11-18.
"""
import numpy as np
import pandas as pd
import pytest

import factor_builder as fb


def D(s):
    return pd.Timestamp(s).as_unit("ns")


def days(*ds):
    """Trading dates (tz-naive midnight) for event_day columns."""
    return pd.DatetimeIndex([D(x) for x in ds])


def bars(*ds):
    """Day-bar timestamps as the lake stores them: midnight US/Eastern = 05:00 UTC (tz-naive UTC).
    (Midnight *UTC* would be the previous evening in New York and land on the previous trading date.)"""
    return days(*ds) + pd.Timedelta(hours=5)


OLD, NEW = "CIK__0000040730", "BBG000NDYB67"
SM_GM = pd.DataFrame({
    "ticker": ["GM", "GM"],
    "composite_figi": [None, NEW],
    "cik": ["0000040730", "0001467858"],
    "effective_start": [pd.NaT, D("2010-11-18")],
    "effective_end": [pd.NaT, pd.NaT],
    "anchor_date": [D("2008-06-02"), pd.NaT],
})


def _px(ticker, *ds):
    return pd.DataFrame({"ticker": ticker, "event_day": days(*ds)})


class TestHolderId:
    def test_figi_then_cik_then_ticker(self):
        assert fb._holder_id("BBG1", "123", "x") == "BBG1"
        assert fb._holder_id(None, "0000040730", "GM") == OLD
        assert fb._holder_id(None, None, "gm") == "NOFIGI__GM"
        assert fb._holder_id(float("nan"), float("nan"), "GM") == "NOFIGI__GM"


class TestAssignHolderIds:
    def test_unknown_ticker_gets_nofigi(self):
        assert fb._assign_holder_ids(_px("ZZZ", "2020-01-02"), SM_GM, "event_day").tolist() == ["NOFIGI__ZZZ"]

    def test_single_holder_ignores_window(self):
        sm = pd.DataFrame({"ticker": ["NVDA"], "composite_figi": ["BBG000BBJQV0"], "cik": ["1"],
                           "effective_start": [D("2003-09-10")], "effective_end": [pd.NaT]})
        px = _px("NVDA", "1999-01-22", "2003-09-10", "2024-06-10")      # first row predates list_date
        assert fb._assign_holder_ids(px, sm, "event_day").tolist() == ["BBG000BBJQV0"] * 3

    def test_recycled_ticker_split_by_holder(self):
        px = _px("GM", "2005-03-01", "2009-06-01", "2010-11-18", "2015-01-02")
        assert fb._assign_holder_ids(px, SM_GM, "event_day").tolist() == [OLD, OLD, NEW, NEW]

    def test_two_bounded_holders_gap_goes_to_nearest(self):
        sm = pd.DataFrame({"ticker": ["X", "X"], "composite_figi": ["A", "B"], "cik": [None, None],
                           "effective_start": [D("2000-01-01"), D("2010-06-01")],
                           "effective_end": [D("2009-01-31"), pd.NaT]})
        px = _px("X", "2005-01-01", "2009-03-01", "2010-03-01", "2012-01-01")
        assert fb._assign_holder_ids(px, sm, "event_day").tolist() == ["A", "A", "B", "B"]

    def test_old_puller_security_master_still_works(self):
        # pre-FIGI puller output: no composite_figi / holder_id / window columns at all
        sm = pd.DataFrame({"ticker": ["AAPL"], "name": ["Apple"]})
        assert fb._assign_holder_ids(_px("AAPL", "2020-01-02"), sm, "event_day").tolist() == ["NOFIGI__AAPL"]


class TestAttach:
    def test_attach_id_never_drops_rows(self):
        px = pd.DataFrame({"ticker": "GM", "datetime": bars("2005-03-01", "2009-06-01", "2015-01-02"),
                           "close": [1.0, 2.0, 3.0], "volume": [1, 1, 1]})
        out = fb._attach_id(px, SM_GM)
        assert len(out) == 3 and out["id"].tolist() == [OLD, OLD, NEW]
        assert "event_day" in out.columns

    def test_attach_id_days(self):
        d = pd.DataFrame({"ticker": "GM", "event_day": days("2005-03-01", "2015-01-02"), "path": ["a", "b"]})
        out = fb._attach_id_days(d, SM_GM)
        assert out["id"].tolist() == [OLD, NEW]
        assert list(out.columns) == ["ticker", "event_day", "id", "path"]


class TestEventIds:
    def test_events_keyed_by_holder_on_event_date(self):
        spl = pd.DataFrame({"ticker": ["GM", "GM"], "execution_date": days("2006-01-10", "2015-06-01"), "ratio": [2.0, 3.0]})
        out = fb._assign_event_ids(spl, SM_GM, ["execution_date"])
        assert out["holder_id"].tolist() == [OLD, NEW]
        assert fb._prep_splits(out)["event_id"].tolist() == [OLD, NEW]

    def test_dividend_date_column_variants(self):
        div = pd.DataFrame({"ticker": ["GM"], "ex_dividend_date": days("2015-03-01"), "cash_amount": [0.3]})
        assert fb._assign_event_ids(div, SM_GM, ["ex_date", "ex_dividend_date"])["holder_id"].tolist() == [NEW]

    def test_prep_falls_back_to_figi_then_ticker(self):
        S = fb._prep_splits(pd.DataFrame({"ticker": ["A", "B"], "execution_date": days("2020-01-01", "2020-01-01"),
                                          "ratio": [2.0, 2.0], "composite_figi": ["BBGA", None]}))
        assert S["event_id"].tolist() == ["BBGA", "NOFIGI__B"]

    def test_empty_events_table(self):
        out = fb._assign_event_ids(pd.DataFrame({"ticker": [], "execution_date": [], "ratio": []}), SM_GM, ["execution_date"])
        assert out.empty and "holder_id" in out.columns


class TestNoCrossApplication:
    """A (hypothetical) 2:1 split of the new GM in 2015 must not adjust the old GM's 2005-2009 prices."""
    SPL = pd.DataFrame({"ticker": ["GM"], "execution_date": days("2015-06-01"), "ratio": [2.0]})
    DAYS = days("2005-03-01", "2009-06-01", "2011-01-03", "2015-06-01")

    def test_batch(self):
        px = pd.DataFrame({"ticker": "GM", "datetime": self.DAYS + pd.Timedelta(hours=5), "close": [30.0, 1.0, 35.0, 17.5], "volume": [1] * 4})
        px_id = fb._attach_id(px, SM_GM)
        spl = fb._assign_event_ids(self.SPL, SM_GM, ["execution_date"])
        F = fb._build_split_factors(px_id, spl, stats={}, workers=1)
        f = px_id[["id", "event_day"]].merge(F, on=["id", "event_day"]).sort_values("event_day")
        assert f.loc[f["id"] == OLD, "split_price_factor"].tolist() == pytest.approx([1.0, 1.0])
        assert f.loc[f["id"] == NEW, "split_price_factor"].tolist() == pytest.approx([0.5, 1.0])

    def test_streaming(self):
        id_days = fb._attach_id_days(pd.DataFrame({"ticker": "GM", "event_day": self.DAYS, "path": list("abcd")}), SM_GM)
        spl = fb._assign_event_ids(self.SPL, SM_GM, ["execution_date"])
        F = fb._build_split_factors_from_days(id_days, spl, edges=None, detect_gaps=False).sort_values("event_day")
        assert F["split_price_factor"].tolist() == pytest.approx([1.0, 1.0, 0.5, 1.0])

    def test_streaming_dividends_do_not_cross_holders(self):
        id_days = fb._attach_id_days(pd.DataFrame({"ticker": "GM", "event_day": self.DAYS, "path": list("abcd")}), SM_GM)
        edges = pd.DataFrame({"ticker": "GM", "event_day": self.DAYS, "first_close": [30.0, 1.0, 35.0, 34.0], "last_close": [30.0, 1.0, 35.0, 34.0]})
        F = id_days[["ticker", "event_day"]].assign(split_price_factor=1.0, split_volume_factor=1.0)
        div = fb._assign_event_ids(pd.DataFrame({"ticker": ["GM"], "ex_date": days("2015-06-01"), "amount": [1.0]}), SM_GM, ["ex_date"])
        base = fb._build_daily_prior_base(id_days, use_split_base=True, F=F, edges=edges)
        G = fb._build_dividend_factors_from_days(id_days, div, base).sort_values("event_day")
        # old holder untouched; new holder: $1 on prior close 35 -> retained 34/35 before the ex-date
        assert G["tr_price_factor"].tolist() == pytest.approx([1.0, 1.0, 34.0 / 35.0, 1.0])

    def test_nofigi_id_still_falls_back_to_ticker_events(self):
        # no security-master information at all -> NOFIGI id, ticker-keyed events still apply (previous behaviour)
        sm = pd.DataFrame({"ticker": [], "composite_figi": []})
        px = pd.DataFrame({"ticker": "Q", "datetime": bars("2015-01-02", "2015-06-01"), "close": [10.0, 5.0], "volume": [1, 1]})
        px_id = fb._attach_id(px, sm)
        spl = fb._assign_event_ids(pd.DataFrame({"ticker": ["Q"], "execution_date": days("2015-06-01"), "ratio": [2.0]}), sm, ["execution_date"])
        F = fb._build_split_factors(px_id, spl, stats={}, workers=1)
        assert px_id["id"].tolist() == ["NOFIGI__Q"] * 2
        assert sorted(F["split_price_factor"].tolist()) == pytest.approx([0.5, 1.0])
