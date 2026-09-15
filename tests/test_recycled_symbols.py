"""
Recycled symbols and ticker case.

A symbol that goes quiet for months and comes back has almost always been reassigned to a different
company: `ARM` printed bars from 2003 and Arm Holdings plc listed in 2023. Without a CONFIRMED start
in the security master the adjuster used to credit every one of those bars to today's company, and
hand them today's splits. These tests pin the segmentation that fixes it, and the case preservation
that keeps `AAp` (Alcoa preferred) out of `AAP` (Advance Auto Parts).
"""
import sys

import numpy as np
import pandas as pd
import pytest

import factor_builder as fb

NS = "datetime64[ns]"


def _days(*ranges):
    out = []
    for a, b in ranges:
        out += list(pd.bdate_range(a, b))
    return pd.DatetimeIndex(out).as_unit("ns")


def _sm(rows):
    cols = ["ticker", "holder_id", "composite_figi", "cik", "effective_start", "effective_end",
            "anchor_date", "start_confirmed", "end_confirmed"]
    return pd.DataFrame(rows, columns=cols)


class TestPriceSegments:
    def test_splits_history_at_a_long_gap(self):
        d = _days(("2003-09-10", "2003-10-10"), ("2023-09-14", "2023-10-10"))
        segs = fb._price_segments(["ARM"] * len(d), d)
        assert len(segs) == 2
        assert segs["seg"].tolist() == [0, 1]
        assert segs.loc[0, "end"] < pd.Timestamp("2004-01-01") < segs.loc[1, "start"]

    def test_continuous_history_is_one_segment(self):
        d = _days(("2020-01-02", "2020-06-30"))
        assert len(fb._price_segments(["AAPL"] * len(d), d)) == 1

    def test_gap_threshold_is_respected(self):
        d = _days(("2020-01-02", "2020-01-31"), ("2020-03-16", "2020-04-15"))   # ~45-day gap
        assert len(fb._price_segments(["X"] * len(d), d, gap_days=60)) == 1
        assert len(fb._price_segments(["X"] * len(d), d, gap_days=30)) == 2


class TestRecyclableSelection:
    def test_only_single_unconfirmed_holders_qualify(self):
        sm = _sm([
            ("ARM", "BBG_NEW", "BBG_NEW", None, pd.NaT, pd.NaT, pd.NaT, False, False),      # unconfirmed
            ("FB", "BBG_META", "BBG_META", None, pd.NaT, pd.to_datetime("2022-06-09"), pd.NaT, False, True),
            ("GM", "BBG_GM", "BBG_GM", None, pd.to_datetime("2010-11-18"), pd.NaT, pd.NaT, True, False),  # confirmed
        ])
        cand = fb._recyclable_tickers(sm)
        assert "ARM" in cand and "FB" in cand and "GM" not in cand

    def test_multi_holder_ticker_is_left_to_the_window_logic(self):
        sm = _sm([
            ("GM", "CIK__40730", None, "40730", pd.NaT, pd.to_datetime("2009-06-01"), pd.NaT, False, True),
            ("GM", "BBG_NEWGM", "BBG_NEWGM", None, pd.to_datetime("2010-11-18"), pd.NaT, pd.NaT, False, False),
        ])
        assert "GM" not in fb._recyclable_tickers(sm)


class TestSegmentIdAssignment:
    @staticmethod
    def _frame():
        d = _days(("2003-09-10", "2003-10-10"), ("2023-09-14", "2023-10-10"))
        return pd.DataFrame({"datetime": d, "ticker": "ARM", "close": 10.0, "volume": 100})

    def test_old_bars_get_their_own_id_and_new_bars_keep_the_holder(self):
        px = self._frame()
        sm = _sm([("ARM", "BBG_ARM", "BBG_ARM", None, pd.NaT, pd.NaT, pd.NaT, False, False)])
        segs = fb._recycled_segments(px["ticker"].to_numpy(), fb._trading_day(px["datetime"]), sm)
        out = fb._attach_id(px, sm, segments=segs)
        old = out[out["datetime"] < "2004-01-01"]["id"].unique().tolist()
        new = out[out["datetime"] > "2023-01-01"]["id"].unique().tolist()
        assert old == ["NOFIGI__ARM#SEG0"] and new == ["BBG_ARM"]

    def test_without_segments_every_bar_is_the_current_holder(self):
        px = self._frame()
        sm = _sm([("ARM", "BBG_ARM", "BBG_ARM", None, pd.NaT, pd.NaT, pd.NaT, False, False)])
        assert fb._attach_id(px, sm)["id"].unique().tolist() == ["BBG_ARM"]

    def test_gap_days_zero_disables_segmentation(self):
        px = self._frame()
        sm = _sm([("ARM", "BBG_ARM", "BBG_ARM", None, pd.NaT, pd.NaT, pd.NaT, False, False)])
        assert fb._recycled_segments(px["ticker"].to_numpy(), fb._trading_day(px["datetime"]), sm, 0).empty

    def test_continuous_ticker_is_untouched(self):
        d = _days(("2020-01-02", "2020-06-30"))
        px = pd.DataFrame({"datetime": d, "ticker": "AAPL", "close": 1.0, "volume": 1})
        sm = _sm([("AAPL", "BBG_AAPL", "BBG_AAPL", None, pd.NaT, pd.NaT, pd.NaT, False, False)])
        segs = fb._recycled_segments(px["ticker"].to_numpy(), fb._trading_day(px["datetime"]), sm)
        assert segs.empty
        assert fb._attach_id(px, sm, segments=segs)["id"].unique().tolist() == ["BBG_AAPL"]


class TestEventsRespectSegments:
    @staticmethod
    def _setup():
        d = _days(("2003-09-10", "2003-10-10"), ("2023-09-14", "2023-10-10"))
        px = pd.DataFrame({"datetime": d, "ticker": "ARM", "close": 10.0, "volume": 100})
        sm = _sm([("ARM", "BBG_ARM", "BBG_ARM", None, pd.NaT, pd.NaT, pd.NaT, False, False)])
        spl = pd.DataFrame({"ticker": ["ARM"], "execution_date": pd.to_datetime(["2023-09-20"]).as_unit("ns"),
                            "split_from": [1.0], "split_to": [2.0]})
        return px, sm, fb._assign_event_ids(spl, sm, ["execution_date"])

    def test_a_later_split_does_not_reach_the_earlier_company(self):
        px, sm, spl = self._setup()
        segs = fb._recycled_segments(px["ticker"].to_numpy(), fb._trading_day(px["datetime"]), sm)
        spl2 = fb._apply_event_segments(spl, segs, ["execution_date"])
        assert spl2["holder_id"].tolist() == ["BBG_ARM"]         # split belongs to the 2023 holder only
        assert fb._prep_splits(spl2)["event_id"].tolist() == ["BBG_ARM"]

        out, _, _ = fb._adjust_frame(px, sm, spl, pd.DataFrame(
            {"ticker": [], "ex_date": pd.to_datetime([]), "cash_amount": []}), adjust="splits")
        old = out[out["datetime"] < "2004-01-01"]
        new = out[out["datetime"] > "2023-01-01"]
        assert (old["split_price_factor"] == 1.0).all(), "2003 bars must not carry the 2023 split"
        assert new["split_price_factor"].min() == pytest.approx(0.5)

    def test_without_the_fix_the_split_would_hit_every_bar(self):
        px, sm, spl = self._setup()
        out, _, _ = fb._adjust_frame(px, sm, spl, pd.DataFrame(
            {"ticker": [], "ex_date": pd.to_datetime([]), "cash_amount": []}), adjust="splits", gap_days=0)
        old = out[out["datetime"] < "2004-01-01"]
        assert (old["split_price_factor"] == 0.5).all()      # the behaviour being fixed


class TestTickerCaseIsPreserved:
    def test_two_securities_under_one_upper_cased_symbol_stay_apart(self):
        d = _days(("2020-01-02", "2020-03-31"))
        px = pd.concat([
            pd.DataFrame({"datetime": d, "ticker": "AAP", "close": 100.0, "volume": 10}),
            pd.DataFrame({"datetime": d, "ticker": "AAp", "close": 25.0, "volume": 1}),
        ], ignore_index=True)
        sm = _sm([("AAP", "BBG_AAP", "BBG_AAP", None, pd.NaT, pd.NaT, pd.NaT, False, False),
                  ("AAp", "BBG_ALCOA_PFD", "BBG_ALCOA_PFD", None, pd.NaT, pd.NaT, pd.NaT, False, False)])
        out = fb._attach_id(px, sm)
        ids = out.groupby("ticker")["id"].unique().to_dict()
        assert ids["AAP"].tolist() == ["BBG_AAP"] and ids["AAp"].tolist() == ["BBG_ALCOA_PFD"]

    def test_a_split_on_the_common_does_not_touch_the_preferred(self):
        d = _days(("2020-01-02", "2020-03-31"))
        px = pd.concat([
            pd.DataFrame({"datetime": d, "ticker": "AAP", "close": 100.0, "volume": 10}),
            pd.DataFrame({"datetime": d, "ticker": "AAp", "close": 25.0, "volume": 1}),
        ], ignore_index=True)
        sm = _sm([("AAP", "BBG_AAP", "BBG_AAP", None, pd.NaT, pd.NaT, pd.NaT, False, False),
                  ("AAp", "BBG_ALCOA_PFD", "BBG_ALCOA_PFD", None, pd.NaT, pd.NaT, pd.NaT, False, False)])
        spl = pd.DataFrame({"ticker": ["AAP"], "execution_date": pd.to_datetime(["2020-02-18"]).as_unit("ns"),
                            "split_from": [1.0], "split_to": [2.0]})
        spl = fb._assign_event_ids(spl, sm, ["execution_date"])
        out, _, _ = fb._adjust_frame(px, sm, spl, pd.DataFrame(
            {"ticker": [], "ex_date": pd.to_datetime([]), "cash_amount": []}), adjust="splits")
        assert out.loc[out["ticker"] == "AAp", "split_price_factor"].eq(1.0).all()
        assert out.loc[out["ticker"] == "AAP", "split_price_factor"].min() == pytest.approx(0.5)

    def test_holder_id_keeps_case(self):
        assert fb._holder_id(None, None, "AAp") == "NOFIGI__AAp"
        assert fb._holder_id(None, None, "AAP") == "NOFIGI__AAP"

    def test_watchlist_matching_folds_case(self):
        v = pd.Series(["AAPL", "AAp", "AAP"])
        assert fb._wanted_mask(v, ["aapl"]).tolist() == [True, False, False]
        assert fb._wanted_mask(v, None).all()
