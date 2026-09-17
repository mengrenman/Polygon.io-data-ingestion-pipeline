"""
Security type inference for the records Polygon leaves untyped.

`/v3/reference/tickers` returns no `type` for 28.8% of delisted stock records and none of the active
ones, so a universe filtered on `type == "CS"` selects for survival: 22.8% of liquid June name-days
in 2004 have no usable type, against 0.0% in 2024. Polygon does not hold the field on any endpoint
(see the module docstring), so it is inferred from the name and symbol.

These tests pin the contract rather than the accuracy: Polygon's own value is never overwritten, the
evidence used is always reported, and the cases that made the rules what they are stay fixed.
"""
import numpy as np
import pandas as pd
import pytest

from polygon_ingest.security_type import UNTYPED_SOURCES, fill_type, infer_type, infer_types
from polygon_ingest.universe import classify_segments


class TestInferType:
    @pytest.mark.parametrize("ticker,name,expected", [
        # the delisted common stocks the gap is really about: acquired or bankrupt, no type from Polygon
        ("ABGX", "ABGENIX INC", "CS"),
        ("ABFS", "ARKANSAS BEST CORP (DEL)", "CS"),
        ("AACC", "ASSET ACCEP CAP CORP", "CS"),
        ("ABCW", "ANCHOR BANCORP WISCONSIN INC COMMON STOCK", "CS"),
        # CORPORATION and INCORPORATED must match: requiring \bCORP\b left 771 of these unlabelled
        ("AACT", "Ares Acquisition Corporation II", "CS"),
        ("ACTA", "Actua Corporation", "CS"),
        # and the instruments that should never enter a common-stock universe
        ("COWNL", "Cowen Inc. 7.75% Senior Notes due 2033", "SP"),
        ("TMCWW", "TMC the metals company Inc. Warrants", "WARRANT"),
        ("ESGRP", "Enstar Group Limited Depositary Shares", "PFD"),
        ("EVGBC", "Eaton Vance Global Income Builder NextShares", "ETF"),
    ])
    def test_reads_the_instrument_out_of_the_name(self, ticker, name, expected):
        assert infer_type(ticker, name)[0] == expected

    def test_lowercase_p_and_r_are_class_codes(self):
        assert infer_type("AAGpT", "")[0] == "PFD"
        assert infer_type("AAGpT", "")[1] == "symbol"

    def test_lowercase_w_is_not_taken_as_a_warrant(self):
        # `w` marks warrants AND when-issued lines, and Polygon types the latter CS: AANw is Aaron's
        # when-issued, ABTw is Abbott ex-distribution. Reading `w` as a warrant mislabelled 279 of them.
        assert infer_type("AANw", "The Aaron's Company, Inc.")[0] == "CS"
        assert infer_type("ABTw", "ABBOTT LABORATORIES COM STK (IL) EX-DIST W.I.")[0] == "CS"
        # a name that does say warrant still wins
        assert infer_type("XYZw", "Some Corp Warrants")[0] == "WARRANT"

    def test_admits_it_does_not_know(self):
        typ, src = infer_type("AAB.WS", "")
        assert typ is None and src in UNTYPED_SOURCES
        typ, src = infer_type("ZZZZ", "")
        assert typ is None and src == "no-name"
        typ, src = infer_type("ZZZZ", "qqq")
        assert typ is None and src == "unmatched"


class TestFillType:
    @pytest.fixture
    def table(self):
        return pd.DataFrame({
            "ticker": ["AAP", "ABGX", "AAB.WS"],
            "name": ["ADVANCE AUTO PARTS INC", "ABGENIX INC", ""],
            "type": pd.array(["CS", None, None], dtype="string"),
        })

    def test_polygon_value_is_never_overwritten(self, table):
        out = fill_type(table)
        assert out.loc[0, "type_inferred"] == "CS" and out.loc[0, "type_source"] == "polygon"
        assert out.loc[0, "type"] == "CS"                       # the original column is untouched

    def test_only_the_nulls_are_inferred(self, table):
        out = fill_type(table)
        assert out.loc[1, "type_inferred"] == "CS" and out.loc[1, "type_source"] == "name"

    def test_an_unresolvable_row_stays_unresolved(self, table):
        out = fill_type(table)
        assert pd.isna(out.loc[2, "type_inferred"])
        assert out.loc[2, "type_source"] in UNTYPED_SOURCES

    def test_infer_types_is_aligned_with_its_inputs(self):
        got = infer_types(["ABGX", "TMCWW"], ["ABGENIX INC", "TMC the metals company Inc. Warrants"])
        assert got["type_inferred"].tolist() == ["CS", "WARRANT"]


class TestUniverseUsesIt:
    @pytest.fixture
    def segments(self):
        return pd.DataFrame({
            "ticker": ["ABGX", "COWNL"],
            "segment": [0, 0],
            "start": pd.to_datetime(["2003-09-10", "2003-09-10"]),
            "end": pd.to_datetime(["2006-04-03", "2023-03-02"]),
            "n_days": [600, 600],
            "is_last": [True, True],
        })

    @pytest.fixture
    def tickers_table(self):
        return pd.DataFrame({
            "ticker": ["ABGX", "COWNL"],
            "name": ["ABGENIX INC", "Cowen Inc. 7.75% Senior Notes due 2033"],
            "type": pd.array([None, None], dtype="string"),
            "primary_exchange": ["XNAS", "XNAS"],
            "active": [False, False],
            "holder_id": ["CIK__1", "CIK__2"],
            "delisted_utc": pd.to_datetime(["2006-04-03", "2023-03-02"]),
        })

    def test_an_untyped_common_stock_becomes_eligible(self, segments, tickers_table):
        out = classify_segments(segments, tickers_table)
        row = out[out["ticker"] == "ABGX"].iloc[0]
        assert row["type"] == "CS" and bool(row["eligible"]) and row["type_source"] == "name"

    def test_an_untyped_note_does_not(self, segments, tickers_table):
        out = classify_segments(segments, tickers_table)
        row = out[out["ticker"] == "COWNL"].iloc[0]
        assert row["type"] == "SP" and not bool(row["eligible"])

    def test_inference_can_be_switched_off(self, segments, tickers_table):
        out = classify_segments(segments, tickers_table, use_inferred_type=False)
        # without it both fall back to the untyped policy, which admits on the symbol heuristic alone
        assert out["type"].isna().all()
        assert out["reason"].tolist() == ["untyped_symbol_ok", "untyped_symbol_ok"]

    def test_polygon_type_still_wins(self, segments, tickers_table):
        t = tickers_table.copy()
        t["type"] = pd.array(["ETF", None], dtype="string")     # Polygon says ETF; the name says CS
        out = classify_segments(segments, t)
        row = out[out["ticker"] == "ABGX"].iloc[0]
        assert row["type"] == "ETF" and row["type_source"] == "polygon" and not bool(row["eligible"])
