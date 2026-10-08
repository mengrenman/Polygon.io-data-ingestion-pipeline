"""
Symbols reused after the lake ends, and corporate actions filed under a company's later symbol.

Today's tickers table names one company per symbol, the one holding it now, so a symbol that stopped trading
inside the lake and was reused after it credits every old bar to the new company: ABX was Barrick Gold until
2018-12-31 and is Abacus Global Management today; BBT was BB&T until 2019-12-06 and is Beacon Financial (the
renamed Berkshire Hills, BHLB) today. Massive also files a company's dividends under a LATER symbol: Barrick's
ABX-era dividends sit under GOLD, mixed with Randgold's (GOLD's holder until 2018-12-28); BB&T's 2011-2019
dividends sit under TFC, whose holder until 2011 was a closed-end fund. These fixtures are scaled-down copies of
those real cases (same ids, same structure, prices simulated) and pin how polygon_pullers.asof untangles them.
"""
import numpy as np
import pandas as pd
import pytest

import factor_builder as fb
import polygon_pullers.asof as A
import polygon_pullers.bulk as bulk

BARRICK, RANDGOLD, BARNES = "BBG000BB07P9", "CIK__0001176338", "BBG000BCSCB1"
ABACUS, GOLDCOM = "BBG00VY1KB95", "BBG005ZVDK48"
TRUIST, BEACON, FUND = "BBG000BYYLS8", "BBG000BB00D7", "CIK__0000836267"
DAYS = pd.bdate_range("2016-01-04", "2020-12-31")


def _q(year, months=(3, 6, 9, 12), day=10):
    return [pd.Timestamp(year, m, day) for m in months]


def _bday(ts):
    return DAYS[DAYS.searchsorted(ts)]


def _line(seed, p0, windows, divs):
    """One security's sessions across its symbols. `windows`: [(ticker, start, end)]; `divs`: {date: amount}
    paid by THIS security (the price drops by the amount at the open)."""
    rng = np.random.default_rng(seed)
    divs = {_bday(d): v for d, v in divs.items()}
    days = [(t, d) for t, s, e in windows for d in DAYS[(DAYS >= s) & (DAYS <= e)]]
    rows, close = [], p0
    for i, (t, d) in enumerate(days):
        open_ = p0 if i == 0 else close * np.exp(rng.normal(0, 0.015)) - divs.get(d, 0.0)
        close = open_ * np.exp(rng.normal(0, 0.01))
        rows.append((t, d, open_, close))
    return rows


def _div(ticker, date, amount, freq, i):
    d = _bday(date)
    return {"id": f"E{ticker}{i:03d}{d:%Y%m%d}", "ticker": ticker, "ex_dividend_date": d, "pay_date": d + pd.Timedelta(days=15),
            "record_date": d, "declaration_date": pd.NaT, "cash_amount": amount, "currency": "USD",
            "frequency": freq, "dividend_type": "CD"}


# --------------------------------------------------------------------------- Barrick / Randgold / Barnes
BARRICK_ABX_SEMI = [pd.Timestamp("2016-05-26"), pd.Timestamp("2016-11-28")]                 # frequency 2
BARRICK_ABX_Q = [d for y in (2017, 2018) for d in _q(y)]                                    # frequency 4
BARRICK_GOLD_Q = _q(2019) + _q(2020, (3, 6))
BARRICK_B_Q = [pd.Timestamp("2020-09-10"), pd.Timestamp("2020-12-10")]
RANDGOLD_ANNUAL = [pd.Timestamp(f"{y}-03-15") for y in (2016, 2017, 2018)]
RANDGOLD_FINAL = pd.Timestamp("2019-01-02")      # ex the day Barrick took GOLD; Randgold's last session 2018-12-28
BARNES_Q = [d for y in range(2016, 2020) for d in _q(y, day=20)] + [pd.Timestamp("2020-02-20")]


def _barrick_world():
    barrick_divs = {**{d: 0.10 for d in BARRICK_ABX_SEMI}, **{d: 0.05 for d in BARRICK_ABX_Q + BARRICK_GOLD_Q + BARRICK_B_Q}}
    rows = (_line(1, 15.0, [("ABX", "2016-01-04", "2018-12-31"), ("GOLD", "2019-01-02", "2020-06-30"),
                            ("B", "2020-07-01", "2020-12-31")], barrick_divs)
            + _line(2, 80.0, [("GOLD", "2016-01-04", "2018-12-28")], {d: 2.0 for d in RANDGOLD_ANNUAL})
            + _line(3, 50.0, [("B", "2016-01-04", "2020-03-31")], {d: 0.16 for d in BARNES_Q}))
    days = pd.DataFrame(rows, columns=["ticker", "date", "open", "close"])
    asof = []
    for y, when in ((2016, "2016-06-30"), (2017, "2017-06-30"), (2018, "2018-06-29"), (2019, "2019-06-28"), (2020, "2020-06-30")):
        if y <= 2018:
            asof.append((when, "ABX", "BARRICK GOLD CORP", "0000756894", BARRICK))
            asof.append((when, "GOLD", "RANDGOLD RES LTD", "0001176338", None))
        else:
            asof.append((when, "GOLD", "Barrick Gold Corp.", "0000756894", BARRICK))
        if y <= 2019:
            asof.append((when, "B", "Barnes Group Inc.", "0000009984", BARNES))
    asof.append(("2020-12-31", "B", "Barrick Mining Corporation", "0000756894", BARRICK))
    asof = pd.DataFrame(asof, columns=["asof", "ticker", "name", "cik", "composite_figi"])
    # Massive's filing: Barrick's whole pre-2019 history under GOLD (its symbol 2019-2025), Randgold's under
    # GOLD too, Barnes' under B, Barrick's from its move to B under B.
    divs = ([_div("GOLD", d, 0.10, 2, i) for i, d in enumerate(BARRICK_ABX_SEMI)]
            + [_div("GOLD", d, 0.05, 4, i) for i, d in enumerate(BARRICK_ABX_Q + BARRICK_GOLD_Q)]
            + [_div("GOLD", d, 2.0, 1, i) for i, d in enumerate(RANDGOLD_ANNUAL)]
            + [_div("GOLD", RANDGOLD_FINAL, 2.5, 1, 99)]
            + [_div("B", d, 0.16, 4, i) for i, d in enumerate(BARNES_Q)]
            + [_div("B", d, 0.05, 4, 50 + i) for i, d in enumerate(BARRICK_B_Q)])
    return days, asof, pd.DataFrame(divs)


# --------------------------------------------------------------------------- BB&T / Truist / fund / Beacon
BBT_Q = [d for y in range(2016, 2020) for d in _q(y, (2, 5, 8, 11), day=8)]
TRUIST_Q = _q(2020, (2, 5, 8, 11), day=8)
BHLB_Q = [d for y in range(2016, 2021) for d in _q(y, (1, 4, 7, 10), day=20)]


def _bbt_world():
    rows = (_line(4, 40.0, [("BBT", "2016-01-04", "2019-12-06"), ("TFC", "2019-12-09", "2020-12-31")],
                  {d: 0.30 for d in BBT_Q} | {d: 0.45 for d in TRUIST_Q})
            + _line(5, 6.0, [("TFC", "2016-01-04", "2017-09-29")], {})
            + _line(6, 30.0, [("BHLB", "2016-01-04", "2020-12-31")], {d: 0.22 for d in BHLB_Q}))
    days = pd.DataFrame(rows, columns=["ticker", "date", "open", "close"])
    asof = []
    for when in ("2016-06-30", "2017-06-30", "2018-06-29", "2019-06-28", "2020-06-30", "2020-12-31"):
        y = int(when[:4])
        if y <= 2019:
            asof.append((when, "BBT", "BB&T CORPORATION", "0000092230", "BBG00JPVTY24"))   # an older FIGI, same CIK
        else:
            asof.append((when, "TFC", "Truist Financial Corporation", "0000092230", TRUIST))
        if y <= 2017:
            asof.append((when, "TFC", "TAIWAN GREATER CHINA FUND", "0000836267", None))
        asof.append((when, "BHLB", "BERKSHIRE HILLS BANCORP INC", "0001108134", BEACON))
    asof = pd.DataFrame(asof, columns=["asof", "ticker", "name", "cik", "composite_figi"])
    divs = ([_div("TFC", d, 0.30, 4, i) for i, d in enumerate(BBT_Q)]           # BB&T's history, under its later symbol
            + [_div("BBT", d, 0.30, 4, i) for i, d in enumerate(BBT_Q[:4])]     # a stale copy of 2016 under BBT
            + [_div("TFC", d, 0.45, 4, 50 + i) for i, d in enumerate(TRUIST_Q)]
            + [_div("BHLB", d, 0.22, 4, i) for i, d in enumerate(BHLB_Q)])
    return days, asof, pd.DataFrame(divs)


def _market_tickers():
    """Today's table: one record per symbol, the company holding it now."""
    rows = [("ABX", "Abacus Global Management, Inc.", True, "0001814287", ABACUS, pd.NaT),
            ("GOLD", "Gold.com, Inc.", True, "0001591588", GOLDCOM, pd.NaT),
            ("B", "Barrick Mining Corporation", True, "0000756894", BARRICK, pd.NaT),
            ("BBT", "Beacon Financial Corporation", True, "0001108134", BEACON, pd.NaT),
            ("TFC", "Truist Financial Corporation", True, "0000092230", TRUIST, pd.NaT),
            ("BHLB", "Berkshire Hills Bancorp, Inc.", False, "0001108134", BEACON, pd.Timestamp("2025-09-02"))]
    t = pd.DataFrame(rows, columns=["ticker", "name", "active", "cik", "composite_figi", "delisted_utc"])
    for c in ("type", "market", "locale", "primary_exchange", "currency_name", "share_class_figi", "last_updated_utc"):
        t[c] = None
    t["type"], t["market"] = "CS", "stocks"
    t["holder_id"] = [bulk.holder_id(f, k, s) for f, k, s in zip(t["composite_figi"], t["cik"], t["ticker"])]
    return t


def _ids_by_piece(lines):
    return {(r.ticker, str(r.start.date())): r.holder_id for r in lines.itertuples()}


@pytest.fixture(scope="module")
def barrick():
    days, asof, divs = _barrick_world()
    lines = A.holder_lines(days, asof, _market_tickers())
    return days, lines, A.refile_actions(divs, lines, date_col="ex_dividend_date", kind="dividend", days=days)


@pytest.fixture(scope="module")
def bbt():
    days, asof, divs = _bbt_world()
    lines = A.holder_lines(days, asof, _market_tickers())
    return days, lines, A.refile_actions(divs, lines, date_col="ex_dividend_date", kind="dividend", days=days)


class TestHolderLines:
    def test_abx_bars_belong_to_barrick_not_todays_abacus(self, barrick):
        ids = _ids_by_piece(barrick[1])
        assert ids[("ABX", "2016-01-04")] == BARRICK != ABACUS

    def test_gold_is_two_companies_split_at_the_handoff(self, barrick):
        lines = barrick[1]
        g = lines[lines["ticker"] == "GOLD"].sort_values("start")
        assert g["holder_id"].tolist() == [RANDGOLD, BARRICK]
        assert g["end"].iloc[0] == pd.Timestamp("2018-12-28") and g["start"].iloc[1] == pd.Timestamp("2019-01-02")
        assert "handoff@2019-01-02" in g["evidence"].iloc[1]

    def test_barrick_is_one_line_across_abx_gold_b(self, barrick):
        lines = barrick[1]
        b = lines[lines["holder_id"] == BARRICK].sort_values("start")
        assert b["ticker"].tolist() == ["ABX", "GOLD", "B"] and b["line"].nunique() == 1
        assert b["open_end"].tolist() == [False, False, True]          # still trading as B today

    def test_barnes_keeps_b_until_it_left(self, barrick):
        ids = _ids_by_piece(barrick[1])
        assert ids[("B", "2016-01-04")] == BARNES and ids[("B", "2020-07-01")] == BARRICK

    def test_bbt_is_bbt_and_truist_one_line_not_todays_beacon(self, bbt):
        lines = bbt[1]
        bb = lines[lines["holder_id"] == TRUIST].sort_values("start")
        assert bb["ticker"].tolist() == ["BBT", "TFC"] and bb["line"].nunique() == 1
        assert (lines.loc[lines["ticker"] == "BBT", "holder_id"] != BEACON).all()

    def test_tfc_earlier_holder_is_the_fund(self, bbt):
        ids = _ids_by_piece(bbt[1])
        assert ids[("TFC", "2016-01-04")] == FUND

    def test_beacon_line_is_bhlb_not_bbt(self, bbt):
        lines = bbt[1]
        assert lines.loc[lines["ticker"] == "BHLB", "holder_id"].tolist() == [BEACON]

    def test_security_master_rows_are_confirmed_windows(self, barrick):
        sm = A.lines_to_security_master(barrick[1])
        abx = sm[sm["ticker"] == "ABX"].iloc[0]
        assert abx["start_confirmed"] and abx["end_confirmed"]
        assert abx["effective_end"] == pd.Timestamp("2018-12-31")
        open_b = sm[(sm["ticker"] == "B") & (sm["holder_id"] == BARRICK)].iloc[0]
        assert pd.isna(open_b["effective_end"]) and not open_b["end_confirmed"]


class TestRefiling:
    def test_barrick_abx_era_dividends_move_from_gold_to_abx(self, barrick):
        r = barrick[2]
        moved = r[(r["filed_ticker"] == "GOLD") & (r["ticker"] == "ABX")]
        assert len(moved) == len(BARRICK_ABX_SEMI) + len(BARRICK_ABX_Q)
        assert (moved["holder_id"] == BARRICK).all()

    def test_randgold_keeps_its_annual_dividends(self, barrick):
        r = barrick[2]
        rg = r[r["holder_id"] == RANDGOLD]
        assert sorted(rg["cash_amount"].tolist()) == [2.0, 2.0, 2.0, 2.5]
        assert (rg["ticker"] == "GOLD").all()

    def test_randgold_final_dividend_is_not_barricks(self, barrick):
        """Ex 2019-01-02, Barrick's first day on GOLD: by (ticker, date) it would be a 17% 'dividend' on Barrick."""
        r = barrick[2]
        final = r[r["ex_dividend_date"] == RANDGOLD_FINAL]
        assert final["holder_id"].tolist() == [RANDGOLD] and final["refile_rule"].iloc[0] == "price_test"

    def test_barnes_dividends_are_never_handed_to_barrick(self, barrick):
        r = barrick[2]
        b = r[(r["filed_ticker"] == "B") & (r["ex_dividend_date"] < "2020-07-01")]
        assert len(b) == len(BARNES_Q) and (b["holder_id"] == BARNES).all() and (b["ticker"] == "B").all()

    def test_bbt_history_moves_from_tfc_to_bbt_and_the_copy_counts_once(self, bbt):
        r = bbt[2]
        bb = r[r["holder_id"] == TRUIST]
        on_bbt = bb[bb["ticker"] == "BBT"]
        assert sorted(on_bbt["ex_dividend_date"]) == sorted(_bday(d) for d in BBT_Q)     # each dividend once
        assert (bb.loc[bb["ticker"] == "TFC", "ex_dividend_date"] >= pd.Timestamp("2019-12-09")).all()

    def test_fund_gets_none_of_bbts_dividends(self, bbt):
        r = bbt[2]
        assert (r["holder_id"] != FUND).all()

    def test_berkshire_dividends_stay_with_berkshire(self, bbt):
        r = bbt[2]
        assert set(r.loc[r["filed_ticker"] == "BHLB", "holder_id"]) == {BEACON}
        assert set(r.loc[r["holder_id"] == BEACON, "ticker"]) == {"BHLB"}

    def test_override_wins(self, barrick):
        days, lines, _ = barrick
        _, _, divs = _barrick_world()
        one = divs[divs["ticker"] == "B"].iloc[[0]]
        ov = pd.DataFrame({"id": [one["id"].iloc[0]], "ticker": [""]})
        r = A.refile_actions(divs, lines, date_col="ex_dividend_date", kind="dividend", days=days, overrides=ov)
        assert one["id"].iloc[0] not in set(r["id"])


class TestReverseMergerAndCopies:
    def test_shared_cik_without_price_continuity_is_not_one_security(self):
        """Schering-Plough's CIK became the new Merck's in the 2009 reverse merger, but MRK's price series is
        old Merck's. A CIK link would hand Schering-Plough old Merck's dividends."""
        rows = (_line(7, 50.0, [("MRK", "2016-01-04", "2020-12-31")], {d: 0.45 for d in _q(2016) + _q(2017)})
                + _line(8, 25.0, [("SGP", "2016-01-04", "2017-06-29")], {}))
        days = pd.DataFrame(rows, columns=["ticker", "date", "open", "close"])
        asof = pd.DataFrame([("2016-06-30", "MRK", "MERCK & CO INC", "0000064978", None),
                             ("2018-06-29", "MRK", "MERCK & CO INC", "0000310158", "BBG000BPD168"),
                             ("2016-06-30", "SGP", "SCHERING PLOUGH CORP", "0000310158", None)],
                            columns=["asof", "ticker", "name", "cik", "composite_figi"])
        mt = pd.DataFrame([("MRK", "Merck & Co., Inc.", True, "0000310158", "BBG000BPD168")],
                          columns=["ticker", "name", "active", "cik", "composite_figi"])
        lines = A.holder_lines(days, asof, mt)
        ids = dict(zip(lines["ticker"], lines["holder_id"]))
        assert ids["MRK"] == "BBG000BPD168" and ids["SGP"] == "CIK__0000310158"
        assert lines.loc[lines["ticker"] == "MRK", "evidence"].iloc[0].startswith("repapered")
        divs = pd.DataFrame([_div("MRK", d, 0.45, 4, i) for i, d in enumerate(_q(2016) + _q(2017))])
        r = A.refile_actions(divs, lines, date_col="ex_dividend_date", kind="dividend", days=days)
        assert set(r["holder_id"]) == {"BBG000BPD168"} and set(r["ticker"]) == {"MRK"}

    def test_successor_copy_is_dropped_and_old_company_keeps_its_own(self):
        """T was AT&T Corp until 2005-11-18 and the former SBC from 2005-12-01. Massive files SBC's dividends
        under both SBC and T; AT&T Corp's own stay under T. Polygon gives SBC no CIK, so the link is on price."""
        att_corp = {d: 0.2375 for d in _q(2016, (2, 5, 8, 11), day=5)}      # all before its last session
        sbc = {d: 0.32 for d in _q(2016, (1, 4, 7, 10), day=12)}
        rows = (_line(9, 20.0, [("T", "2016-01-04", "2016-11-18")], att_corp)
                + _line(10, 25.0, [("SBC", "2016-01-04", "2016-11-30")], sbc))
        last = rows[-1]
        rows += [("T", d, last[3] * (1.002 if i == 0 else 1.0), last[3]) for i, d in enumerate(DAYS[(DAYS >= "2016-12-01") & (DAYS <= "2017-03-31")])]
        days = pd.DataFrame(rows, columns=["ticker", "date", "open", "close"])
        asof = pd.DataFrame([("2016-06-30", "T", "A T & T CORP (NEW)", "0000005907", None),
                             ("2016-06-30", "SBC", "SBC COMMUNICATIONS INC", None, None),
                             ("2017-03-31", "T", "AT&T INC. COM", "0000732717", "BBG000BSJK37")],
                            columns=["asof", "ticker", "name", "cik", "composite_figi"])
        mt = pd.DataFrame([("T", "AT&T Inc.", True, "0000732717", "BBG000BSJK37")],
                          columns=["ticker", "name", "active", "cik", "composite_figi"])
        lines = A.holder_lines(days, asof, mt)
        assert set(lines.loc[lines["ticker"] == "SBC", "holder_id"]) == {"BBG000BSJK37"}
        divs = pd.DataFrame([_div("T", d, 0.2375, 4, i) for i, d in enumerate(att_corp)]
                            + [_div("T", d, 0.32, 4, 10 + i) for i, d in enumerate(sbc)]
                            + [_div("SBC", d, 0.32, 4, i) for i, d in enumerate(sbc)])
        r = A.refile_actions(divs, lines, date_col="ex_dividend_date", kind="dividend", days=days)
        corp = r[r["holder_id"] == "CIK__0000005907"]
        assert sorted(corp["cash_amount"]) == [0.2375] * 4
        assert len(r[r["holder_id"] == "BBG000BSJK37"]) == 4                # SBC's, once each


class TestPullAndDerive:
    def test_pull_tickers_asof_is_resumable_per_date(self, tmp_path):
        calls = []

        def fetch(url, params=None):
            calls.append(params)
            return {"results": [{"ticker": "ABX", "name": "BARRICK GOLD CORP", "cik": "0000756894",
                                 "composite_figi": BARRICK}], "next_url": None}

        out = tmp_path / "asof.parquet"
        A.pull_tickers_asof(out, fetch, ["2010-06-30"])
        df = A.pull_tickers_asof(out, fetch, ["2010-06-30", "2011-06-30"])
        assert [c["date"] for c in calls] == ["2010-06-30", "2011-06-30"]          # 2010 not refetched
        assert sorted(df["asof"].dt.strftime("%Y-%m-%d").unique()) == ["2010-06-30", "2011-06-30"]
        assert all(c["market"] == "stocks" and c["limit"] == 1000 for c in calls)

    def test_derive_and_adjust_end_to_end(self, tmp_path, barrick):
        """Derive with --holder-lines, then factor_builder's batch path: ABX bars carry Barrick's id and its
        dividends, Randgold's GOLD bars only Randgold's, and Barrick's total return runs on across the switch."""
        days, lines, refiled = barrick
        _, _, divs = _barrick_world()
        md = tmp_path / "_market"
        md.mkdir()
        _market_tickers().to_parquet(md / bulk.MARKET_TICKERS, index=False)
        divs.to_parquet(md / bulk.MARKET_DIVIDENDS, index=False)
        empty = pd.DataFrame(columns=bulk.SPLIT_COLS)
        empty.to_parquet(md / bulk.MARKET_SPLITS, index=False)
        lines.to_parquet(md / bulk.HOLDER_LINES, index=False)
        refiled.to_parquet(md / bulk.MARKET_DIVIDENDS_REFILED, index=False)
        A.refile_actions(empty, lines, date_col="execution_date", kind="split").to_parquet(md / bulk.MARKET_SPLITS_REFILED, index=False)
        out = tmp_path / "all"
        bulk.derive_collection_refdata(md, ["ABX", "GOLD", "B"], out, holder_lines=True)
        sm, spl, div = (pd.read_parquet(out / f) for f in ("security_master.parquet", "stock_splits.parquet", "cash_dividends.parquet"))
        assert {"filed_ticker", "refile_rule", "refile_holder_id"} <= set(div.columns)
        spl = fb._assign_event_ids(spl, sm, ["execution_date"])
        div = fb._assign_event_ids(div, sm, ["ex_date", "ex_dividend_date"])
        px = days.assign(datetime=days["date"] + pd.Timedelta(hours=5), high=days[["open", "close"]].max(axis=1),
                         low=days[["open", "close"]].min(axis=1), volume=1000)[["ticker", "datetime", "open", "high", "low", "close", "volume"]]
        adj, _, _ = fb._adjust_frame(px, sm, spl, div, "both", workers=1)
        adj = adj.sort_values(["id", "event_day"])
        assert set(adj.loc[adj["ticker"] == "ABX", "id"]) == {BARRICK}
        gold = adj[adj["ticker"] == "GOLD"]
        assert set(gold.loc[gold["event_day"] <= "2018-12-28", "id"]) == {RANDGOLD}

        def ex_days(g):
            tr = g["tr_price_factor"].to_numpy()
            return int((np.abs(tr[:-1] / tr[1:] - 1) > 1e-12).sum())
        assert ex_days(adj[adj["id"] == RANDGOLD]) == len(RANDGOLD_ANNUAL)
        b = adj[adj["id"] == BARRICK]
        assert ex_days(b) == len(BARRICK_ABX_SEMI) + len(BARRICK_ABX_Q) + len(BARRICK_GOLD_Q) + len(BARRICK_B_Q)
        # across ABX 2018-12-31 -> GOLD 2019-01-02 the total return equals the price return (no dividend that day)
        sw = b[b["event_day"].isin([pd.Timestamp("2018-12-31"), pd.Timestamp("2019-01-02")])]
        assert sw["close_tr"].iloc[1] / sw["close_tr"].iloc[0] == pytest.approx(sw["close"].iloc[1] / sw["close"].iloc[0], rel=1e-12)

    def test_derive_without_lines_is_unchanged(self, tmp_path, barrick):
        md = tmp_path / "_market"
        md.mkdir()
        _market_tickers().to_parquet(md / bulk.MARKET_TICKERS, index=False)
        _barrick_world()[2].to_parquet(md / bulk.MARKET_DIVIDENDS, index=False)
        pd.DataFrame(columns=bulk.SPLIT_COLS).to_parquet(md / bulk.MARKET_SPLITS, index=False)
        bulk.derive_collection_refdata(md, ["ABX", "GOLD", "B"], tmp_path / "all")
        sm = pd.read_parquet(tmp_path / "all" / "security_master.parquet")
        assert dict(zip(sm["ticker"], sm["holder_id"])) == {"ABX": ABACUS, "B": BARRICK, "GOLD": GOLDCOM}
        assert "refile_rule" not in pd.read_parquet(tmp_path / "all" / "cash_dividends.parquet").columns
        with pytest.raises(FileNotFoundError):
            bulk.derive_collection_refdata(md, ["ABX"], tmp_path / "x", holder_lines=True)
