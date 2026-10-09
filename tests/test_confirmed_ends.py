"""
A vendor delisting date is a claim, not a confirmed end.

Polygon's tickers list gives CMCSK (Comcast Class A Special, CIK 0001166691) a delisting date of 2012-12-14,
and the symbol printed a bar every session until 2015. The derive used to mark every such date confirmed
(17,870 of 29,078 security-master rows), and the adjuster sends a single holder's rows after a confirmed end to
'NOFIGI__<TICKER>'. 164 tickers that kept trading were relabeled mid-history (17,726 `day_adj` rows); because
each id anchors its own factors, CMCSK's `close_tr` rose 8.2% on 2012-12-17 while `close` rose 2.5%
(`tr_price_factor` 0.8988 -> 0.9486, `split_price_factor` 1.0 on both sides).

The rule now: an end is confirmed by a ticker event, or by the ticker's own bars - nothing printed for
RECYCLE_GAP_DAYS or more after its last bar on or before the date, or nothing after it at all.
"""
import gzip
import sys

import pandas as pd
import pytest

import factor_builder as fb
from polygon_ingest import ingest

CMCSK = "CIK__0001166691"
END = "2012-12-14"


def D(s):
    return pd.Timestamp(s).as_unit("ns")


def _sm_row(ticker, holder, *, end=None, source="market:delisted", end_confirmed=True, cik=None, figi=None):
    """One security-master row the way the derive wrote it before the fix (a delisting marked confirmed)."""
    return {"ticker": ticker, "holder_id": holder, "holder_source": source, "composite_figi": figi, "cik": cik,
            "effective_start": pd.NaT, "effective_end": pd.Timestamp(end) + pd.Timedelta(hours=5) if end else pd.NaT,
            "anchor_date": pd.NaT, "start_confirmed": False, "end_confirmed": end_confirmed}


def _sm(*rows):
    return pd.DataFrame(list(rows))


def _bars(ticker, *ranges):
    days = pd.DatetimeIndex([d for a, b in ranges for d in pd.bdate_range(a, b)]).as_unit("ns")
    return pd.DataFrame({"ticker": ticker, "event_day": days})


SM_CMCSK = _sm(_sm_row("CMCSK", CMCSK, end=END, cik="0001166691"))


class TestConfirmEndsByBars:
    def test_cmcsk_kept_trading_so_its_vendor_end_is_not_confirmed(self):
        px = _bars("CMCSK", ("2012-09-04", "2013-03-28"))
        smn, confirmed = fb._confirm_ends_by_bars(SM_CMCSK, px["ticker"], px["event_day"])
        assert confirmed == set() and not bool(smn["end_confirmed"].item())
        assert set(fb._assign_holder_ids(px, smn, "event_day")) == {CMCSK}

    def test_old_masters_flag_is_ignored_for_vendor_rows(self):
        # the deployed master marks the vendor date confirmed; read as-is it still splits CMCSK at 2012-12-14
        px = _bars("CMCSK", ("2012-12-10", "2012-12-20"))
        assert set(fb._assign_holder_ids(px, SM_CMCSK, "event_day")) == {CMCSK}
        new_derive = SM_CMCSK.assign(end_confirmed=False)
        assert set(fb._assign_holder_ids(px, new_derive, "event_day")) == {CMCSK}

    def test_a_real_end_is_still_confirmed_and_cuts_the_later_rows(self):
        # RET: Equity Securities Trust II stops 2005-02-14 (vendor end 02-15); the symbol prints again in 2008
        sm = _sm(_sm_row("RET", "CIK__0001161676", end="2005-02-15", cik="0001161676", end_confirmed=False))
        px = _bars("RET", ("2004-06-01", "2005-02-14"), ("2008-09-10", "2008-10-10"))
        smn, confirmed = fb._confirm_ends_by_bars(sm, px["ticker"], px["event_day"])
        assert confirmed == {"RET"}
        ids = fb._assign_holder_ids(px, smn, "event_day")
        assert set(ids[px["event_day"] <= D("2005-02-15")]) == {"CIK__0001161676"}
        assert set(ids[px["event_day"] > D("2005-02-15")]) == {"NOFIGI__RET"}

    def test_the_gap_is_counted_from_the_last_bar_on_or_before_the_end(self):
        sm = _sm(_sm_row("X", "BBGX", end="2020-03-02", figi="BBGX"))
        last = D("2020-02-28")                                       # last bar before the vendor end
        for gap, want in ((59, set()), (60, {"X"})):
            px = pd.DataFrame({"ticker": "X", "event_day": [D("2020-02-03"), last, last + pd.Timedelta(days=gap)]})
            assert fb._confirm_ends_by_bars(sm, px["ticker"], px["event_day"], gap_days=60)[1] == want, gap

    def test_no_bar_before_the_end_counts_from_the_end_itself(self):
        # JACQW, RPRXW: warrants the vendor ends on 2012-12-14 whose first bars come 14 and 25 days later
        sm = _sm(_sm_row("JACQW", "CIK__0001548281", end=END, cik="0001548281"))
        px = _bars("JACQW", ("2012-12-28", "2013-06-28"))
        smn, confirmed = fb._confirm_ends_by_bars(sm, px["ticker"], px["event_day"])
        assert confirmed == set() and set(fb._assign_holder_ids(px, smn, "event_day")) == {"CIK__0001548281"}

    def test_a_ticker_that_never_trades_after_its_end_is_confirmed(self):
        px = _bars("CMCSK", ("2012-09-04", END))
        assert fb._confirm_ends_by_bars(SM_CMCSK, px["ticker"], px["event_day"])[1] == {"CMCSK"}

    def test_an_end_from_ticker_events_is_kept_without_bar_evidence(self):
        # Meta left FB on 2022-06-08 (META's ticker events); a symbol change is observed, not claimed, so a
        # successor printing at once does not undo it
        sm = _sm(_sm_row("FB", "BBG000MM2P62", end="2022-06-08", source="events", figi="BBG000MM2P62"))
        px = _bars("FB", ("2022-05-02", "2022-07-29"))
        smn, confirmed = fb._confirm_ends_by_bars(sm, px["ticker"], px["event_day"])
        assert confirmed == set() and bool(smn["end_confirmed"].item())
        ids = fb._assign_holder_ids(px, smn, "event_day")
        assert set(ids[px["event_day"] > D("2022-06-08")]) == {"NOFIGI__FB"}

    def test_gap_days_zero_confirms_nothing(self):
        px = _bars("CMCSK", ("2012-09-04", END))
        smn, confirmed = fb._confirm_ends_by_bars(SM_CMCSK, px["ticker"], px["event_day"], gap_days=0)
        assert confirmed == set() and not bool(smn["end_confirmed"].item())

    def test_multi_holder_tickers_are_left_to_the_window_logic(self):
        sm = _sm(_sm_row("GM", "CIK__0000040730", end="2009-07-10", cik="0000040730"),
                 _sm_row("GM", "BBG000NDYB67", source="market:active", end_confirmed=False, figi="BBG000NDYB67"))
        px = _bars("GM", ("2009-06-01", "2009-08-31"))
        assert fb._confirm_ends_by_bars(sm, px["ticker"], px["event_day"])[1] == set()


class TestEventsFollowTheConfirmedEnd:
    def test_a_dividend_after_a_confirmed_end_moves_with_the_rows(self):
        sm = _sm(_sm_row("RET", "CIK__0001161676", end="2005-02-15", cik="0001161676", end_confirmed=False))
        px = _bars("RET", ("2004-06-01", "2005-02-14"), ("2008-09-10", "2008-10-10"))
        div = fb._assign_event_ids(pd.DataFrame({"ticker": ["RET", "RET"], "ex_date": [D("2004-09-01"), D("2008-10-01")],
                                                 "cash_amount": [0.1, 0.2]}), sm, ["ex_date"])
        spl = fb._assign_event_ids(pd.DataFrame({"ticker": ["NOPE"], "execution_date": [D("2008-10-01")], "ratio": [2.0]}),
                                   sm, ["execution_date"])
        assert div["holder_id"].tolist() == ["CIK__0001161676"] * 2         # before the bars are seen: one holder
        smn, segs, spl2, div2 = fb._key_by_bars(px["ticker"].to_numpy(), px["event_day"], sm, spl, div)
        ids = fb._attach_id(px.assign(datetime=px["event_day"] + pd.Timedelta(hours=5)), smn, segments=segs)
        later = ids.loc[ids["event_day"] > D("2005-02-15"), "id"].unique().tolist()
        assert div2["holder_id"].iloc[1] == later[0]                       # the 2008 dividend is the 2008 rows'


# ---------------------------------------------------------------------------
# End to end: CMCSK through every adjuster path. Its price drops by exactly each dividend, so one holder's
# total-return series is flat; the defect showed up as a step at 2012-12-17, where the ids changed.
# ---------------------------------------------------------------------------
HEADER = "ticker,volume,open,close,high,low,window_start,transactions\n"
SESSIONS = [d.strftime("%Y-%m-%d") for d in pd.bdate_range("2012-12-03", "2013-01-15")
            if d.strftime("%m-%d") not in ("12-25", "01-01")]
DIVS = pd.DataFrame({"ticker": ["CMCSK", "CMCSK"], "ex_date": pd.to_datetime(["2012-12-12", "2013-01-02"]),
                     "cash_amount": [0.50, 0.50]})          # one before the vendor end, one after it
SPLITS = pd.DataFrame({"ticker": ["NOPE"], "execution_date": pd.to_datetime(["2012-12-20"]), "split_from": [1], "split_to": [2]})
SM_E2E = _sm(_sm_row("CMCSK", CMCSK, end=END, cik="0001166691"),
             _sm_row("ZZZ", "BBGZZZ", source="market:active", end_confirmed=False, figi="BBGZZZ"))


def _close(t, d):
    if t == "ZZZ":
        return 5.0
    return 35.0 if d < "2012-12-12" else (34.5 if d < "2013-01-02" else 34.0)


def _ns_et(ts):
    return pd.Timestamp(ts, tz="US/Eastern").tz_convert("UTC").value


def _src(tmp_path, tf):
    src = tmp_path / f"src_{tf}"
    for d in SESSIONS:
        rows = []
        for t in ("CMCSK", "ZZZ"):
            c = _close(t, d)
            for hhmm in (("00:00",) if tf == "day" else ("09:30", "15:59")):
                rows.append(f"{t},1000,{c},{c},{c},{c},{_ns_et(f'{d} {hhmm}')},1\n")
        p = src / d[:4] / d[5:7] / f"{d}.csv.gz"
        p.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(p, "wt") as f:
            f.write(HEADER + "".join(rows))
    return src


def _refdir(tmp_path, end_confirmed):
    ref = tmp_path / f"ref_{end_confirmed}"
    ref.mkdir()
    SM_E2E.assign(end_confirmed=[end_confirmed, False]).to_parquet(ref / "security_master.parquet", index=False)
    SPLITS.to_parquet(ref / "stock_splits.parquet", index=False)
    DIVS.to_parquet(ref / "cash_dividends.parquet", index=False)
    return ref


def _build(prices, ref, out, granularity, monkeypatch, *extra):
    monkeypatch.setattr(sys, "argv", ["factor_builder.py", "--prices", str(prices), "--refdir", str(ref),
                                      "--granularity", granularity, "--outdir", str(out), "--adjust", "both",
                                      "--materialize", "ohlc", "--write-workers", "1", "--no-copy-manifest", *extra])
    fb.main()
    files = [p for p in out.rglob("*.parquet") if not p.name.endswith(".idx.parquet")]
    df = pd.concat([pd.read_parquet(p) for p in files], ignore_index=True)
    return df[df["ticker"] == "CMCSK"].sort_values("datetime").reset_index(drop=True)


def _assert_one_holder_flat_total_return(cm):
    assert set(cm["id"]) == {CMCSK}
    assert cm["close_tr"].tolist() == pytest.approx([34.0] * len(cm))
    d = cm["datetime"].dt.tz_localize("UTC").dt.tz_convert("US/Eastern").dt.strftime("%Y-%m-%d")
    f = cm.groupby(d)["tr_price_factor"].first()
    assert f["2012-12-17"] == pytest.approx(f["2012-12-14"])            # no ex-date between: no step


@pytest.mark.parametrize("end_confirmed", [True, False], ids=["deployed-master", "new-derive"])
class TestCmcskEndToEnd:
    def test_day_batch_single_process_and_sharded(self, tmp_path, monkeypatch, end_confirmed):
        lake = tmp_path / "day"
        ingest.run_ingest("day", _src(tmp_path, "day"), lake, workers=1, quiet_console=True, layout="market")
        ref = _refdir(tmp_path, end_confirmed)
        for w in ("1", "2"):
            _assert_one_holder_flat_total_return(_build(lake, ref, tmp_path / f"adj{w}", "day", monkeypatch, "--workers", w))

    def test_minute_streaming_market_and_ticker_layouts(self, tmp_path, monkeypatch, end_confirmed):
        ref = _refdir(tmp_path, end_confirmed)
        for layout in ("market", "ticker"):
            lake = tmp_path / f"minute_{layout}"
            ingest.run_ingest("minute", _src(tmp_path, "minute"), lake, workers=1, quiet_console=True, layout=layout)
            cm = _build(lake, ref, tmp_path / f"adj_{layout}", "minute", monkeypatch, "--minute-stream",
                        "--stream-read-workers", "1")
            _assert_one_holder_flat_total_return(cm)
