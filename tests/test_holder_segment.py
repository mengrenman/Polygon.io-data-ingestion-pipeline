"""
The segment a holder's bars stop in keeps the holder's id.

`_recycled_segments` cuts a single-holder ticker with an unconfirmed start at 60-day trading gaps and gives each
segment but one its own id, `NOFIGI__<T>#SEG<n>`. It used to spare the last segment, taking it for the master's
holder. When the holder has a confirmed end and the symbol prints again after it, the last segment is someone
else's: RET (Equity Securities Trust II, CIK 0001161676) stops on 2005-02-14, a day before its vendor end, and the
symbol trades again from 2008-09-10. Its 358 bars were `NOFIGI__RET#SEG0` and the 2008 ones `NOFIGI__RET`, so the
trust's id was on none of its bars. On the day lake, 55 tickers had that shape, with 22,329 rows under a `#SEG` id
that belong to the holder.

The rule now: the holder's segment is the last one that starts on or before a confirmed end (the last segment when
there is no confirmed end). Segments before it keep their #SEG ids. After it, the last segment keeps
'NOFIGI__<T>', the id of rows after a confirmed end, and any segment in between gets its own #SEG id.
"""
import gzip
import sys

import pandas as pd
import pytest

import factor_builder as fb
from polygon_ingest import ingest

RET = "CIK__0001161676"


def D(s):
    return pd.Timestamp(s).as_unit("ns")


def _sm_row(ticker, holder, *, end=None, source="market:delisted", end_confirmed=False, cik=None, figi=None):
    """One security-master row as the derive writes it: a vendor delisting date, start unknown."""
    return {"ticker": ticker, "holder_id": holder, "holder_source": source, "composite_figi": figi, "cik": cik,
            "effective_start": pd.NaT, "effective_end": pd.Timestamp(end) + pd.Timedelta(hours=5) if end else pd.NaT,
            "anchor_date": pd.NaT, "start_confirmed": False, "end_confirmed": end_confirmed}


def _bars(ticker, *ranges):
    days = pd.DatetimeIndex([d for a, b in ranges for d in pd.bdate_range(a, b)]).as_unit("ns")
    return pd.DataFrame({"ticker": ticker, "event_day": days})


def _no_events():
    return (pd.DataFrame({"ticker": pd.Series(dtype=object), "execution_date": pd.Series(dtype="datetime64[ns]"),
                          "ratio": pd.Series(dtype=float), "holder_id": pd.Series(dtype=object)}),
            pd.DataFrame({"ticker": pd.Series(dtype=object), "ex_date": pd.Series(dtype="datetime64[ns]"),
                          "cash_amount": pd.Series(dtype=float), "holder_id": pd.Series(dtype=object)}))


def _ids(px, sm, spl=None, div=None):
    """Ids per bar the way every builder path keys them: confirm ends, cut segments, attach."""
    if spl is None:
        spl, div = _no_events()
    smn, segs, spl2, div2 = fb._key_by_bars(px["ticker"].to_numpy(), px["event_day"], sm, spl, div)
    out = fb._attach_id(px.assign(datetime=px["event_day"] + pd.Timedelta(hours=5)), smn, segments=segs)
    return pd.Series(out["id"].to_numpy(), index=px["event_day"].to_numpy()), spl2, div2


def _by_range(ids, *ranges):
    return [sorted(set(ids[(ids.index >= D(a)) & (ids.index <= D(b))])) for a, b in ranges]


class TestHolderSegment:
    @pytest.mark.parametrize("end_confirmed", [True, False], ids=["deployed-master", "new-derive"])
    def test_ret_keeps_its_id_and_the_later_bars_are_an_unknown_holder(self, end_confirmed):
        sm = pd.DataFrame([_sm_row("RET", RET, end="2005-02-15", cik="0001161676", end_confirmed=end_confirmed)])
        px = _bars("RET", ("2004-06-01", "2005-02-14"), ("2008-09-10", "2008-10-10"))
        ids, _, _ = _ids(px, sm)
        assert _by_range(ids, ("2004-06-01", "2005-02-14"), ("2008-09-10", "2008-10-10")) == [[RET], ["NOFIGI__RET"]]

    def test_an_earlier_segment_keeps_its_own_id(self):
        # ANSL: a 2003-05 tenant, Ansoft from 2005-04-14 to its end (2006-06-05), then one bar on 2007-06-13
        sm = pd.DataFrame([_sm_row("ANSL", "CIK__0000791440", end="2006-06-05", cik="0000791440")])
        px = _bars("ANSL", ("2004-10-01", "2005-01-14"), ("2005-04-14", "2006-06-02"), ("2007-06-13", "2007-06-13"))
        ids, _, _ = _ids(px, sm)
        assert _by_range(ids, ("2004-10-01", "2005-01-14"), ("2005-04-14", "2006-06-02"), ("2007-06-13", "2007-06-13")) \
            == [["NOFIGI__ANSL#SEG0"], ["CIK__0000791440"], ["NOFIGI__ANSL"]]

    def test_segments_between_the_end_and_the_last_get_their_own_ids(self):
        # ROICU: the units' holder ends 2012-12-14; the symbol prints in three separate stretches after it
        sm = pd.DataFrame([_sm_row("ROICU", "CIK__0001407623", end="2012-12-14", cik="0001407623")])
        win = [("2011-01-03", "2012-06-08"), ("2013-01-07", "2013-01-11"), ("2013-03-18", "2013-04-10"),
               ("2013-07-02", "2013-09-18")]
        ids, _, _ = _ids(_bars("ROICU", *win), sm)
        assert _by_range(ids, *win) == [["CIK__0001407623"], ["NOFIGI__ROICU#SEG1"], ["NOFIGI__ROICU#SEG2"],
                                        ["NOFIGI__ROICU"]]

    def test_an_end_the_bars_do_not_back_changes_nothing(self):
        # the vendor end falls inside the last segment and the bars run on: the last segment is the holder's
        sm = pd.DataFrame([_sm_row("X", "BBGX", end="2005-03-01", figi="BBGX")])
        win = [("2004-01-02", "2004-03-31"), ("2005-01-03", "2005-06-30")]
        ids, _, _ = _ids(_bars("X", *win), sm)
        assert _by_range(ids, *win) == [["NOFIGI__X#SEG0"], ["BBGX"]]

    def test_a_holder_with_no_bars_before_its_end_names_no_segment(self):
        # every bar comes after the end, the first more than 60 days after it: no segment is the holder's
        sm = pd.DataFrame([_sm_row("X", "BBGX", end="2004-01-02", figi="BBGX")])
        win = [("2004-06-01", "2004-07-30"), ("2005-01-03", "2005-02-28")]
        ids, _, _ = _ids(_bars("X", *win), sm)
        assert _by_range(ids, *win) == [["NOFIGI__X#SEG0"], ["NOFIGI__X"]]

    def test_an_event_end_inside_the_holders_segment_keeps_two_later_holders_apart(self):
        # Meta left FB on 2022-06-08 (ticker events). If FB printed again after a gap, the rows after the end in
        # the holder's segment are 'NOFIGI__FB' already, so the later segment takes its own id
        sm = pd.DataFrame([_sm_row("FB", "BBG000MM2P62", end="2022-06-08", source="events", end_confirmed=True,
                                   figi="BBG000MM2P62")])
        px = _bars("FB", ("2022-05-02", "2022-07-29"), ("2023-01-03", "2023-02-28"))
        ids, _, _ = _ids(px, sm)
        assert _by_range(ids, ("2022-05-02", "2022-06-08"), ("2022-06-09", "2022-07-29"), ("2023-01-03", "2023-02-28")) \
            == [["BBG000MM2P62"], ["NOFIGI__FB"], ["NOFIGI__FB#SEG1"]]

    def test_segments_carry_no_id_where_rows_keep_theirs(self):
        sm = pd.DataFrame([_sm_row("RET", RET, end="2005-02-15", cik="0001161676")])
        px = _bars("RET", ("2004-06-01", "2005-02-14"), ("2008-09-10", "2008-10-10"))
        smn, _ = fb._confirm_ends_by_bars(sm, px["ticker"], px["event_day"])
        segs = fb._recycled_segments(px["ticker"].to_numpy(), px["event_day"], smn)
        assert segs["id"].isna().all() and segs["is_last"].tolist() == [False, True]
        # the raw master reads a vendor end as unconfirmed, so it keeps the old cut: pass the confirmed master
        assert fb._recycled_segments(px["ticker"].to_numpy(), px["event_day"], sm)["id"].tolist() \
            == ["NOFIGI__RET#SEG0", None]


class TestCorporateActionsFollow:
    WIN = [("2004-10-01", "2005-01-14"), ("2005-04-14", "2006-06-02"), ("2006-09-01", "2006-10-31"),
           ("2007-06-13", "2007-06-13")]

    def _setup(self):
        sm = pd.DataFrame([_sm_row("ANSL", "CIK__0000791440", end="2006-06-05", cik="0000791440")])
        px = _bars("ANSL", *self.WIN)
        div = pd.DataFrame({"ticker": "ANSL", "ex_date": pd.to_datetime(["2004-11-15", "2005-11-15", "2006-10-02",
                                                                          "2007-06-13"]).as_unit("ns"),
                            "cash_amount": [0.1, 0.2, 0.3, 0.4]})
        spl = pd.DataFrame({"ticker": "ANSL", "execution_date": pd.to_datetime(["2005-06-01"]).as_unit("ns"),
                            "split_from": [1.0], "split_to": [2.0]})
        return sm, px, fb._assign_event_ids(spl, sm, ["execution_date"]), fb._assign_event_ids(div, sm, ["ex_date"])

    def test_each_action_lands_on_the_id_of_the_bars_on_its_date(self):
        sm, px, spl, div = self._setup()
        ids, spl2, div2 = _ids(px, sm, spl, div)
        assert div2["holder_id"].tolist() == [ids[d] for d in div2["ex_date"]] \
            == ["NOFIGI__ANSL#SEG0", "CIK__0000791440", "NOFIGI__ANSL#SEG2", "NOFIGI__ANSL"]
        assert spl2["holder_id"].tolist() == ["CIK__0000791440"]

    def test_the_holders_actions_adjust_only_the_holders_bars(self):
        sm, px, spl, div = self._setup()
        px = px.assign(datetime=px["event_day"] + pd.Timedelta(hours=5), close=10.0, volume=100)
        out, _, _ = fb._adjust_frame(px.drop(columns="event_day"), sm, spl, div, "both")
        out = out.set_index("event_day").sort_index()
        holder = out.loc[D("2005-04-14"):D("2006-06-02")]
        assert set(holder["id"]) == {"CIK__0000791440"}
        assert holder.loc[:D("2005-05-31"), "split_price_factor"].eq(0.5).all()
        assert holder.loc[D("2005-06-01"):, "split_price_factor"].eq(1.0).all()
        assert holder.loc[:D("2005-11-14"), "tr_price_factor"].lt(1.0).all()          # its 2005 dividend
        assert holder.loc[D("2005-11-15"):, "tr_price_factor"].eq(1.0).all()          # nothing later reaches it
        rest = out.drop(holder.index)
        assert rest["split_price_factor"].eq(1.0).all()


class TestSnapshotLines:
    """polygon_pullers.asof (`--holder-lines`) gives every segment a snapshot observes a CONFIRMED window, which takes
    the ticker out of the gap cut. Its ids must agree with the cut's where both name a segment, so a RET-like
    ticker keeps its ids when the snapshot pull lands. Skipped until the branch carries polygon_pullers.asof."""

    @staticmethod
    def _master(lines):
        from polygon_pullers import SM_COLUMNS, merge_holders
        A = pytest.importorskip("polygon_pullers.asof")
        row = dict.fromkeys(SM_COLUMNS) | _sm_row("RET", RET, end="2005-02-15", cik="0001161676") | {
            "name": "EQUITY SECURITIES TRUST II", "active": False, "market": "stocks",
            "delisted_utc": pd.Timestamp("2005-02-15 05:00")}
        return merge_holders(pd.DataFrame([row])[SM_COLUMNS], A.lines_to_security_master(lines))

    @staticmethod
    def _lines(px, snapshots):
        A = pytest.importorskip("polygon_pullers.asof")
        days = px.rename(columns={"event_day": "date"}).assign(open=20.0, close=20.0)
        days.loc[days["date"] >= D("2008-01-01"), ["open", "close"]] = 10.0
        asof = pd.DataFrame(snapshots, columns=["asof", "ticker", "name", "cik", "composite_figi"])
        market = pd.DataFrame({"ticker": ["RET"], "name": ["EQUITY SECURITIES TRUST II"], "active": [False],
                               "cik": ["0001161676"], "composite_figi": [None], "holder_id": [RET],
                               "delisted_utc": [pd.Timestamp("2005-02-15")]})
        return A.holder_lines(days, asof, market)

    def test_a_snapshot_of_the_trust_leaves_the_ids_as_the_cut_gives_them(self):
        px = _bars("RET", ("2004-06-01", "2005-02-14"), ("2008-09-10", "2008-10-10"))
        lines = self._lines(px, [("2004-06-30", "RET", "EQUITY SECURITIES TRUST II", "0001161676", None)])
        assert lines["holder_id"].tolist() == [RET]                 # the 2008 segment is unobserved and last
        sm = self._master(lines)
        without, _, _ = _ids(px, pd.DataFrame([_sm_row("RET", RET, end="2005-02-15", cik="0001161676")]))
        with_lines, _, _ = _ids(px, sm)
        assert with_lines.equals(without)
        assert _by_range(with_lines, ("2004-06-01", "2005-02-14"), ("2008-09-10", "2008-10-10")) == [[RET], ["NOFIGI__RET"]]

    def test_a_snapshot_of_the_later_tenant_names_it_and_the_trust_keeps_its_bars(self):
        px = _bars("RET", ("2004-06-01", "2005-02-14"), ("2008-09-10", "2008-10-10"))
        lines = self._lines(px, [("2004-06-30", "RET", "EQUITY SECURITIES TRUST II", "0001161676", None),
                                 ("2008-09-30", "RET", "LATER TENANT INC", "0009999999", None)])
        ids, _, _ = _ids(px, self._master(lines))
        assert _by_range(ids, ("2004-06-01", "2005-02-14"), ("2008-09-10", "2008-10-10")) == [[RET], ["CIK__0009999999"]]


# ---------------------------------------------------------------------------
# End to end: RET through every adjuster path. The trust paid a dividend on 2005-01-20 and the 2008 holder one on
# 2008-09-24, each with the price dropping by the amount, so each holder's total-return series is flat.
# ---------------------------------------------------------------------------
HEADER = "ticker,volume,open,close,high,low,window_start,transactions\n"
HOLDER = [d.strftime("%Y-%m-%d") for d in pd.bdate_range("2005-01-03", "2005-02-14")]
LATER = [d.strftime("%Y-%m-%d") for d in pd.bdate_range("2008-09-10", "2008-10-10")]
DIVS = pd.DataFrame({"ticker": ["RET", "RET"], "ex_date": pd.to_datetime(["2005-01-20", "2008-09-24"]),
                     "cash_amount": [0.50, 0.20]})
SPLITS = pd.DataFrame({"ticker": ["NOPE"], "execution_date": pd.to_datetime(["2008-09-20"]), "split_from": [1], "split_to": [2]})


def _close(t, d):
    if t == "ZZZ":
        return 5.0
    if d <= "2005-02-14":
        return 20.0 if d < "2005-01-20" else 19.5
    return 10.0 if d < "2008-09-24" else 9.8


def _ns_et(ts):
    return pd.Timestamp(ts, tz="US/Eastern").tz_convert("UTC").value


def _src(tmp_path, tf):
    src = tmp_path / f"src_{tf}"
    for d in HOLDER + LATER:
        rows = []
        for t in ("RET", "ZZZ"):
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
    pd.DataFrame([_sm_row("RET", RET, end="2005-02-15", cik="0001161676", end_confirmed=end_confirmed),
                  _sm_row("ZZZ", "BBGZZZ", source="market:active", figi="BBGZZZ")]
                 ).to_parquet(ref / "security_master.parquet", index=False)
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
    df = df[df["ticker"] == "RET"].sort_values("datetime").reset_index(drop=True)
    return df.assign(day=df["datetime"].dt.tz_localize("UTC").dt.tz_convert("US/Eastern").dt.strftime("%Y-%m-%d"))


def _assert_ret(df):
    trust, later = df[df["day"] <= "2005-02-14"], df[df["day"] >= "2008-09-10"]
    assert len(trust) + len(later) == len(df)
    assert set(trust["id"]) == {RET} and set(later["id"]) == {"NOFIGI__RET"}
    assert not df["id"].str.contains("#SEG").any()
    assert trust["close_tr"].tolist() == pytest.approx([19.5] * len(trust))     # the trust's own dividend only
    assert later["close_tr"].tolist() == pytest.approx([9.8] * len(later))
    before_ex = trust.loc[trust["day"] < "2005-01-20", "tr_price_factor"]
    assert len(before_ex) and before_ex.tolist() == pytest.approx([0.975] * len(before_ex))


@pytest.mark.parametrize("end_confirmed", [True, False], ids=["deployed-master", "new-derive"])
class TestRetEndToEnd:
    def test_day_batch_single_process_and_sharded(self, tmp_path, monkeypatch, end_confirmed):
        lake = tmp_path / "day"
        ingest.run_ingest("day", _src(tmp_path, "day"), lake, workers=1, quiet_console=True, layout="market")
        ref = _refdir(tmp_path, end_confirmed)
        for w in ("1", "2"):
            _assert_ret(_build(lake, ref, tmp_path / f"adj{w}", "day", monkeypatch, "--workers", w))

    def test_minute_streaming_market_and_ticker_layouts(self, tmp_path, monkeypatch, end_confirmed):
        ref = _refdir(tmp_path, end_confirmed)
        for layout in ("market", "ticker"):
            lake = tmp_path / f"minute_{layout}"
            ingest.run_ingest("minute", _src(tmp_path, "minute"), lake, workers=1, quiet_console=True, layout=layout)
            _assert_ret(_build(lake, ref, tmp_path / f"adj_{layout}", "minute", monkeypatch, "--minute-stream",
                               "--stream-read-workers", "1"))
