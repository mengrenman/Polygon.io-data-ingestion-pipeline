"""
Refdata puller behaviour without network: retry/backoff on rate limits, hard-fail on unknown
tickers, and loud recording of tickers whose pull failed (a silently dropped splits row means an
unadjusted series across that ticker's splits downstream).
"""
import pandas as pd
import pytest
from polygon.exceptions import BadResponse

import polygon_pullers as pp


class TestRetryingCall:
    def test_rate_limit_backs_off_at_least_twelve_seconds(self, monkeypatch):
        sleeps: list[float] = []
        monkeypatch.setattr(pp.time, "sleep", lambda s: sleeps.append(s))
        calls = {"n": 0}

        def fn():
            calls["n"] += 1
            if calls["n"] < 3:   # what urllib3 raises once the client's own 429 retries are spent
                raise RuntimeError("HTTPSConnectionPool: Max retries exceeded (too many 429 error responses)")
            return "ok"

        assert pp._retrying_call(fn) == "ok"
        assert calls["n"] == 3
        assert len(sleeps) == 2 and all(s >= pp._RATE_LIMIT_MIN_SLEEP_SEC for s in sleeps)

    def test_not_found_is_not_retried(self, monkeypatch):
        sleeps: list[float] = []
        monkeypatch.setattr(pp.time, "sleep", lambda s: sleeps.append(s))

        def fn():
            raise BadResponse('{"status":"NOT_FOUND","message":"Ticker not found."}')

        with pytest.raises(BadResponse):
            pp._retrying_call(fn)
        assert sleeps == []

    def test_gives_up_after_retries(self, monkeypatch):
        monkeypatch.setattr(pp.time, "sleep", lambda s: None)

        def fn():
            raise ConnectionError("boom")

        with pytest.raises(ConnectionError):
            pp._retrying_call(fn, _retries=2)


class TestFailureRecording:
    def test_pull_splits_records_failed_ticker_and_keeps_the_rest(self, tmp_path, monkeypatch):
        monkeypatch.setattr(pp.time, "sleep", lambda s: None)

        class Split:
            def __init__(self, d, f, t):
                self.execution_date, self.split_from, self.split_to = d, f, t

        class FakeClient:
            def list_splits(self, ticker, **kw):
                if ticker == "BAD":
                    raise RuntimeError("too many 429 error responses")
                return iter([Split("2024-06-10", 1, 10)])

        monkeypatch.setattr(pp, "_client", lambda key: FakeClient())
        out = tmp_path / "stock_splits.parquet"
        df = pp.pull_splits(["GOOD", "BAD"], out_parquet=out, api_key="x")

        assert df["ticker"].tolist() == ["GOOD"]
        assert df["ratio"].tolist() == [10.0]
        failed = (tmp_path / "_splits_failed_tickers.txt").read_text()
        assert failed.startswith("BAD\t") and "429" in failed

    def test_pull_dividends_empty_result_keeps_schema_and_no_failure_file(self, tmp_path, monkeypatch):
        class FakeClient:
            def list_dividends(self, ticker, **kw):
                return iter([])

        monkeypatch.setattr(pp, "_client", lambda key: FakeClient())
        out = tmp_path / "cash_dividends.parquet"
        df = pp.pull_dividends(["NONE"], out_parquet=out, api_key="x")

        assert df.empty and "ex_date" in df.columns and out.exists()
        assert not (tmp_path / "_dividends_failed_tickers.txt").exists()


# ---------------------------------------------------------------------------
# Holder ids in the pullers: probe dates find a recycled ticker's previous company; ticker events give
# the symbol history per holder; refine_windows tightens a holder's window to when it used the symbol.
# ---------------------------------------------------------------------------
class _D:
    """Minimal TickerDetails / TickerChangeResults stand-in."""
    def __init__(self, **kw):
        self.__dict__.update(kw)


_NEW_GM = dict(ticker="GM", name="General Motors Company", composite_figi="BBG000NDYB67", share_class_figi="BBG001SM1DK6",
               cik="0001467858", active=True, list_date="2010-11-18", delisted_utc=None, type="CS", locale="us",
               currency_name="usd", primary_exchange="XNYS", market="stocks", updated=None)
_OLD_GM = dict(ticker="GM", name="GENERAL MOTORS CORP", composite_figi=None, share_class_figi=None, cik="0000040730",
               active=True, list_date=None, delisted_utc=None, type="CS", locale="us", currency_name="usd",
               primary_exchange="XNYS", market="stocks", updated=None)


class TestHolderIds:
    def test_holder_id_rule_matches_factor_builder(self):
        import factor_builder as fb
        for args in (("BBG1", "123", "x"), (None, "0000040730", "GM"), (None, None, "gm"), ("", "", "GM")):
            assert pp.holder_id(*args) == fb._holder_id(*args)

    def test_security_master_probe_dates_find_previous_holder(self, tmp_path, monkeypatch):
        class FakeClient:
            def get_ticker_details(self, ticker, date=None):
                if date == "2008-06-02":
                    return _D(**_OLD_GM)
                if date == "1990-01-02":
                    raise BadResponse('{"status":"NOT_FOUND","message":"Ticker not found."}')
                return _D(**_NEW_GM)          # today and 2012-01-03 -> the current company

        monkeypatch.setattr(pp, "_client", lambda key: FakeClient())
        df = pp.pull_security_master(["GM"], out_parquet=tmp_path / "sm.parquet", api_key="x",
                                     probe_dates=["2008-06-02", "2012-01-03", "1990-01-02"])

        assert sorted(df["holder_id"]) == ["BBG000NDYB67", "CIK__0000040730"]
        cur = df[df["holder_id"] == "BBG000NDYB67"].iloc[0]
        old = df[df["holder_id"] == "CIK__0000040730"].iloc[0]
        assert cur["holder_source"] == "current"
        assert cur["effective_start"] == pd.Timestamp("2010-11-18") and pd.isna(cur["effective_end"])
        assert cur["anchor_date"] == pd.Timestamp("2012-01-03")          # probe that resolved to the current company
        assert old["holder_source"] == "probe:2008-06-02"
        assert old["anchor_date"] == pd.Timestamp("2008-06-02") and pd.isna(old["effective_start"])
        assert pd.read_parquet(tmp_path / "sm.parquet").shape[0] == 2

    def test_probe_holder_without_figi_or_cik_is_ignored(self, tmp_path, monkeypatch):
        class FakeClient:
            def get_ticker_details(self, ticker, date=None):
                if date:
                    return _D(**{**_OLD_GM, "cik": None})
                return _D(**_NEW_GM)

        monkeypatch.setattr(pp, "_client", lambda key: FakeClient())
        df = pp.pull_security_master(["GM"], out_parquet=tmp_path / "sm.parquet", api_key="x", probe_dates=["2008-06-02"])
        assert df["holder_id"].tolist() == ["BBG000NDYB67"]

    def test_ticker_events_and_symbol_history(self, tmp_path, monkeypatch):
        class FakeClient:
            def get_ticker_events(self, ticker):
                return _D(name="Meta Platforms", composite_figi="BBG000MM2P62", cik="0001326801", events=[
                    {"ticker_change": {"ticker": "META"}, "type": "ticker_change", "date": "2022-06-09"},
                    {"ticker_change": {"ticker": "FB"}, "type": "ticker_change", "date": "2012-05-18"},
                ])

        monkeypatch.setattr(pp, "_client", lambda key: FakeClient())
        ev = pp.pull_ticker_events(["META"], out_parquet=tmp_path / "ev.parquet", api_key="x")
        assert ev["ticker"].tolist() == ["FB", "META"] and (ev["holder_id"] == "BBG000MM2P62").all()

        h = pp.symbol_history(ev).sort_values("start")
        assert h["ticker"].tolist() == ["FB", "META"]
        assert h.iloc[0]["end"] == pd.Timestamp("2022-06-08") and pd.isna(h.iloc[1]["end"])

    def test_events_not_found_is_recorded_not_raised(self, tmp_path, monkeypatch):
        class FakeClient:
            def get_ticker_events(self, ticker):
                raise BadResponse('{"status":"NOT_FOUND","message":"No events found for given ID"}')

        monkeypatch.setattr(pp, "_client", lambda key: FakeClient())
        ev = pp.pull_ticker_events(["ZZZ"], out_parquet=tmp_path / "ev.parquet", api_key="x")
        assert ev.empty and (tmp_path / "_events_failed_tickers.txt").read_text().startswith("ZZZ\tNOT_FOUND")

    def test_refine_windows_tightens_start_to_symbol_adoption(self):
        sm = pd.DataFrame({"ticker": ["META"], "holder_id": ["BBG000MM2P62"],
                           "effective_start": [pd.Timestamp("2012-05-18")], "effective_end": [pd.NaT]})
        hist = pd.DataFrame({"holder_id": ["BBG000MM2P62"] * 2, "ticker": ["FB", "META"],
                             "start": [pd.Timestamp("2012-05-18"), pd.Timestamp("2022-06-09")],
                             "end": [pd.Timestamp("2022-06-08"), pd.NaT]})
        out = pp.refine_windows(sm, hist)
        assert out["effective_start"].iloc[0] == pd.Timestamp("2022-06-09") and pd.isna(out["effective_end"].iloc[0])
        assert pp.refine_windows(sm, hist.iloc[0:0]).equals(sm)


class TestEventsDerivedHolders:
    EV = pd.DataFrame({"query_ticker": ["META", "META", "NVDA"], "holder_id": ["BBG000MM2P62", "BBG000MM2P62", "BBG000BBJQV0"],
                       "composite_figi": ["BBG000MM2P62", "BBG000MM2P62", "BBG000BBJQV0"], "cik": ["0001326801", "0001326801", "0001045810"],
                       "name": ["Meta Platforms", "Meta Platforms", "Nvidia Corp"], "event_type": "ticker_change",
                       "date": pd.to_datetime(["2022-06-09", "2012-05-18", "2003-09-10"]), "ticker": ["META", "FB", "NVDA"]})

    def test_holders_from_history_windows_and_confirmation(self):
        h = pp.holders_from_history(self.EV, pp.symbol_history(self.EV)).set_index(["ticker", "holder_id"])
        fb_row = h.loc[("FB", "BBG000MM2P62")]
        assert fb_row["effective_start"] == pd.Timestamp("2012-05-18") and fb_row["effective_end"] == pd.Timestamp("2022-06-08")
        assert bool(fb_row["start_confirmed"]) and bool(fb_row["end_confirmed"]) and fb_row["holder_source"] == "events"
        assert fb_row["name"] == "Meta Platforms" and fb_row["cik"] == "0001326801"
        meta_row = h.loc[("META", "BBG000MM2P62")]
        assert meta_row["effective_start"] == pd.Timestamp("2022-06-09") and bool(meta_row["start_confirmed"]) and pd.isna(meta_row["effective_end"])
        nvda = h.loc[("NVDA", "BBG000BBJQV0")]
        assert nvda["effective_start"] == pp.HISTORY_START and not bool(nvda["start_confirmed"])   # data-start artifact

    def test_merge_holders_confirmed_start_wins_over_list_date(self):
        current = pd.DataFrame([{**{c: None for c in pp.SM_COLUMNS}, "ticker": "META", "holder_id": "BBG000MM2P62",
                                 "holder_source": "current", "name": "Meta Platforms, Inc.", "list_date": pd.Timestamp("2012-05-18"),
                                 "effective_start": pd.Timestamp("2012-05-18"), "effective_end": pd.NaT,
                                 "start_confirmed": False, "end_confirmed": False}])
        sm = pp.merge_holders(current, pp.holders_from_history(self.EV, pp.symbol_history(self.EV)))
        meta = sm[sm["ticker"] == "META"].iloc[0]
        assert meta["effective_start"] == pd.Timestamp("2022-06-09") and bool(meta["start_confirmed"])
        assert meta["name"] == "Meta Platforms, Inc."                       # descriptive fields from the current row
        assert sorted(sm["ticker"]) == ["FB", "META", "NVDA"]               # the FB window row was added

    def test_open_end_beats_confirmed_delisting_of_same_company(self):
        rows = [{**{c: None for c in pp.SM_COLUMNS}, "ticker": "RLST", "holder_id": "R1", "holder_source": "market:active",
                 "effective_start": pd.NaT, "effective_end": pd.NaT, "start_confirmed": False, "end_confirmed": False},
                {**{c: None for c in pp.SM_COLUMNS}, "ticker": "RLST", "holder_id": "R1", "holder_source": "market:delisted",
                 "effective_start": pd.NaT, "effective_end": pd.Timestamp("2015-03-02"), "start_confirmed": False, "end_confirmed": True}]
        out = pp._dedupe_holders(pd.DataFrame(rows))
        assert len(out) == 1 and pd.isna(out["effective_end"].iloc[0]) and not bool(out["end_confirmed"].iloc[0])
