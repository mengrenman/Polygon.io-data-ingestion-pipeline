"""
Refdata puller behaviour without network: retry/backoff on rate limits, hard-fail on unknown
tickers, and loud recording of tickers whose pull failed (a silently dropped splits row means an
unadjusted series across that ticker's splits downstream).
"""
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
