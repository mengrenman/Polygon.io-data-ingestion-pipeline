"""
Bulk (market-wide) pullers without network: pagination via next_url, 429 backoff, incremental merge by
Polygon id, holder ids from active + delisted ticker records, and derived collection files in the
per-ticker schema that factor_builder's prep functions accept.
"""
import io
import urllib.error

import pandas as pd
import pytest

import factor_builder as fb
import polygon_pullers as pp
import polygon_pullers.bulk as bulk


def fake_fetch_from(series):
    """series: {key: [page_rows, ...]}; key = path or path|active. First call carries params, later calls follow
    next_url which encodes '<key>|<page>' in a cursor. Records every (url, params) call."""
    calls = []

    def fetch(url, params=None):
        calls.append((url, params))
        if "cursor=" in url:
            key, page = url.split("cursor=")[1].split("|")
            page = int(page)
        else:
            path = url.replace(bulk.BASE, "").split("?")[0]
            key = path + ("|" + params["active"] if params and "active" in params else "")
            page = 0
        pages = series[key]
        nxt = f"{bulk.BASE}/x?cursor={key}|{page + 1}" if page + 1 < len(pages) else None
        return {"status": "OK", "results": pages[page], "next_url": nxt}

    fetch.calls = calls
    return fetch


class TestPagination:
    def test_iter_results_follows_next_url_and_drops_params(self):
        fetch = fake_fetch_from({"/v3/reference/splits": [[{"id": "a"}], [{"id": "b"}], [{"id": "c"}]]})
        pages = list(bulk.iter_results("/v3/reference/splits", {"limit": 1000}, fetch))
        assert [r["id"] for p in pages for r in p] == ["a", "b", "c"]
        assert len(fetch.calls) == 3
        assert fetch.calls[0][1] == {"limit": 1000} and fetch.calls[1][1] is None and fetch.calls[2][1] is None

    def test_make_fetch_backs_off_on_429(self, monkeypatch):
        sleeps, attempts = [], {"n": 0}
        monkeypatch.setattr(bulk.time, "sleep", lambda s: sleeps.append(s))

        class Resp:
            def __enter__(self): return self
            def __exit__(self, *a): return False
            def read(self): return b'{"status":"OK","results":[]}'

        def urlopen(req, timeout=None):
            attempts["n"] += 1
            assert req.get_header("Authorization") == "Bearer k"
            assert "apiKey" not in req.full_url
            if attempts["n"] <= 2:
                raise urllib.error.HTTPError(req.full_url, 429, "Too Many Requests", {}, io.BytesIO(b""))
            return Resp()

        monkeypatch.setattr(bulk.urllib.request, "urlopen", urlopen)
        out = bulk.make_fetch("k")(bulk.BASE + "/v3/reference/splits", {"limit": 1000})
        assert out["results"] == [] and attempts["n"] == 3
        assert len(sleeps) == 2 and all(s >= pp._RATE_LIMIT_MIN_SLEEP_SEC for s in sleeps)

    def test_make_fetch_raises_on_4xx_other_than_429(self, monkeypatch):
        def urlopen(req, timeout=None):
            raise urllib.error.HTTPError(req.full_url, 403, "Forbidden", {}, io.BytesIO(b""))
        monkeypatch.setattr(bulk.urllib.request, "urlopen", urlopen)
        with pytest.raises(urllib.error.HTTPError):
            bulk.make_fetch("k")(bulk.BASE + "/v3/reference/splits", None)


ACTIVE = [{"ticker": "NVDA", "name": "Nvidia Corp", "active": True, "type": "CS", "market": "stocks", "locale": "us",
           "primary_exchange": "XNAS", "currency_name": "usd", "cik": "0001045810", "composite_figi": "BBG000BBJQV0",
           "share_class_figi": "BBG001S5TZJ6", "delisted_utc": None, "last_updated_utc": "2026-09-14T00:00:00Z"},
          {"ticker": "GM", "name": "General Motors Company", "active": True, "type": "CS", "market": "stocks", "locale": "us",
           "primary_exchange": "XNYS", "currency_name": "usd", "cik": "0001467858", "composite_figi": "BBG000NDYB67",
           "share_class_figi": None, "delisted_utc": None, "last_updated_utc": "2026-09-14T00:00:00Z"}]
DELISTED = [{"ticker": "GM", "name": "OLD GM CORP", "active": False, "type": "CS", "market": "stocks", "locale": "us",
             "primary_exchange": "XNYS", "currency_name": "usd", "cik": "0000040730", "composite_figi": None,
             "share_class_figi": None, "delisted_utc": "2009-07-10T00:00:00Z", "last_updated_utc": "2009-07-10T00:00:00Z"},
            {"ticker": "FB", "name": "Meta Platforms", "active": False, "type": "CS", "market": "stocks", "locale": "us",
             "primary_exchange": "XNAS", "currency_name": "usd", "cik": "0001326801", "composite_figi": "BBG000MM2P62",
             "share_class_figi": None, "delisted_utc": "2022-06-09T00:00:00Z", "last_updated_utc": "2022-06-09T00:00:00Z"}]
DELISTED += [{"ticker": "NVDA", "name": "NVDA OLD LISTING RECORD", "active": False, "type": None, "market": "stocks", "locale": "us",
              "primary_exchange": "XNAS", "currency_name": "usd", "cik": None, "composite_figi": None,
              "share_class_figi": None, "delisted_utc": "2001-01-05T05:00:00Z", "last_updated_utc": "2025-01-16T00:00:00Z"},
             {"ticker": "RLST", "name": "Relisted Co", "active": False, "type": "CS", "market": "stocks", "locale": "us",
              "primary_exchange": "XNYS", "currency_name": "usd", "cik": "0000777777", "composite_figi": "BBGRLST0001",
              "share_class_figi": None, "delisted_utc": "2015-03-02T05:00:00Z", "last_updated_utc": "2015-03-02T00:00:00Z"}]
ACTIVE += [{"ticker": "RLST", "name": "Relisted Co", "active": True, "type": "CS", "market": "stocks", "locale": "us",
            "primary_exchange": "XNYS", "currency_name": "usd", "cik": "0000777777", "composite_figi": "BBGRLST0001",
            "share_class_figi": None, "delisted_utc": None, "last_updated_utc": "2026-09-14T00:00:00Z"}]
SPLITS = [{"id": "s1", "ticker": "NVDA", "execution_date": "2021-07-20", "split_from": 1, "split_to": 4},
          {"id": "s2", "ticker": "NVDA", "execution_date": "2024-06-10", "split_from": 1, "split_to": 10},
          {"id": "s3", "ticker": "AAPL", "execution_date": "2020-08-31", "split_from": 1, "split_to": 4}]
DIVS = [{"id": "d1", "ticker": "NVDA", "ex_dividend_date": "2024-06-11", "pay_date": "2024-06-28", "record_date": "2024-06-11",
         "declaration_date": "2024-05-22", "cash_amount": 0.01, "currency": "USD", "frequency": 4, "dividend_type": "CD"},
        {"id": "d2", "ticker": "GM", "ex_dividend_date": "2015-03-09", "pay_date": "2015-03-27", "record_date": "2015-03-11",
         "declaration_date": "2015-02-04", "cash_amount": 0.36, "currency": "USD", "frequency": 4, "dividend_type": "CD"}]


class TestMarketTables:
    def test_tickers_holders_and_delisting_windows(self, tmp_path):
        fetch = fake_fetch_from({"/v3/reference/tickers|true": [ACTIVE], "/v3/reference/tickers|false": [DELISTED]})
        df = bulk.pull_market_tickers(tmp_path / "t.parquet", fetch)
        assert len(df) == len(ACTIVE) + len(DELISTED) and (tmp_path / "t.parquet").exists()
        gm = df[df["ticker"] == "GM"].sort_values("active", ascending=False)
        assert gm["holder_id"].tolist() == ["BBG000NDYB67", "CIK__0000040730"]
        assert pd.isna(gm.iloc[0]["delisted_utc"]) and gm.iloc[1]["delisted_utc"] == pd.Timestamp("2009-07-10")
        assert df[df["ticker"] == "FB"]["holder_id"].iloc[0] == "BBG000MM2P62"   # same company as META

    def test_splits_ratio_and_incremental_merge_by_id(self, tmp_path):
        out = tmp_path / "s.parquet"
        fetch = fake_fetch_from({"/v3/reference/splits": [SPLITS[:2], SPLITS[2:]]})
        df = bulk.pull_market_splits(out, fetch)
        assert df["ratio"].tolist() == [4.0, 4.0, 10.0] and fetch.calls[0][1].get("execution_date.gte") is None
        # refresh: s2 corrected + s4 new
        fetch2 = fake_fetch_from({"/v3/reference/splits": [[{**SPLITS[1], "split_to": 20},
                                                             {"id": "s4", "ticker": "MSFT", "execution_date": "2025-01-02", "split_from": 1, "split_to": 2}]]})
        df2 = bulk.pull_market_splits(out, fetch2, since="2024-05-01")
        assert fetch2.calls[0][1]["execution_date.gte"] == "2024-05-01"
        assert len(df2) == 4 and df2.loc[df2["id"] == "s2", "ratio"].iloc[0] == 20.0

    def test_dividends_schema_and_incremental_since(self, tmp_path):
        out = tmp_path / "d.parquet"
        df = bulk.pull_market_dividends(out, fake_fetch_from({"/v3/reference/dividends": [DIVS]}))
        assert list(df.columns) == bulk.DIVIDEND_COLS and df["ex_dividend_date"].dtype.kind == "M"
        assert bulk._incremental_since(out, "ex_dividend_date") == (pd.Timestamp("2024-06-11") - pd.Timedelta(days=30)).strftime("%Y-%m-%d")

    def test_pull_market_refdata_incremental_by_default(self, tmp_path):
        series = {"/v3/reference/tickers|true": [ACTIVE], "/v3/reference/tickers|false": [DELISTED],
                  "/v3/reference/splits": [SPLITS], "/v3/reference/dividends": [DIVS]}
        bulk.pull_market_refdata(tmp_path, fake_fetch_from(series))
        fetch = fake_fetch_from(series)
        bulk.pull_market_refdata(tmp_path, fetch)
        params = {u.split("?")[0].replace(bulk.BASE, ""): p for u, p in fetch.calls if p}
        assert params["/v3/reference/splits"]["execution_date.gte"] == "2024-05-11"     # 2024-06-10 - 30 days
        assert params["/v3/reference/dividends"]["ex_dividend_date.gte"] == "2024-05-12"
        fetch3 = fake_fetch_from(series)
        bulk.pull_market_refdata(tmp_path, fetch3, full=True)
        assert all("execution_date.gte" not in (p or {}) for _, p in fetch3.calls)


class TestDeriveCollection:
    def _market(self, tmp_path):
        series = {"/v3/reference/tickers|true": [ACTIVE], "/v3/reference/tickers|false": [DELISTED],
                  "/v3/reference/splits": [SPLITS], "/v3/reference/dividends": [DIVS]}
        bulk.pull_market_refdata(tmp_path / "_market", fake_fetch_from(series))
        return tmp_path / "_market"

    def test_filters_to_universe_in_per_ticker_schema(self, tmp_path):
        m = self._market(tmp_path)
        summary = bulk.derive_collection_refdata(m, ["nvda", "GM", "ZZZ"], tmp_path / "coll")
        assert summary["missing"] == ["ZZZ"] and (tmp_path / "coll" / "_missing_tickers.txt").read_text() == "ZZZ\n"
        assert summary["multi_holder_tickers"] == 1                       # GM: active + delisted holder

        sm = pd.read_parquet(tmp_path / "coll" / "security_master.parquet")
        assert list(sm.columns) == pp.SM_COLUMNS
        gm = sm[sm["ticker"] == "GM"].set_index("holder_id")
        assert gm.loc["BBG000NDYB67", "holder_source"] == "market:active" and pd.isna(gm.loc["BBG000NDYB67", "effective_end"])
        assert gm.loc["CIK__0000040730", "effective_end"] == pd.Timestamp("2009-07-10") and pd.isna(gm.loc["CIK__0000040730", "effective_start"])

        spl = pd.read_parquet(tmp_path / "coll" / "stock_splits.parquet")
        assert spl["ticker"].tolist() == ["NVDA", "NVDA"] and list(spl.columns) == ["ticker", "execution_date", "split_from", "split_to", "ratio"]
        div = pd.read_parquet(tmp_path / "coll" / "cash_dividends.parquet")
        assert list(div.columns) == ["ticker", "ex_date", "pay_date", "cash_amount", "declaration_date", "record_date", "frequency"]
        assert sorted(div["ticker"]) == ["GM", "NVDA"]

        # downstream prep accepts the derived files and keys events by holder
        S = fb._prep_splits(fb._assign_event_ids(spl, sm, ["execution_date"]))
        assert (S["event_id"] == "BBG000BBJQV0").all()
        D = fb._prep_dividends(fb._assign_event_ids(div, sm, ["ex_date", "ex_dividend_date"]))
        assert D.set_index("ticker")["event_id"].to_dict() == {"GM": "BBG000NDYB67", "NVDA": "BBG000BBJQV0"}

    def test_recycled_ticker_rows_split_by_delisting_date(self, tmp_path):
        m = self._market(tmp_path)
        bulk.derive_collection_refdata(m, ["GM"], tmp_path / "coll")
        sm = pd.read_parquet(tmp_path / "coll" / "security_master.parquet")
        px = pd.DataFrame({"ticker": "GM", "event_day": pd.to_datetime(["2005-03-01", "2009-07-10", "2010-11-18", "2015-01-02"])})
        assert fb._assign_holder_ids(px, sm, "event_day").tolist() == ["CIK__0000040730", "CIK__0000040730", "BBG000NDYB67", "BBG000NDYB67"]

    def test_probe_holders_merge_into_security_master(self, tmp_path):
        m = self._market(tmp_path)
        extra = pd.DataFrame([{**{c: None for c in pp.SM_COLUMNS}, "ticker": "NVDA", "holder_id": "CIK__0000999999",
                               "holder_source": "probe:2004-01-02", "name": "Some Prior Co", "cik": "0000999999",
                               "anchor_date": pd.Timestamp("2004-01-02")}])
        summary = bulk.derive_collection_refdata(m, ["NVDA"], tmp_path / "coll", extra_holders=extra)
        sm = pd.read_parquet(tmp_path / "coll" / "security_master.parquet")
        assert summary["multi_holder_tickers"] == 1 and sorted(sm["holder_id"]) == ["BBG000BBJQV0", "CIK__0000999999"]

    def test_identifierless_delisted_record_is_not_a_holder_but_is_reported(self, tmp_path):
        m = self._market(tmp_path)
        summary = bulk.derive_collection_refdata(m, ["NVDA"], tmp_path / "coll")
        sm = pd.read_parquet(tmp_path / "coll" / "security_master.parquet")
        assert sm["holder_id"].tolist() == ["BBG000BBJQV0"] and summary["multi_holder_tickers"] == 0
        assert summary["ambiguous_delisted"] == ["NVDA"]
        amb = pd.read_csv(tmp_path / "coll" / "_ambiguous_delisted_records.csv")
        assert amb["ticker"].tolist() == ["NVDA"] and amb["name"].iloc[0] == "NVDA OLD LISTING RECORD"

    def test_same_company_delisted_and_relisted_stays_one_open_holder(self, tmp_path):
        m = self._market(tmp_path)
        bulk.derive_collection_refdata(m, ["RLST"], tmp_path / "coll")
        sm = pd.read_parquet(tmp_path / "coll" / "security_master.parquet")
        assert len(sm) == 1 and sm["holder_id"].iloc[0] == "BBGRLST0001"
        assert pd.isna(sm["effective_end"].iloc[0]), "the active record's open end must win over the stale delisted one"
        px = pd.DataFrame({"ticker": "RLST", "event_day": pd.to_datetime(["2010-01-04", "2020-01-02"])})
        assert fb._assign_holder_ids(px, sm, "event_day").tolist() == ["BBGRLST0001"] * 2
