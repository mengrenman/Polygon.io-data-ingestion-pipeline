"""
Ticker letter case is data. Polygon spells the share class in it: `AAP` is Advance Auto Parts
(common stock), `AAp` is the Alcoa Inc. $3.75 preferred, and a lowercase suffix letter marks a
preferred series (`p`), a warrant (`w`) or a right (`r`). The ingester used to upper-case every
symbol, which merged 90 such pairs in the day lake into one series - 29,258 duplicated ticker-days
carrying two securities' bars, 0.13% of the rows.

These tests pin the contract: storage keeps the source spelling, and matching a user-supplied list
is exact, with a case-insensitive fallback only where it cannot be ambiguous.
"""
import gzip
import json
from pathlib import Path

import pandas as pd
import pytest

from polygon_ingest import ingest
from polygon_ingest.lake_io import load_polygonio_lake
from polygon_ingest.tickers import case_collisions, clean_list, resolve


def _ns(ts_utc: str) -> int:
    return pd.Timestamp(ts_utc, tz="UTC").value


HEADER = "ticker,volume,open,close,high,low,window_start,transactions\n"


def _write_csv(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt") as f:
        f.write(HEADER + "".join(rows))


def _src(tmp_path):
    """One session holding both securities that Polygon spells AAP and AAp, plus the ticker NA.

    The two AAP rows are the shape the real flat file has on 2003-09-10: the common stock trades in
    size, the preferred does not, and only the case tells them apart. `NA` is Nano Labs, which
    pandas reads as a null unless keep_default_na is off.
    """
    src = tmp_path / "src"
    ts = _ns("2024-01-16 05:00")
    _write_csv(src / "2024/01/2024-01-16.csv.gz", [
        f"AAP,1630200,34.31,34.35,34.55,34.20,{ts},1139\n",     # Advance Auto Parts, common
        f"AAp,201900,24.68,24.60,24.70,24.53,{ts},122\n",       # Alcoa $3.75 preferred
        f"AANw,500,1.10,1.20,1.25,1.05,{ts},4\n",               # a warrant
        f"NA,7000,5.00,5.10,5.20,4.90,{ts},40\n",               # Nano Labs, not a missing value
    ])
    return src


class TestIngestKeepsSpelling:
    def test_both_securities_survive_as_distinct_symbols(self, tmp_path):
        out = tmp_path / "lake"
        ingest.run_ingest("day", _src(tmp_path), out, workers=1, quiet_console=True, layout="market")

        df = pd.read_parquet(out / "2024" / "01.parquet")
        assert sorted(df["ticker"]) == ["AANw", "AAP", "AAp", "NA"]
        # neither row was dropped, and they are not the same bar
        assert df.loc[df["ticker"] == "AAP", "volume"].item() == 1_630_200
        assert df.loc[df["ticker"] == "AAp", "volume"].item() == 201_900
        # the defect this fixes: one (ticker, date) per security
        df["date"] = df["datetime"].dt.tz_convert("US/Eastern").dt.normalize()
        assert df.duplicated(["ticker", "date"]).sum() == 0

    def test_ticker_na_is_a_symbol_not_a_missing_value(self, tmp_path):
        out = tmp_path / "lake"
        ingest.run_ingest("day", _src(tmp_path), out, workers=1, quiet_console=True, layout="market")
        df = pd.read_parquet(out / "2024" / "01.parquet")
        assert df["ticker"].isna().sum() == 0
        assert (df["ticker"] == "NA").sum() == 1

    def test_ticker_layout_keeps_the_case_in_the_directory_name(self, tmp_path):
        out = tmp_path / "lake"
        ingest.run_ingest("day", _src(tmp_path), out, workers=1, quiet_console=True,
                          watch=_watchlist(tmp_path, ["AAp", "AANw"]))
        assert sorted(p.name for p in out.iterdir() if p.is_dir()) == ["AANw", "AAp"]
        assert pd.read_parquet(out / "AAp" / "2024" / "01.parquet")["ticker"].tolist() == ["AAp"]


def _watchlist(tmp_path, symbols) -> Path:
    p = tmp_path / f"watch_{'_'.join(symbols)}.json"
    p.write_text(json.dumps(symbols))
    return p


class TestWatchlistMatching:
    def test_watchlist_is_exact_so_the_common_stock_does_not_claim_the_preferred(self, tmp_path):
        out = tmp_path / "lake"
        ingest.run_ingest("day", _src(tmp_path), out, workers=1, quiet_console=True, layout="market",
                          watch=_watchlist(tmp_path, ["AAP"]))
        assert pd.read_parquet(out / "2024" / "01.parquet")["ticker"].tolist() == ["AAP"]

    def test_watchlist_entry_selects_only_its_own_spelling(self, tmp_path):
        out = tmp_path / "lake"
        ingest.run_ingest("day", _src(tmp_path), out, workers=1, quiet_console=True, layout="market",
                          watch=_watchlist(tmp_path, ["AAp"]))
        assert pd.read_parquet(out / "2024" / "01.parquet")["ticker"].tolist() == ["AAp"]

    def test_ignore_case_opts_back_in_to_matching_on_letters_alone(self, tmp_path):
        out = tmp_path / "lake"
        ingest.run_ingest("day", _src(tmp_path), out, workers=1, quiet_console=True, layout="market",
                          watch=_watchlist(tmp_path, ["aap"]), ignore_case=True)
        assert sorted(pd.read_parquet(out / "2024" / "01.parquet")["ticker"]) == ["AAP", "AAp"]

    def test_only_flag_is_exact_too(self, tmp_path):
        out = tmp_path / "lake"
        ingest.run_ingest("day", _src(tmp_path), out, workers=1, quiet_console=True, layout="market", only="AAp")
        assert pd.read_parquet(out / "2024" / "01.parquet")["ticker"].tolist() == ["AAp"]

    def test_near_misses_are_reported_not_silently_dropped(self, tmp_path, capsys):
        out = tmp_path / "lake"
        ingest.run_ingest("day", _src(tmp_path), out, workers=1, quiet_console=True, layout="market",
                          watch=_watchlist(tmp_path, ["AAP", "MSFT"]))
        printed = capsys.readouterr().out
        assert "AAp" in printed                       # the symbol we skipped for differing only in case
        assert "MSFT" in printed                      # the watchlist entry that matched nothing


class TestTickerLayoutOnACaseFoldingFilesystem:
    """<root>/AAP and <root>/AAp are one directory on APFS/HFS+/NTFS, so one security's file would
    replace the other's. The run has to fail instead of losing rows."""

    def test_a_watchlist_with_two_spellings_is_refused_up_front(self, tmp_path, monkeypatch):
        monkeypatch.setattr(ingest, "fs_folds_case", lambda p: True)
        with pytest.raises(SystemExit, match="folds letter case"):
            ingest.run_ingest("day", _src(tmp_path), tmp_path / "lake", workers=1, quiet_console=True,
                              watch=_watchlist(tmp_path, ["AAP", "AAp"]))

    def test_two_spellings_reaching_the_writer_fail_the_run(self, tmp_path, monkeypatch):
        monkeypatch.setattr(ingest, "fs_folds_case", lambda p: True)
        with pytest.raises(Exception, match="AAP vs AAp|AAp vs AAP"):
            ingest.run_ingest("day", _src(tmp_path), tmp_path / "lake", workers=1, quiet_console=True)

    def test_market_layout_is_unaffected(self, tmp_path, monkeypatch):
        monkeypatch.setattr(ingest, "fs_folds_case", lambda p: True)
        out = tmp_path / "lake"
        ingest.run_ingest("day", _src(tmp_path), out, workers=1, quiet_console=True, layout="market")
        assert sorted(pd.read_parquet(out / "2024" / "01.parquet")["ticker"]) == ["AANw", "AAP", "AAp", "NA"]


class TestReadBack:
    def test_loader_returns_the_symbol_that_was_asked_for(self, tmp_path):
        out = tmp_path / "lake"
        ingest.run_ingest("day", _src(tmp_path), out, workers=1, quiet_console=True, layout="market")
        for want in ("AAP", "AAp"):
            got = load_polygonio_lake([want], "2024-01-01", "2024-01-31", out, granularity="day")
            assert got["ticker"].tolist() == [want]

    def test_ticker_layout_resolves_a_request_typed_in_the_wrong_case(self, tmp_path):
        out = tmp_path / "lake"
        ingest.run_ingest("day", _src(tmp_path), out, workers=1, quiet_console=True,
                          watch=_watchlist(tmp_path, ["NA", "AANw"]))
        # 'aanw' can only have meant AANw, so it resolves; the frame still carries Polygon's spelling
        got = load_polygonio_lake(["aanw"], "2024-01-01", "2024-01-31", out, granularity="day")
        assert got["ticker"].tolist() == ["AANw"]


class TestResolver:
    def test_exact_match_wins_over_a_case_insensitive_one(self):
        mapping, unresolved, ambiguous = resolve(["AAP"], ["AAP", "AAp"])
        assert mapping == {"AAP": "AAP"} and not unresolved and not ambiguous

    def test_unique_case_insensitive_match_is_accepted(self):
        mapping, _, _ = resolve(["aapl", "Msft"], ["AAPL", "MSFT"])
        assert mapping == {"aapl": "AAPL", "Msft": "MSFT"}

    def test_an_ambiguous_request_is_refused_rather_than_guessed(self):
        mapping, unresolved, ambiguous = resolve(["aap"], ["AAP", "AAp"])
        assert not mapping and not unresolved
        assert ambiguous == {"aap": ["AAP", "AAp"]}

    def test_case_collisions_names_the_pairs(self):
        assert case_collisions(["AAP", "AAp", "MSFT"]) == {"AAP": ["AAP", "AAp"]}

    def test_clean_list_strips_but_never_folds_case(self):
        assert clean_list([" AAp ", "AAP", "AAp", ""]) == ["AAp", "AAP"]
