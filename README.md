# Polygon.io Data Lake Builder

A pipeline to turn **Polygon.io flat files** into a local **Parquet lake**, pull **refdata** (splits/dividends/security master), and build **adjusted** lakes (split-adjusted + total-return). Scripts are reproducible and notebook-friendly.

<p align="center">
<img src="figures/adjust.png" alt="NVDA day bars: unadjusted close vs split-adjusted close vs total-return close, base 100" width="1000">
</p>

<p align="center"><sub>NVDA 2020–2024 day bars from the Polygon flat files; splits and dividends pulled with Step 4; adjusted with Step 5 (<code>-m ohlc</code>); rendered through <code>polygon_ingest.lake_io.load_series</code>. The lower panel is the total-return premium over the split-adjusted close: for a stock yielding ~0.1%/yr it must sit within a fraction of a percent of zero and step up only at ex-dates.</sub></p>

---

## Features

- Reproducible **unadjusted** lakes (minute/day).
- **Refdata** in a few hundred requests for any universe size: market-wide tickers (active + delisted), splits and dividends tables, refreshed incrementally; collection files derived by filtering. **Holder ids** (FIGI → CIK → ticker) keep a recycled ticker's previous company separate.
- **Adjusted** lakes (split-adjusted OHLC/VWAP/Volume + total-return).
- Helper scripts to build **ticker lists** (SPX, NDX, combined) or extract from flatfiles, and a **point-in-time universe** builder (membership per rebalance date from trailing dollar volume; survivorship-free).
- A schema-safe loader module for notebooks/QA plots.

> **Pipeline steps**
>
> 1) Download Polygon flat files  
> 2) Download/build ticker lists  
> 3) Build unadjusted Parquet lakes (**needs ticker lists**)  
> 4) Pull refdata from Polygon (**needs ticker lists**)  
> 5) Build adjusted Parquet lakes from unadjusted + refdata (**needs ticker lists**)

---

## Requirements

- Python 3.10+ (tested on 3.12)
- Runtime: `pandas`, `pyarrow`, `numpy`, `tqdm`, `typer`, `polygon-api-client` (all installed by `pip install -e .`)
- Extras: `[dev]` → `pytest`; `[notebooks]` → `matplotlib`, `ipykernel`, `jupyterlab`, `lxml` (QA notebooks and the Wikipedia ticker-list script)

Install in editable mode (venv):

```bash
python -m venv .venv && source .venv/bin/activate
pip install -U pip
pip install -e ".[dev,notebooks]"
```

or with conda (`environment.yml` provides the interpreter and installs the same extras):

```bash
conda env create -f environment.yml && conda activate poly_ingest
```

Run the regression tests (adjustment math, holder ids, layouts, pullers, universe):

```bash
pytest
```

Set your API key (used in Step 4):

- Add a repo-level `.env`:
  ```ini
  POLYGON_API_KEY=your_key_here
  ```
  or export manually:
  ```bash
  export POLYGON_API_KEY=your_key_here
  ```

---

## Repository Layout

```
repo_polygonio/
├─ pyproject.toml
├─ .gitignore                            # ignores .env, lakes, refdata, caches
├─ .env                                  # optional: POLYGON_API_KEY, etc. (git-ignored)
├─ src/
│  ├─ polygon_ingest/
│  │  ├─ __init__.py
│  │  ├─ ingest.py                       # CSV.GZ → Parquet lake (minute/day)
│  │  ├─ cli.py                          # `poly bars` CLI entry (ingestion)
│  │  ├─ lake_io.py                      # schema-safe readers for notebooks/QA
│  │  └─ universe.py                     # point-in-time universe: segments, eligibility, membership
│  │ 
│  └─ polygon_pullers/
│     ├─ __init__.py                     # per-ticker pullers (details/events/splits/dividends), holder ids
│     └─ bulk.py                         # market-wide tables + derived collection files (default)
│
├─ legacy_scripts/
│  ├─ polygon_ingest_day.py              # thin shim → run_ingest(tf="day")
│  ├─ polygon_ingest_minute.py           # thin shim → run_ingest(tf="minute")
│  ├─ run_pullers.py                     # orchestrates refdata pulls
│  ├─ factor_builder.py                  # builds adjusted lakes from unadj + refdata
│  └─ polygon_lake_loader.py             # optional CLI shim using lake_io
│
├─ scripts/
│  ├─ build_pit_universe.py              # point-in-time universe (membership per rebalance date)
│  ├─ build_adjusted_lake.sh             # unified builder (minute/day)
│  ├─ pull_ref_data.sh                   # wrapper to run `run_pullers.py` (loads .env)
│  ├─ build_index_universes.py           # build SPX/NDX/combined ticker lists
│  └─ extract_tickers_from_flatfiles.py  # discover tickers from CSV.GZ flatfiles
│
├─ data/
│  └─ ticker_lists/                      # generated universe lists (*.json / *.txt)
│
├─ refdata/
│  ├─ _market/                           # market-wide tickers / splits / dividends (generated, shared)
│  └─ spx_ndx_combined/                  # derived for the collection (SM/splits/divs)
│
├─ lake/                                 # unadjusted lakes (generated, git-ignored)
│  ├─ minute/<collection>/<TICKER>/<YYYY>/<MM>/<DD>.parquet
│  ├─ day/<collection>/<TICKER>/<YYYY>/<MM>.parquet
│  ├─ day/all/<YYYY>/<MM>.parquet         # --layout market: every ticker, one file per month
│  └─ minute/all/<YYYY>/<MM>/<DD>.parquet # --layout market: every ticker, one file per day (+ <DD>.idx.parquet)
│
├─ lake_adj/                             # adjusted lakes (generated, git-ignored)
│  ├─ minute/<collection>_adjusted/<TICKER>/<YYYY>/<MM>/<DD>.parquet
│  └─ day/<collection>_adjusted/<TICKER>/<YYYY>/<MM>.parquet
│
└─ notebooks/
   └─ 03_load_data_inspect_adjustment.ipynb  # QA: unadj vs split-adj vs TR
```

---

## Quickstart

### Step 1 — Download Polygon flat files

Download Polygon CSV.GZ drops and place them somewhere on disk, e.g.:

```bash
$HOME/data/polygonio_data/flatfiles/
```

> Tip: keep minute and day in separate subfolders if you have both.

---

### Step 2 — Build ticker lists

**Option A — Build SPX/NDX/Combined from Wikipedia** (today's constituents — survivorship-biased when applied to history; see Option C)
```bash
python scripts/build_index_universes.py --outdir data/ticker_lists
# writes: data/ticker_lists/{spx,ndx,spx_ndx_combined}.{json,txt}
```

**Option B — Extract tickers from your flatfiles**
```bash
python scripts/extract_tickers_from_flatfiles.py \
  --src $HOME/data/polygonio_data/flatfiles \
  --outdir data/ticker_lists \
  --name all_polygonio_tickers
# writes: data/ticker_lists/all_polygonio_tickers.{json,txt}
```

**Option C — Point-in-time universe (use this for research).** Options A and B are *static* lists. Today's S&P 500 applied back to 2003 contains only the companies that survived and grew into the index — the textbook look-ahead / survivorship bias — while the flat files themselves are survivorship-free (Lehman, Bear Stearns, Washington Mutual, the old GM are all in them). Build membership **per rebalance date from data available on that date** instead:

```bash
# needs the whole-universe day lake (Step 3, --layout market) and the market tickers table (Step 4)
python scripts/build_pit_universe.py \
  --lake ./lake/day/all \
  --market-tickers refdata/_market/market_tickers.parquet \
  --out data/universes/top1000_cs_monthly \
  --top-n 1000 --min-price 1 --exclude-tickers QQQ,VXX --emit-watchlist \
  --static-list data/ticker_lists/spx_ndx_combined.json     # optional: quantify what the static list misses
```

Each month-end, eligible common stocks are ranked by trailing 63-day dollar volume and the top N are members; a ticker must have traded within the last 5 days and have ≥ 40 observations in the window (fresh listings excluded). Recycled symbols are handled by splitting a ticker's history at trading gaps of ≥ 60 days (exchange-listed names print daily; Bear Stearns → an ETN under `BSC` took 69 days): the tickers table describes the *current* holder, so its type applies to the last segment only, and earlier segments are admitted unless the symbol looks like a derivative (`--untyped-policy`). That recovers Bear Stearns, Meta under `FB`, Wachovia, Sun, DirecTV — and also a fund that came back to its own symbol (QQQ 2003–04, VXX), which no table field can tell apart; `summary.json` lists the admitted untyped segments for review and `--exclude-tickers QQQ,VXX` removes them by hand. Outputs: `membership.parquet` (rebalance_date, ticker, rank, adv_usd, …), `segments.parquet`, `summary.json` (members per year, share of members that are no longer an active common stock today, share missing from the static list) and, with `--emit-watchlist`, `watchlist.json` — the union of all members, usable as `--watch` for a minute ingest. Use `polygon_ingest.universe.expand_daily()` to get the membership on every trading day. The universe is a **membership table you join at research time**, not an ingest filter: build the adjusted lake for the whole `day/all` lake (bulk refdata covers every ticker) and filter by (date, ticker) afterwards. Live result on 2003–2025: 262 rebalances, 4,171 distinct members; 55 % of the 2003 members are no longer an active common stock today.

---

### Step 3 — Create the **unadjusted** Parquet lakes (needs ticker lists)

Use the `poly` CLI (installed with this package) or the legacy shims.

**Using `poly` (example):**
```bash
# Day (unadjusted)
poly bars \
  --tf    day \
  --src   $HOME/data/polygonio_data/flatfiles/day \
  --out   ./lake/day/spx_ndx_combined \
  --watch data/ticker_lists/spx_ndx_combined.json

# Minute (unadjusted)
poly bars \
  --tf    minute \
  --src   $HOME/data/polygonio_data/flatfiles/minute \
  --out   ./lake/minute/spx_ndx_combined \
  --watch data/ticker_lists/spx_ndx_combined.json
```

**Using legacy shims (alternative):**
```bash
python legacy_scripts/polygon_ingest_day.py \
  --src   $HOME/data/polygonio_data/flatfiles/day \
  --out   ./lake/day/spx_ndx_combined \
  --watch data/ticker_lists/spx_ndx_combined.json

python legacy_scripts/polygon_ingest_minute.py \
  --src   $HOME/data/polygonio_data/flatfiles/minute \
  --out   ./lake/minute/spx_ndx_combined \
  --watch data/ticker_lists/spx_ndx_combined.json
```

Output layout (`<collection>` is the ticker-list name, e.g. `spx_ndx_combined`):
- `lake/day/<collection>/<TICKER>/<YYYY>/<MM>.parquet`
- `lake/minute/<collection>/<TICKER>/<YYYY>/<MM>/<DD>.parquet`

Flags: `--out` is the destination lake root, `--watch` is the ticker list (json/txt). Use `--only NVDA` for a single ticker and `--write-manifest` to emit a manifest the loader can use.

**Whole universe (every ticker in the flat files): use the market layout.** One file per ticker per month means ~2.4 million tiny files for the full market; `--layout market` writes one file per period holding all tickers instead — 264 monthly files for the day history, ticker-major with row-group statistics so a single symbol is pruned on read:

```bash
poly bars --tf day --layout market \
  --src $HOME/data/polygonio_data/flatfiles/day \
  --out ./lake/day/all          # -> lake/day/all/<YYYY>/<MM>.parquet, no --watch
```

The loaders and Step 5 detect the layout from the directory structure (`<YYYY>/` vs `<TICKER>/`), so nothing downstream needs a flag. Workers write each period's files as soon as it can no longer receive rows, so memory stays at about two periods per worker for either layout.

**Whole-universe minute lake.** `poly bars --tf minute --layout market` writes one file per trading day holding every ticker (`lake/minute/all/<YYYY>/<MM>/<DD>.parquet`, ticker-major, 128k-row row groups) plus a `<DD>.idx.parquet` sidecar (per ticker: first/last close, row count, row positions). That is 5,517 files for the full history instead of ~50 million per-ticker-day files. The streaming adjuster (Step 5, `-t minute`, layout auto-detected) reads each day file once, joins that day's split/dividend factors and holder ids by ticker, and writes the same layout; the sidecars make its day-edge scan instant, and readers use them to skip days a symbol is absent from. A one-day file is ~1.4 M rows (2024); expect roughly 150–200 GB for the full market.

---

### Step 4 — Pull **refdata** from Polygon (needs ticker lists)

Make sure `POLYGON_API_KEY` is set (via `.env` or env var) and run:

```bash
bash scripts/pull_ref_data.sh
```

This pulls **market-wide** reference tables once — a few hundred requests in total, the same for 3 tickers or 10,000 — into `refdata/_market/`, then **derives** the collection's files by filtering to your ticker list:

| `refdata/_market/` (shared) | `refdata/<collection>/` (derived) |
|---|---|
| `market_tickers.parquet` — every active **and delisted** stock: FIGI, CIK, delisting date, `holder_id` | `security_master.parquet` — one row per **(ticker, holder)** |
| `market_splits.parquet` — every split, with Polygon's row `id` | `stock_splits.parquet` |
| `market_dividends.parquet` — every cash dividend, with `id` | `cash_dividends.parquet` |
| | `_missing_tickers.txt`, `_ticker_normalization_map.csv` |

Re-running is **incremental**: splits and dividends are fetched from the latest date already held minus 30 days and merged by `id`; the tickers table is refreshed in full. Pass `--full` (via `bash scripts/pull_ref_data.sh --full`) to refetch everything.

A **holder** is the company behind a ticker, `holder_id` = composite FIGI → `CIK__<cik>` → `NOFIGI__<TICKER>`. A recycled symbol (General Motors Corp until 2009, General Motors Company from 2010-11-18) appears in the tickers table as one active and one delisted record and becomes two holders with a window boundary at the delisting date, so the second company's splits and dividends never touch the first one's prices. If the previous company was *renamed* before delisting, its delisted record is under its final symbol; use probe dates for those:

```bash
POLYGON_PROBE_DATES=2005-01-03,2012-01-03 bash scripts/pull_ref_data.sh   # +1 request per ticker per date
POLYGON_EVENTS=1 bash scripts/pull_ref_data.sh                            # +1 request per ticker: symbol history (FB → META), written to ticker_symbol_history.parquet
POLYGON_PER_TICKER=1 bash scripts/pull_ref_data.sh                        # the older per-ticker pullers (several requests per ticker)
POLYGON_REFDATA_ROOT=/data/parquet_lake/refdata bash scripts/pull_ref_data.sh   # keep refdata next to your lakes
```

Polygon returns no FIGI for some delisted holders, hence the CIK fallback. Point Step 5 at a non-default refdata location with `-r <dir>`.

**Symbols that changed hands without a trace.** A previous holder that left no record under the symbol is invisible to the tickers table: `FB` is now a ProShares ETF (listed 2024) and Polygon keeps no delisted `FB` record for Meta. `POLYGON_EVENTS=1` fixes this: each holder's ticker-change history becomes security-master rows per (holder, symbol) window — `FB` → Meta 2012-05-18..2022-06-08 from META's events — and a **confirmed** adoption date (a real ticker change, not Polygon's 2003-09-10 history start) stops a holder from claiming rows before it took the symbol; those rows fall to an unknown holder (`NOFIGI__FB`) instead of the wrong company. Include both the current and the former symbol in your ticker list to stitch a company across a rename.

---

### Step 5 — Build the **adjusted** lakes (needs ticker lists)

Use the unified builder:

```bash
# Day — write all split-adjusted columns + close_tr
scripts/build_adjusted_lake.sh -t day -c spx_ndx_combined -m ohlc \
  -p ./lake/day/spx_ndx_combined \
  -o ./lake_adj/day/spx_ndx_combined_adjusted

# Minute — narrow to a window if desired
scripts/build_adjusted_lake.sh -t minute -c spx_ndx_combined -m ohlc \
  -p ./lake/minute/spx_ndx_combined \
  -o ./lake_adj/minute/spx_ndx_combined_adjusted \
  -s 2023-01-01 -e 2023-12-31
```

Defaults when `-p` / `-o` are omitted. Note these point at `~/data/...`, **not** at the `./lake` output from Step 3, which is why the examples above pass them explicitly:
- Unadjusted input (`-p`): `~/data/polygonio_data/parquet_lake/<tf>_aggs_v1/<collection>`
- Adjusted output (`-o`): `~/data/polygonio_data/parquet_lake/<tf>_aggs_v1/<collection>_adjusted`
- Refdata: `refdata/<collection>/` (derived from `-c`)
- Workers: `-w` defaults to 90 (day mode); lower it to roughly your core count

`-m` (`--materialize`) options:
- `minimal` → only what’s needed (e.g., `close_tr`)
- `close` → adds `close_sa`
- `ohlc` → adds all `*_sa` (`open_sa`, `high_sa`, `low_sa`, `close_sa`, `vwap_sa`, `volume_sa`) + `close_tr` **(recommended)**

Adjusted output mirrors unadjusted (and is what the QA notebook expects):
- `lake_adj/day/<collection>_adjusted/<TICKER>/<YYYY>/<MM>.parquet`
- `lake_adj/minute/<collection>_adjusted/<TICKER>/<YYYY>/<MM>/<DD>.parquet`

---

## Notebook / QA

Use `notebooks/03_load_data_inspect_adjustment.ipynb` to load and plot:
- **Unadjusted `close`**
- **Split-adjusted `close_sa`**
- **Total-return `close_tr`**

The loader (`polygon_ingest.lake_io`) is schema-safe:
- Accepts `datetime` or `date/timestamp`.
- For **day** data, merges on **calendar date** (not exact time).
- Maps `*_split → *_sa` if adjusted files use that naming.

---

## Troubleshooting

- **`POLYGON_API_KEY` not found**  
  Put it in `.env` (repo root) or export it in your shell. `scripts/pull_ref_data.sh` auto-loads `.env`.

- **`429` / `Too Many Requests` during Step 4**  
  Your Polygon plan is rate-limited (the free tier allows 5 requests/minute). The default bulk mode needs only a few hundred requests in total; pace them with
  ```bash
  POLYGON_MIN_INTERVAL_SEC=12.5 bash scripts/pull_ref_data.sh   # or put the variable in .env
  ```
  (about an hour or two for the full market on the free tier, minutes on a paid plan). In per-ticker mode (`POLYGON_PER_TICKER=1`) tickers whose pull failed after retries are listed in `refdata/<collection>/_splits_failed_tickers.txt` / `_dividends_failed_tickers.txt`; do **not** build an adjusted lake while the splits list is non-empty — a ticker with no splits row comes out **unadjusted across its splits**.

- **Empty plots / empty merges**  
  Double-check notebook paths match your lakes. For **day**, both lakes must overlap on dates.

- **Missing `close_sa` in adjusted files**  
  Build with `-m ohlc`. The loader also maps `close_split → close_sa` when present.

- **Mixed schemas across years**  
  `lake_io` inspects each Parquet file’s schema and only reads columns that exist.

---

## Notes & Conventions

- **Time zone:** the `datetime` column in the unadjusted lake is tz-aware **US/Eastern**. Lake files are partitioned on the **ET trading date** (`<YYYY>/<MM>/<DD>`), and split/dividend factors are aligned on that same date, so after-hours bars (up to 20:00 ET) stay with their session instead of spilling into the next UTC day.
- **Precision:** prices are stored as `float64`.
- **Layouts:** `ticker` (`<root>/<TICKER>/<YYYY>/<MM>[/<DD>].parquet`, default, best for a few hundred symbols) or `market` (`<root>/<YYYY>/<MM>[/<DD>].parquet`, all tickers per file, for the whole universe and cross-sectional work such as point-in-time universes; minute day files carry a `.idx.parquet` per-ticker sidecar). Detected automatically by the loaders and by both adjuster paths; `factor_builder.py --layout` / `build_adjusted_lake.sh -L` override.
- **Ids:** every adjusted row carries `id`, the holder (company) of the ticker on that date, keyed like the security master. Splits and dividends are matched by `id`, never by ticker alone, so a recycled ticker's previous company keeps only its own corporate actions and anchors its own adjustment factors. A ticker with a single known holder gets that id for all its rows regardless of dates (windows only disambiguate between holders), except rows before a *confirmed* symbol-adoption date or after a *confirmed* end (real ticker changes / delistings from the events and tickers tables), which are left to an unknown holder. An unconfirmed `list_date` never cuts, so an imprecise date cannot split one company's history. To stitch one company across a symbol change (`FB` → `META`), include both symbols in the ingest watchlist; `ticker_symbol_history.parquet` lists them.
- **Total return (`close_tr`)**: built over split-adjusted prices, reinvesting cash dividends on ex-date. Sanity check: on a day with no dividend `close_tr` moves exactly like `close_sa`, and across an ex-date where the price drops by exactly the dividend the `close_tr` return is 0. Polygon reports dividends in raw dollars, so amounts are scaled by the split factor in force before being divided by the split-adjusted base.
- **QA plot normalization:** base-100 (first value → 100) to compare paths. Shapes are unchanged.
