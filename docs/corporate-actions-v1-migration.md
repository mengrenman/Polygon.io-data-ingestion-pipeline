# Migrating splits and dividends to `/stocks/v1`

*Written 2026-10-09. **Status: a plan, not implemented.** The pipeline still calls the v3 endpoints
and should keep calling them until the planned equity refresh is finished (§6).*

Massive's docs now head `GET /v3/reference/splits` and `GET /v3/reference/dividends` "(Deprecated)",
with no sunset date and no notice text. The successors are `GET /stocks/v1/splits` and
`GET /stocks/v1/dividends`. This document lists what changes between the two, what the new
adjustment factors mean for `legacy_scripts/factor_builder.py`, and how to diff the two sources
across the whole market before switching.

Evidence is marked:

- **(docs)**: read on Massive's docs pages on 2026-10-09:
  [v3 splits](https://massive.com/docs/rest/stocks/corporate-actions/reference-splits.md),
  [v3 dividends](https://massive.com/docs/rest/stocks/corporate-actions/reference-dividends.md),
  [v1 splits](https://massive.com/docs/rest/stocks/corporate-actions/splits.md),
  [v1 dividends](https://massive.com/docs/rest/stocks/corporate-actions/dividends.md).
- **(probed)**: five live requests on 2026-10-09 to `api.polygon.io` with the repo key, on the
  Basic account: v1 splits for AAPL (once with the new sort syntax, once with the pipeline's current
  parameters) and MULN, and v1 and v3 dividends for AAPL. Three series, 71 rows in all.
- **(local)**: measured on `refdata/_market/market_{splits,dividends}.parquet` (pulled 2026-09-15)
  or on the `day_adj` lake.
- **(estimate)**: inferred, not checked.

## Summary

- **The rows are the same so far.** For the three probed series, v1's ids, dates, ratios and amounts
  equal v3's exactly. **(probed)** v3 already returns two values that only v1 documents
  (`dividend_type = "recurring"` on 2 rows, `frequency = 365` on 32), so both endpoints are probably
  served from one store. **(local, estimate)**
- **The risk is in the parameters, not the data.** v1 ignores `order=asc`. With the pipeline's
  current parameters it returns rows **newest first** **(probed)**, which silently breaks the
  checkpoint and resume logic in `_pull_paged_table` (§2).
- **One column disappears without an error.** `dividend_type` (CD, SC, LT, ST) is gone. In its place
  is `distribution_type`, which uses a different set of categories. The pullers read fields with
  `r.get(c)`, so swapping the URL would fill `dividend_type` with nulls and raise nothing. **(docs)**
  (§3)
- **Use the new factors as a check, not an input.** `historical_adjustment_factor` (HAF) is the same
  quantity `factor_builder` computes, but anchored to today, keyed by ticker instead of holder,
  rewritten whenever a new event arrives, and rounded to 6 decimals. On AAPL, the dividend HAF
  matches the lake's total-return factor at all 53 ex-dates to within 9e-7, below its rounding
  floor. Keep computing factors from `split_from` / `split_to` and `cash_amount`, and use HAF to
  check them (§4).
- **Keep all three `adjustment_type` values in the split factor.** v3 already delivers stock
  dividends as split rows, and the lake already applies them. Filtering to forward and reverse splits
  would put a fake drop into `close_tr` on every stock-dividend date (§4.2).
- **Run the diff on Starter, right after the refresh's v3 re-pull.** The refresh already re-pulls
  splits and dividends in full on v3. A v1 pull is about 420 pages at 5,000 rows a page, which takes
  minutes without a request cap. On Basic, the same diff takes about 7.5 hours, almost all of it the
  v3 dividends pull at 1,000 rows a page. Ask before running anything market-wide on Basic (§5).

## 1. Where the pipeline calls these endpoints

| path | code | how it reaches Massive | writes |
|---|---|---|---|
| bulk (default) | `src/polygon_pullers/bulk.py`: `pull_market_splits`, `pull_market_dividends` → `_pull_paged_table` | raw HTTP through `make_fetch` + `iter_results`; `BASE = https://api.polygon.io`; `limit=1000, order=asc, sort=<date field>`; incremental with `<date field>.gte` | `refdata/_market/market_splits.parquet` and `market_dividends.parquet`, normalized by `_normalize_splits` / `_normalize_dividends` to `SPLIT_COLS` / `DIVIDEND_COLS` |
| per-ticker (`POLYGON_PER_TICKER=1`) | `src/polygon_pullers/__init__.py`: `pull_splits`, `pull_dividends` | SDK `cli.list_splits` / `cli.list_dividends`. polygon-api-client 1.16.3 hard-codes the v3 paths and has no v1 splits or dividends method | the collection's `stock_splits.parquet`, `cash_dividends.parquet` |
| CLI | `legacy_scripts/run_pullers.py`, wrapped by `scripts/pull_ref_data.sh` | calls one of the two paths above | — |

Downstream, `derive_collection_refdata` filters the market tables into each collection's files and
keeps `ticker, execution_date, split_from, split_to, ratio` for splits and
`ticker, ex_date, pay_date, cash_amount, declaration_date, record_date, frequency` for dividends. It
drops `id`, `currency` and `dividend_type`. `factor_builder.py` reads only the dates,
`ratio` (or `split_from` / `split_to`), `cash_amount` and `ticker`, and `lake_io` reads the same.
Nothing downstream reads `dividend_type`. **(local)**

## 2. Endpoint-level differences

| | v3 `/v3/reference/{splits,dividends}` | v1 `/stocks/v1/{splits,dividends}` | consequence |
|---|---|---|---|
| sort | `sort=<field>` plus `order=asc\|desc` | `sort=<field>.asc` (a comma-separated list); **no `order` parameter** | With today's parameters, v1 returned AAPL's splits 2020 → 1987. **(probed)** |
| default sort | — | splits: `execution_date` descending; dividends: **`ticker` ascending** | Always pass the sort explicitly. **(docs)** |
| maximum `limit` | 1,000 | 5,000 | About 5× fewer pages: about 413 dividend pages instead of about 2,063. **(docs, local)** |
| host | `api.polygon.io` (`bulk.BASE`) | also served on `api.polygon.io` | No host change needed. **(probed)** |
| filters dropped | splits: `reverse_split`; dividends: ranges on `record_date`, `declaration_date`, `pay_date`, `cash_amount`, and `dividend_type` | — | The pipeline uses none of them. **(docs)** |
| filters added | — | `ticker.any_of`, `adjustment_type`, `distribution_type`, ranges on `frequency` | `ticker.any_of` would let the per-ticker path batch its requests. **(docs)** |
| history on Basic | 2 years | 2 years | **Not enforced on 2026-10-09:** on Basic, v1 returned AAPL's splits back to 1987 and its dividends back to 2012. **(probed)** If the limit is enforced later, a pull would come back truncated and look complete. |
| first records | splits 1978-10-25; dividends 2000-01-15 | the same | The local v3 tables start 1978-10-25 (splits) and 2000-08-15 (dividends). **(docs, local)** |

**Why the sort order matters.** `_pull_paged_table` writes a checkpoint every 25 pages. On a rerun,
it resumes from the table's latest date minus 30 days (`_incremental_since`). That is safe only
because pages arrive oldest first. If they arrive newest first and a full pull is interrupted, the
newest rows are on disk, the rerun starts at "latest minus 30 days", and the older history is never
fetched. Nothing raises. An uninterrupted descending pull would be complete, but multi-hour pulls on
Basic are exactly where interruptions happen.

## 3. Field-by-field differences

### 3.1 Splits

`SPLIT_COLS = id, ticker, execution_date, split_from, split_to, ratio`

| v3 field | v1 field | in `SPLIT_COLS` | difference |
|---|---|---|---|
| `id` | `id` | yes | Same ids: 5 of 5 for AAPL, 9 of 9 for MULN. **(probed)** Both docs pages use `E90a77bd…` for AAPL's 2005-02-28 split. Ids start with `E` (28,092 rows) or `P` (275 rows) in the local table, and v1 returns both prefixes. |
| `ticker` | `ticker` | yes | Same. |
| `execution_date` | `execution_date` | yes | Same values. **(probed)** v1 now documents the session boundary: the prior day's post-market is the last pre-split session, and the pre-market on the execution date is already adjusted. That matches `factor_builder`, which aligns each split to the first ET trading day on or after the date. |
| `split_from` | `split_from` | yes | Same values. **(probed)** |
| `split_to` | `split_to` | yes | Same values. **(probed)** |
| — | (`ratio`, derived) | yes | Unchanged: `split_to / split_from`. |
| — | `adjustment_type` | no | **New:** `forward_split`, `reverse_split` or `stock_dividend`. Today's column filter would drop it silently. |
| — | `historical_adjustment_factor` | no | **New:** a cumulative split factor, §4.1. |

### 3.2 Dividends

`DIVIDEND_COLS = id, ticker, ex_dividend_date, pay_date, record_date, declaration_date, cash_amount, currency, frequency, dividend_type`

| v3 field | v1 field | in `DIVIDEND_COLS` | difference |
|---|---|---|---|
| `id` | `id` | yes | Same ids, 57 of 57 for AAPL. **(probed)** |
| `ticker` | `ticker` | yes | Same. |
| `ex_dividend_date`, `pay_date`, `record_date`, `declaration_date` | same names | yes | No differences on AAPL's 57 rows. **(probed)** |
| `cash_amount` | `cash_amount` | yes | No differences. **(probed)** Still the amount per share as declared, in the dividend's currency, and not split-adjusted. |
| `currency` | `currency` | yes | Same. The local table has 47 currencies, and 126,028 of its rows are not in USD. **(local)** |
| `frequency` | `frequency` | yes | Same name and meaning. v1 also documents 3, 104 and 365, in addition to v3's 0, 1, 2, 4, 12, 24 and 52. The local v3 table already has 32 rows with 365 (all SATA, 2026). **(docs, local)** |
| `dividend_type` (CD, SC, LT, ST) | — | **yes** | **Removed.** Pulling v1 through today's code would leave this column all null and raise no error. |
| — | `distribution_type` | no | **New**, with different categories: `recurring`, `special`, `supplemental`, `irregular`, `unknown`. It is not a renamed `dividend_type`: AAPL's three 2012–13 rows (frequency 0) are `unknown`. **(probed)** The local v3 table already has 2 rows whose `dividend_type` is `recurring` (BBUC 2026-03-23, ELME 2026-01-08). **(local)** |
| — | `split_adjusted_cash_amount` | no | **New:** `cash_amount` scaled by the splits after the ex-date, on today's share basis, rounded to 6 decimals. AAPL 2012-08-09: 2.65 → 0.094643, which is 2.65 / 28. **(probed)** |
| — | `historical_adjustment_factor` | no | **New:** a cumulative dividend factor, §4.3. |

`dividend_type` counts in the local v3 table: CD 2,052,819; SC 9,965; `recurring` 2; no LT or ST.
**(local)**

## 4. What the new fields mean for `factor_builder.py`

### 4.1 The split `historical_adjustment_factor`

The docs say: to adjust a price on date D, find the first split whose `execution_date` is after D
and multiply the unadjusted price by that split's HAF. The probes give the formula. For a ticker's
splits k = 1…n in date order:

    HAF_k = 1 / (ratio_k × ratio_(k+1) × … × ratio_n)

AAPL's 1987-06-16 split has HAF 0.004464, which is 1/224 = 1/(2·2·2·7·4). The last split always has
HAF × ratio = 1. MULN's nine reverse splits give 1.35e14 for its 2016 split. **(probed)**

This is the quantity `factor_builder` calls `split_price_factor`, which is `F_t / F_last`, where F
is the running product of ratios aligned to trading days. Four differences keep HAF from being a
drop-in replacement:

1. **Anchor.** HAF is on today's share basis. `factor_builder` anchors each holder to its last day
   in the lake, and the lake ends 2025-08-13. For a name that split after its last lake day, every
   HAF differs from the lake's factor by that later ratio. Only the ratio of consecutive HAFs is
   comparable.
2. **Key.** HAF is computed per ticker. `factor_builder` computes per holder (`holder_id`), and per
   trading segment for recycled symbols, so one company's splits never reach the bars of the
   previous company that used the symbol. Whether v1 chains a recycled symbol's events across
   companies is unknown. Probably it does, since the rows carry only a ticker. **(estimate)** Either
   way, a per-ticker product is the wrong unit for this pipeline.
3. **Mutability.** HAF_k includes every later split, so each new split for a ticker changes the HAF
   of all its earlier rows. The incremental refresh re-fetches only the last 30 days and merges by
   id, so a stored HAF column would go stale on every older row. The docs' sample row for AAPL's
   2025-08-11 dividend shows HAF 0.997899, and the live value on 2026-10-09 is 0.995190, which fits
   that behavior. **(docs, probed)**
4. **Precision.** HAF has 6 decimal places. AAPL's 1987 factor is off by 6e-5 in relative terms, and
   the ratio implied by two consecutive HAFs is off by up to 1.1e-4. A factor near 1e-4 keeps only
   two or three significant digits. **(probed)**

**Recommendation:** keep computing factors from `split_from` / `split_to`, as now, and don't store
HAF in the market table. In the diff (§5), use it for an identity check: `HAF_k × ratio_k =
HAF_(k+1)`, and `HAF × ratio = 1` on the last row, within rounding. A row that fails it means the
vendor's own factor and ratio disagree.

### 4.2 `adjustment_type`

v3 already returns stock dividends as split rows. In the local table, 5,954 forward rows have a
non-integer ratio, for example ACEIY 1.048, CMPCY 1.003 and PREKF 1.00369. **(local)**
`factor_builder` applies them in `split_price_factor`, as CRSP does: the price factor absorbs every
kind of share-count change. Because the dividends endpoint carries only cash, `close_tr` stays
continuous across a stock dividend.

- **Keep all three types in the split factor.** Filtering to `forward_split` and `reverse_split`
  would remove stock dividends from `close_split`, and `close_tr` would then show an unadjusted
  drop of a few percent on every stock-dividend date.
- **Store the field** by adding it to `SPLIT_COLS`. It costs nothing and lets the summary CSV and
  the diff report split events by kind.
- **Check it against the ratio in the diff:** `reverse_split` should mean `split_from > split_to`,
  and `forward_split` or `stock_dividend` should mean `split_to > split_from`. Also crosstab
  `stock_dividend` against non-integer ratios.
- **Same-day events.** The local table has 185 (ticker, date) pairs, and `factor_builder`
  multiplies ratios that fall on the same day. In 179 pairs the two ratios differ (two stock
  dividends on one day, as at AFOVF). In 6 pairs the ratios are identical, which suggests duplicates
  that are applied twice. **(local)** The diff should check how v1 returns and classifies these
  pairs.
- `--detect-split-gaps` only guesses ratios of 2, 3, 4, 5, 10 and 20, so it already ignores moves
  the size of a stock dividend. Nothing to change there.

### 4.3 The dividend `historical_adjustment_factor`

The docs say: to adjust a price on date D, find the first dividend whose ex-date is after D and
multiply the price by that dividend's HAF. That is a backward total-return factor:

    HAF_k = g_k × g_(k+1) × … × g_n,   g_j = 1 − D_j / (close on the day before ex_j)

`factor_builder` computes the same retained fraction, `g = (prior_base − amount × spf) / prior_base`,
and its `tr_price_factor` is `G_last / G_t`. To test this, I compared each per-event
`g_k = HAF_k / HAF_(k+1)` from v1 with the jump in `tr_price_factor` in
`day_adj/spx_ndx_combined_adjusted/AAPL` at each of AAPL's 53 ex-dates from 2012-08-09 to 2025-08-11.
The median relative difference is 3.6e-7 and the largest is 9.0e-7, below the 6-decimal rounding
floor of about 1.2e-6. The cumulative products are 0.840562 and 0.840563. **(probed, local)** So for
this one ticker, the vendor uses the same multiplicative convention, the prior day's close as the
base, and cash on the same share basis.

What this means:

- **The dividend HAF covers dividends only.** The full adjustment is split HAF × dividend HAF, which
  is how the lake builds `close_tr = close × split_price_factor × tr_price_factor`.
- **The four caveats in §4.1 apply here too**: anchor, per-ticker key, rewriting on each new
  dividend, and 6 decimals. Rounding hurts more here. On AAPL, the dividend yield implied by HAF
  (`1 − g`) is off by up to 5e-4 in relative terms. For a dividend that yields 1e-5, the error would
  be several percent.
- **Base close.** The vendor presumably uses the official daily close. **(estimate)** The minute
  lake's base is the previous day's last minute bar (`last_close` from the day-edge scan), which can
  be an after-hours print. Compare against the **day** lake, as in the AAPL check.
- **Currency** is the open question that matters most (§7). `cash_amount` is in the dividend's
  currency, and the collection files drop the `currency` column before `factor_builder` sees them.
  2,703 non-USD rows on 160 tickers, mostly CAD, reach `refdata/all/cash_dividends.parquet`, and 13
  rows on 5 tickers reach `spx_ndx_combined`. **(local)** This problem predates the migration. If
  the vendor converts currency inside its factor, the HAF comparison will show it on exactly these
  tickers, which makes the comparison a useful detector.

### 4.4 `split_adjusted_cash_amount` and `distribution_type`

- **`split_adjusted_cash_amount`** is `cash_amount × spf` on today's share basis, rounded to 6
  decimals. Don't use it in place of `factor_builder`'s own scaling, because it has the anchor and
  precision problems above. Use the ratio `split_adjusted_cash_amount / cash_amount` as a check
  against the split HAF of the first split after the ex-date. That check also shows whether stock
  dividends count as splits there.
- **`distribution_type`.** `factor_builder` does not filter on dividend type today: CD and SC rows
  both go into the total-return factor. That is correct, because a special dividend is still cash
  paid to the holder. Keep it that way, and store `distribution_type` for reporting only.

## 5. Diff procedure

The goal is to show, before any switch, that a v1-sourced table gives the same adjusted prices as
the v3 table, and to explain every row that differs.

**When and where.** The equity refresh in
[asset-class-expansion.md](asset-class-expansion.md) (§ Joint purchase recommendation, item 8)
already includes a full re-pull of splits and dividends on v3 after Stocks Starter is bought, and
Starter has no request cap. That re-pull is the v3 side of the diff. The only extra cost is the v1
pull, which takes minutes on Starter. If the diff has to happen on Basic instead, the owner must
approve it first.

**Request cost** at 12.5 s per request (`POLYGON_MIN_INTERVAL_SEC=12.5`), using row counts from
2026-09-15:

| pull | rows | rows per page | pages | time on Basic |
|---|---|---|---|---|
| v3 splits | 28,367 | 1,000 | ~29 | ~6 min |
| v3 dividends | 2,062,786 | 1,000 | ~2,063 | ~7.2 h |
| v1 splits | (same) | 5,000 | ~6 | ~1 min |
| v1 dividends | (same) | 5,000 | ~413 | ~86 min |

### Step 1: snapshot

Copy `refdata/_market/market_{splits,dividends,tickers}.parquet` to
`refdata/_market/_pre_v1_<date>/`. Nothing in the diff writes to `_market/`.

### Step 2: pull both sides on the same day, into separate directories

- **v3:** the refresh's own full re-pull. Outside the refresh, the equivalent is
  `pull_market_refdata(<refdata>/_diff_<date>/v3, fetch, full=True, tables=("splits", "dividends"))`.
  Copy the snapshot's `market_tickers.parquet` into both `v3/` and `v1/` first, so both sides
  derive against the same tickers table.
- **v1:** a throwaway script, not pipeline code. It reuses `bulk.make_fetch` (pacing, retries, the
  key sent in a header) and `bulk.iter_results` with `path="/stocks/v1/splits"` and
  `params={"sort": "execution_date.asc", "limit": 5000}` (for dividends,
  `sort=ex_dividend_date.asc`). It writes **every** field of every row to
  `<refdata>/_diff_<date>/v1/`, checkpoints as it goes, and asserts that dates never decrease within
  or across pages, so a wrong sort fails immediately instead of corrupting a resume.

Record the start and end time of each pull. If the vendor edits rows while the pulls run, those rows
show up as value differences, and Step 4 separates them from real differences between endpoints.

### Step 3: compare rows

Match on `id` first. For ids found on only one side, fall back in order to (ticker, date, ratio or
amount), then (ticker, date), then (ticker, date ± 3 trading days). That way "id changed", "date
moved" and "event missing" are counted separately.

**Splits**

1. Row counts: in total, per year and per ticker; ids on both sides, on v3 only and on v1 only.
2. For matched ids: `ticker`, `execution_date`, `split_from` and `split_to` must be exactly equal.
3. `adjustment_type` against the ratio (§4.2), and how v1 handles the 185 same-day pairs.
4. The HAF identity per ticker (§4.1), within a tolerance of 0.5e-6 × (1 + ratio).

**Dividends**

1. Row counts: in total, per year, per currency and per frequency; ids on both sides, on v3 only and
   on v1 only; a crosstab of `dividend_type` × `distribution_type`.
2. For matched ids: `ticker`, all four dates, `cash_amount` (exactly), `currency` and `frequency`.
3. Duplicates. The local v3 table has 54,796 (ticker, ex-date) pairs that repeat, and 686 of them
   also repeat the amount. For example, AAON 2012-11-29 appears twice at 0.12, once with frequency 2
   and once with 0. **(local)** Count the same on both sides. If v1 removes duplicates, the
   total-return factor changes for those names.
4. `split_adjusted_cash_amount / cash_amount` against the v1 split HAF (§4.4).

### Step 4: compare factors (the deciding test)

Row differences matter only if they change adjusted prices.

1. Derive the `all` collection's files from each side:
   `run_pullers.py --bulk --tables "" --market-dir <refdata>/_diff_<date>/{v3,v1} --outdir <refdata>/_diff_<date>/{v3,v1}/all`
   with the same `--tickers` file used for `refdata/all`. About 9 minutes each, with no requests,
   though `POLYGON_API_KEY` must still be set. The v1 tables first go through the same
   `_normalize_*` code, with columns mapped.
2. Build the **day** adjusted lake from `day/all` twice, with
   `build_adjusted_lake.sh -t day -c all -r <refdata>/_diff_<date>/{v3,v1}/all -o <lake>/day_adj/_diff_{v3,v1}`.
   Each build takes minutes and writes beside the production lake, never over it.
3. For every (holder id, day), compare `split_price_factor` and `tr_price_factor`. List every holder
   where either |Δ log factor| exceeds 1e-9, with the first day the two diverge and the rows that
   cause it. Identical tables give identical factors to floating-point precision, so any holder
   above 1e-9 traces back to a row difference.
4. Separately, for each event, compare the lake's `g` with the `g` implied by the vendor's HAF
   (§4.3). Group misses by cause: non-USD currency, duplicate rows, missing prior close,
   recycled-symbol segment, or unexplained.

Recompute every count in the report from the raw pulled parquet, not from intermediate frames. If a
holder's cumulative factor moves by more than 1%, re-fetch that ticker from both endpoints (two
requests) before blaming the endpoint rather than a vendor edit made during the pull.

### Step 5: report and acceptance

Write the results to `docs/corporate-actions-v1-diff-<date>.md`: the count tables, mismatch counts
by class, up to 50 examples per class, and the factor-level list. Switch only when all of these
hold:

- Every v3-only split id is explained. An unexplained one is a split that would vanish and leave a
  series unadjusted across it.
- `split_from` and `split_to` agree on every matched id, or each difference is explained.
- Every holder above the Step 4 threshold is explained, and none as "unknown".
- The v1 tables reach as far back as the v3 tables. This catches the Basic 2-year limit if it
  becomes enforced.

## 6. Order of work

1. **Now:** nothing changes. v3 still answers. **(probed 2026-10-09)**
2. **Equity refresh, on v3.** It stays on v3 on purpose, so the panel does not change source partway
   through. It is blocked until Stocks Starter is bought, which also brings flat-file access.
3. **Diff** (§5), on the day of the refresh's v3 re-pull or soon after.
4. **Switch PR**, only after the diff passes:
   - `bulk.py`: use the v1 paths; `sort=<field>.asc`; drop `order`; `limit=5000`. Add
     `adjustment_type` to `SPLIT_COLS`, and add `distribution_type` and `split_adjusted_cash_amount`
     to `DIVIDEND_COLS`. New pulls no longer fill `dividend_type`.
   - Add a check on the first page that raises if an expected field is missing, because `r.get(c)`
     turns a missing field into a silent null. Add the date-order assertion from Step 2. Write a
     sidecar manifest recording the source (`v3` or `v1`) next to each table.
   - Per-ticker path: polygon-api-client 1.16.3 has no v1 splits or dividends method, so route
     `pull_splits` and `pull_dividends` through `make_fetch` with `ticker=` (or with `ticker.any_of=`
     in batches) instead of the SDK.
   - Make the first v1 pull a **full refetch into a new file**, then swap. An incremental merge by id
     into the v3 table would leave a table that is half `dividend_type` and half
     `distribution_type`.
   - Tests: `tests/test_bulk_pullers.py` fakes the `/v3/reference/*` paths and v3 rows, and
     `tests/test_pullers.py` fakes `list_splits` / `list_dividends`. Port both, and add tests for
     the sort parameter, the order assertion and the schema check.
   - The `bulk.py` docstring calls the dividends pull "a few hundred pages", and the README,
     `run_pullers.py --help` and `pull_ref_data.sh` say the whole bulk pull takes "a few hundred
     requests". The dividends pull alone is about 2,063 pages at 1,000 rows a page, so those claims
     become true only at 5,000 rows a page (about 413). Update the wording in the switch PR.
5. **Adjusted lakes:** if Step 4 found no factor differences, nothing needs rebuilding. Otherwise
   rebuild with build-then-swap.

**If v3 is switched off first.** The probed rows are identical, so the refresh can continue on v1
with the switch-PR changes. The diff then runs against the snapshot from Step 1 instead of a fresh
v3 pull. A 404 from v3 raises today, because `make_fetch` retries only 429 and 5xx responses, so a
shutdown would be noticed. An empty 200 would not: an incremental refresh that returns no rows for a
30-day window counts as success, even though the market always has splits and dividends in any
30-day window. A guard against that belongs in the switch PR, or before it.

## 7. Open questions the probe did not settle

1. **Basic's history limit.** Both docs pages say 2 years on Basic, and neither endpoint enforced it
   on 2026-10-09. If it is enforced later, a full pull on Basic would return a table that starts in
   2024 and looks complete. The last acceptance check in Step 5 catches this.
2. **Recycled symbols.** Does v1 attach the previous company's events to a recycled symbol, as v3
   does? That affects only HAF, which this plan does not use, unless the event rows themselves
   differ.
3. **Same-day pairs and duplicate dividends.** Does v1 return both rows of the 185 same-day split
   pairs and of the 686 same-amount dividend pairs?
4. **Currency inside the dividend HAF.** Does the vendor convert a CAD dividend before dividing by a
   USD close? See §4.3.
5. **Sunset date.** None has been announced. Re-read the two v3 pages when the equity refresh is
   scheduled.
