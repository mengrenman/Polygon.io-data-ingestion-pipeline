# Symbols reused after the lake ends: wrong holder ids and misfiled corporate actions

Status, 2026-10-09: **measured; fix implemented behind an opt-in flag and tested on fixtures and scratch
outputs; not run on the real lake, and blocked from it.** No dataset under `/Users/mengren/local/parquet_lake`
or the flat files may be built, re-derived, overwritten or copied until the owner decides how to proceed under
Massive's license terms. Section 8 of the Market Data Terms requires deleting Market Data if the account is
"terminated, restricted, or suspended"; see `docs/asset-class-expansion.md` §1 in
[#26](https://github.com/mengrenman/Polygon.io-data-ingestion-pipeline/pull/26). After that decision the full run
also needs a point-in-time snapshot pull of about 250 requests (about an hour at the Basic plan's 5 requests per
minute), which waits for the owner's go-ahead. Nothing in the lakes or the refdata has been changed.

Every number below comes from the real lake (`day/all`, `day_adj/all_adjusted`, `refdata/_market`, the
`top1000_cs_monthly` universe) and is reproduced by `scripts/audit_symbol_holders.py` in about 40 seconds.
Per-segment detail for strategy-lab is in [`symbol-reuse-audit-2026-10-09/`](symbol-reuse-audit-2026-10-09/).

## 1. What is wrong

`refdata/all/security_master.parquet` has one row per ticker, built from `market_tickers.parquet`, which Massive
returns with **today's** company for each symbol. When a symbol stopped trading inside the lake (which ends
2025-08-13) and was given to a new company afterwards, the lake has one trading segment for it, the recycled-symbol
cut (a 60-day gap) never fires, and every bar is credited to the new company. `ABX` printed Barrick Gold's bars
from 2003-09-10 to 2018-12-31 and carries `BBG00VY1KB95`, Abacus Global Management.

The README's *Recycled symbols — fixed* entry covers reuse inside the lake after a gap of 60 days or more. It does
not cover a symbol reused after the lake ends, a symbol handed over with a shorter gap, or the corporate actions,
which share one root cause:

**Massive files a company's dividends and splits under a symbol the company held at some point: the symbol at the
event date or a later one, never an earlier one.** Barrick's 2003–2018 dividends, paid as `ABX`, are filed
under `GOLD`, its symbol from 2019 to 2025. BB&T's 2011–2019 dividends are under `TFC`. Northeast Utilities'
are under `ES`, Laclede's under `SR`, AmerisourceBergen's under `COR`. A company that was acquired keeps its
history under its last symbol (Schering-Plough under `SGP`, Anadarko under `APC`, Barnes Group under `B`).
`factor_builder` keys every action by (symbol, date) through the security master, so a misfiled action lands on
whoever held the symbol on that date.

The defect shows up in three places:

| Class | What happens | Example |
|---|---|---|
| **A** reused after the lake ends | the whole last segment gets today's company's id; its dividends filed under a later symbol are missing; today's company's own history (filed under *its* earlier symbol, same holder id) is applied to the old bars | `ABX` = Barrick, id is Abacus; `BBT` = BB&T, receives Berkshire Hills' dividends |
| **B** handed over inside a segment (gap under 60 days) | two companies under one id; the price jump at the handover is a return | `GOLD`: Randgold $82.89 → Barrick $13.10 on 2019-01-02, `close_tr` −79.5% |
| **C** earlier holder absorbs today's history | a `#SEG` segment receives the dividends today's holder filed under the symbol before it took it | `TFC#SEG0` (Taiwan Greater China Fund, ~$6) receives BB&T's $0.15–$0.47 a quarter |

### Corrections to the starting premises

- **Barrick's pre-2019 dividends are not under `B`.** `B`'s 60 pre-2019 records, and 83 up to 2025-01, are
  Barnes Group's. Barnes held `B` until 2025-01-24: its 2:1 split is on 2006-06-12 and it paid $0.16 a quarter
  from 2018 to 2024. The current build gives them to `B#SEG0` (Barnes), which is correct: 83 applied, 83 its own.
  Barrick's 49 pre-2019 dividends are under `GOLD`, mixed with Randgold's 13 annual ones. Massive's `frequency`
  field separates them: Randgold's are annual (`1`), Barrick's semiannual or quarterly.
- **`GOLD` from 2003 to 2025-05-08 is two companies, not one**: Randgold (CIK 1176338, no FIGI) until 2018-12-28,
  Barrick from 2019-01-02. The universe treats it as one segment for 206 member-months.
- **The events endpoint does not give a usable symbol history.** `/vX/reference/tickers/BBG000BB07P9/events`
  returns one event, the move to `B` on 2025-05-09. Nothing for `ABX` or `GOLD`. `TFC`'s events show the change
  to `TFC` on 2019-12-09 without naming `BBT`.
- **"Re-key from the company's current ticker by holder id" is not enough, and by CIK it is wrong.** Barrick's
  current ticker `B` holds none of its history (it is under `GOLD`). And CIK is the legal issuer, not the security:
  Schering-Plough's CIK 310158 became the new Merck's in the 2009 reverse merger, while `MRK`'s price series is
  old Merck's. Re-keying `MRK`'s 2003–2009 dividends to the CIK would hand old Merck's dividends to
  Schering-Plough. CIK also spans share lines (`APC` shares Anadarko's with its debentures `AEUA`; `BBT` with
  BB&T's preferreds). FIGIs change too: aTyr is `BBG00JVC1TH4` in 2015 and `BBG001J2P692` in 2024.
- `FISV`, `HOS` and `SVA`, the suspected relistings: `FISV` and `SVA` are the same company (Fiserv returned to
  `FISV` from `FI` in November 2025; Sinovac), so their ids are right. `HOS` is not: the 2004–2019 issuer is
  CIK 354359, today's `HOS` is the post-bankruptcy issuer, CIK 866829, whose equity replaced the cancelled one.

## 2. Size

| | count | share |
|---|---:|---:|
| tickers in `day/all` | 33,833 | |
| ticker-days in `day/all` | 46,490,567 | |
| **A:** last segments ending before 2025-06-30 while the master marks the ticker active | **416** | |
| rows in those segments, `day/all` | 490,110 | 1.05% |
| rows in those segments, `day_adj/all_adjusted`, all carrying today's holder id | 490,110 | 1.05% |
| universe member segments among them | 30 | |
| universe member-months | 1,770 of 262,000 | 0.68% |
| member segments held by another company (point-in-time lookup) | 28 of 30 | |
| member segments held by the same company (`FISV`, `SVA`) | 2 of 30 | |
| segments with dividends arriving through the wrong holder id | 13 of 416 | |
| segments with **splits** arriving through the wrong holder id | 4 of 416 | |
| flagged segments with a rename successor by price continuity alone (not verified) | 120 of 416 | |
| **B:** handovers inside a segment confirmed by point-in-time lookup | 5 | |
| raw candidates (≥3 missed sessions and a >15% break, or a >2× break, no split near) | 4,125 | mostly data holes |
| **C:** `#SEG` segments | 4,421 | 3,050,163 rows |
| `#SEG` segments with implausible dividends (>15%/yr or one ex-date >20%) | 39 | |
| of those, confirmed as today's holder's re-filed history | 10 | |

The 416 segments end throughout the history, not only near the lake's end: 1 in 2003, between 7 and 30 a year from
2004 to 2022, then 52 in 2023, 35 in 2024 and 23 in 2025.

The 30 member segments were classified with 60 point-in-time lookups, one near each end of each segment:
`GET /v3/reference/tickers?ticker=X&date=D`. Another 22 identified the rename successors, the class-B handovers
and the previous holders of `B` and `TFC`, and fed the real-data check in 4.4. 6 went on probing the endpoints.
That is 88 requests in all. Class C was confirmed from the dividend records, not the API.

**Class B, confirmed:**

| symbol | handover | first company → second | `close_tr` that day | member-months before |
|---|---|---|---:|---:|
| `GOLD` | 2019-01-02 | Randgold → Barrick | −79.5% | 130 |
| `T` | 2005-12-01 | AT&T Corp → AT&T Inc (formerly SBC) | +24.3% | 24 |
| `MGM` | 2005-05-02 | Metro-Goldwyn-Mayer → MGM Mirage | +483% | 5 |
| `MS` | 2006-01-17 | Milestone Scientific → Morgan Stanley | +4,605% | 0 |
| `AI` | 2020-12-09 | Arlington Asset → C3.ai | +3,288% | 0 |

`T`'s 2003–2005 bars also receive SBC's dividends, which Massive files under both `SBC` and `T`. That is −16.1
log % of foreign total return in 2.2 years, on top of AT&T Corp's own.

**Class C, confirmed:** `TFC#SEG0` (BB&T's), `ES#SEG0` (Northeast Utilities'), `SR#SEG0` (Laclede's),
`LSI#SEG0` (Sovran/Life Storage's), `GHC#SEG0` (Washington Post's), `FHI#SEG0` (Federated's), `COR#SEG0`
(AmerisourceBergen's), `TTE#SEG0` (Total's), `RIO#SEG0` (Rio Tinto's) and `KT#SEG0` (KT Corp's). In each, the
holder's quarterly or semiannual stream runs unbroken through the old company's window, on a stock priced far
lower. Two are universe members: `LSI#SEG0` for 126 months (−35.5 log % a year) and `RIO#SEG0` for 66 (−15.6).
Others among the 39 look like genuine specials (`AMTD`'s $6 in 2006, `BBI`, `RGC`, `ALD`). The list is in
[`seg_dividends_implausible.csv`](symbol-reuse-audit-2026-10-09/seg_dividends_implausible.csv).

## 3. Total-return error

There are four routes. All are measured on what `day_adj/all_adjusted` actually applies, read off
`tr_price_factor` and `split_price_factor`, against the true company's records.

1. **Missing:** the company's dividends are filed under its later symbol. `ABX` has none applied; Barrick's 49
   are under `GOLD`.
2. **Foreign, through the holder id:** today's company renamed into the symbol, and its own history, filed under
   its earlier symbol, carries the same id. `BBT` receives Berkshire Hills' (`BHLB`); `GOLD` receives A-Mark's
   (`AMRK`); `ACH` receives Owens & Minor's. This includes **splits**:

   | symbol | date | foreign split | `close` | `close_tr` | member that month |
   |---|---|---|---:|---:|---|
   | `GOLD` | 2022-06-07 | A-Mark 2:1 | +1.1% | **+102.2%** | yes |
   | `ACH` | 2010-04-01 | Owens & Minor 3:2 | +1.9% | **+52.9%** | yes |
   | `VIP` | 2013-06-19 | 1:8 | −1.0% | +692.3% | no |
   | `CIRC` | 2023-08-31 | 1:40 | −0.7% | −97.5% | no |

3. **Foreign, through the symbol:** an earlier holder's segment receives the later holder's history (class C), or
   both companies' records apply under one id (class B).
4. **Handover jumps** (class B, the table above).

**The 30 member segments.** TR error is current minus true, in log % over the segment; positive means
`close_tr` is too low.

| segment | true company | member-months | TR error | route |
|---|---|---:|---:|---|
| `ABX` 2003-09-10..2018-12-31 | Barrick | 182 | **+17.7** (1.2/yr) | 49 missing (under `GOLD`) |
| `ACH` 2003-09-10..2022-09-01 | Aluminum Corp of China | 35 | **−84.7** (−4.5/yr) | 72 foreign dividends via id; foreign 3:2 split |
| `BBT` 2003-09-10..2019-12-06 | BB&T | 193 | **−12.1** (−0.7/yr) | 36 missing (under `TFC`), 56 foreign (Berkshire Hills) |
| `GOLD` 2003-09-10..2018-12-28 | Randgold | 206 (whole segment) | **−38.4** (−2.5/yr) | 62 foreign: Barrick's and A-Mark's |
| `GOLD` 2019-01-02..2025-05-08 | Barrick | (same) | **−44.2** (−7.0/yr) | 16 foreign: A-Mark's; plus the 2:1 split and the handover jump |
| `MICC` 2003-09-10..2011-05-27 | Millicom | 62 | **+11.3** (1.5/yr) | missing (filed under `TIGO`) |
| `VIP` 2003-09-10..2017-03-30 | VimpelCom | 103 | **+10.9** (0.8/yr) | 5 missing (under `VEON`) |
| 24 others (`APC`, `BID`, `SGP`, `NHP`, `P`, …) | the old company | 989 | 0.0 | wrong id and name only: their own dividends sit under their own symbol |

So **781 of the 1,770 affected member-months carry a total-return error**, 989 only a wrong id and name, plus
`T`'s 24 months as AT&T Corp (class B) and `LSI` and `RIO`'s 192 (class C). The detail is in
[`member_segments_tr_error.csv`](symbol-reuse-audit-2026-10-09/member_segments_tr_error.csv); every flagged
segment, with the id the lake gives it, is in [`flagged_segments.csv`](symbol-reuse-audit-2026-10-09/flagged_segments.csv).

## 4. The fix

### 4.1 Point-in-time holders

`GET /v3/reference/tickers?market=stocks&date=D` returns every symbol with the company that held it on `D`: a
name always, a CIK on 91.5% of historical rows, a FIGI on 38.6% (measured on the first page for 2010-06-30).
It works on the current Basic plan: all 82 per-ticker lookups made for this audit returned a holder, from
2003-10-08 to 2025-07-01, covering every one of the 30 member segments. One
snapshot a year names the holder of nearly every trading segment. The switch day comes from the lake. New:
`polygon_pullers.asof.pull_tickers_asof`, run as `run_pullers.py --bulk --asof-dates annual`. That pulls every
June 30 from 2004 plus 2003-09-30, about 11 pages a date, into `refdata/_market/market_tickers_asof.parquet`. It
is resumable by date.

### 4.2 Lines: one security across its symbols (`holder_lines`)

1. **Observe.** Each snapshot row is matched to the lake segment of its symbol that contains it.
2. **Split inside a segment.** If two snapshots in one segment name different companies (no shared CIK or
   FIGI), the switch is the session between them with the most missed sessions, then the largest overnight break.
   The segment is cut there only if the price breaks (≥10%, and either a missed session or a ≥1.5× move).
   Otherwise it is one security re-papered and stays one piece: old Merck to new Merck, a SPAC merging into
   Clarivate.
3. **Link across symbols** when the price carries on within 5 sessions and the two pieces share a CIK or FIGI
   (`ABX`→`GOLD`→`B`, `BBT`→`TFC`, `Q`→`IQV`, `CCC`→`CLVT`). Also when a CIK returns on a fresh symbol after 60
   days or more (Millicom: `MICC` until 2011, `TIGO` from 2019). Also, on price alone (within 2%), when one side
   carries no id and the successor took over an existing symbol at a handover: Polygon gives `SBC` no CIK, and
   SBC became `T`. **A CIK alone never links**, which is what keeps Schering-Plough off old Merck's line.
4. **Ids.** A line keeps today's holder id if it runs to the lake's end and today's record for its last symbol is
   the same company. Otherwise it takes a record carrying one of its FIGIs, or a record with its CIK *on one of
   its own symbols*. Failing those, it gets a fresh id from its latest FIGI or CIK (Randgold:
   `CIK__0001176338`). Two securities never share an id.
5. **Unobserved segments** of an observed symbol get `NOFIGI__<T>#SEG<n>`, exactly as `factor_builder` names them,
   so they cannot fall to today's open-ended holder. An unobserved *last* segment stays with today's holder, as
   before.

`lines_to_security_master` turns pieces into security-master rows with **confirmed** windows. `factor_builder`
already assigns bars by window for multi-holder symbols, so it needs no change for the bars.

### 4.3 Re-filing corporate actions (`refile_actions`)

For a record under symbol Y on date d, there are three kinds of candidate:

- **own:** the line on Y at d.
- **prior:** the line whose last session on Y was just before d. Randgold's final $2.69 went ex on 2019-01-02,
  Barrick's first day on `GOLD`.
- **later:** a line that takes Y later and traded under another symbol X at d. Barrick in 2010 traded on `ABX`.

With one candidate, the record goes to it. With two:

1. **Copy.** A record with the same date and value under X is the company's own copy, so this one is dropped
   (`BBT`/`TFC` 2004–2011, `SBC`/`T`).
2. **Price test.** Each line's ex-date overnight moves are compared with the payout (or split ratio), as a
   log-likelihood ratio summed over the stream (filed symbol, the two lines, dividend frequency). A single record
   overrules a decisive stream only on overwhelming evidence: Randgold's $2.69 would be a 20% drop on Barrick's
   line.
3. **Frequency mismatch.** A dividend whose frequency differs from every stream the price test gave the holder of
   Y goes to the later line. Under `GOLD`, Randgold's annual dividends are decided by price, so the semiannual and
   quarterly ones are Barrick's, even though $0.05 on a $15 stock is too small to test.
4. Anything else stays as filed and is reported `unresolved`, for a reviewed override in
   [`data/refdata_overrides/action_holders.csv`](../data/refdata_overrides/action_holders.csv). The six there now:
   Barrick's 2003–2005 $0.11 semiannual records, which Massive files with `frequency` 0, and a BB&T $0.01 under
   `TFC`.

Two weaker fallbacks were tried and rejected, because they give Barnes Group's quarterly dividends to Barrick
whenever the price test is unsure: "the later company keeps paying this stream under Y", and "the later company
filed nothing under its own symbol at d". Barrick pays quarterly under `B` too, and filed nothing under `ABX`.

The re-filed record carries `ticker` (the symbol on the event date), `filed_ticker`, `refile_rule` and
`refile_holder_id`. `factor_builder._assign_event_ids` honors `refile_holder_id` where present, because
(symbol, date) cannot express "Randgold's dividend, dated Barrick's first day on `GOLD`". That is the only change
to the builder, and it does nothing unless the column is there.

### 4.4 Verified

- `tests/test_asof_holders.py`, 21 tests. The fixtures are scaled-down copies of the real cases with the real ids:
  Barrick/Randgold/Barnes on `ABX`/`GOLD`/`B`, BB&T/the fund/Truist/Berkshire Hills on `BBT`/`TFC`/`BHLB`,
  Schering-Plough/Merck, and AT&T Corp/SBC. They run through the derive and `factor_builder`'s batch path end to
  end. Disabling cross-symbol links fails 12 of them, the in-segment split 9, the frequency rule 3, the copy rule
  1, and `refile_holder_id` 1. The other 167 tests pass unchanged.
- **Real data, 24 symbols**, with the 82 point-in-time lookups standing in for snapshots, derived and adjusted
  in scratch space:

  | segment | id before → after | dividends applied before → after (log % of TR) |
  |---|---|---|
  | `ABX` 2003–2018 | Abacus → Barrick `BBG000BB07P9` | 0 → 49 (0 → −17.7) |
  | `GOLD` 2003–2018 | Gold.com → Randgold `CIK__0001176338` | 61 → 12 (−19.0 → −8.4) |
  | `BBT` 2003–2019 | Beacon → Truist `BBG000BYYLS8` (one line with `TFC`) | 86 → 66 (−65.4 → −56.0) |
  | `TFC` 2004–2011 | `NOFIGI__TFC#SEG0` → the fund `CIK__0000836267` | 31 → 1 (−198.2 → −0.1) |
  | `T` 2003–2005 | AT&T Inc → AT&T Corp `CIK__0000005907` | 15 → 8 (−26.7 → −10.6) |
  | `B` 2003–2025-01 | `NOFIGI__B#SEG0` → Barnes `BBG000BCSCB1` | 83 → 83 (unchanged) |
  | `SGP`, `MRK`, `APC` | ids named; `SGP` stays apart from `MRK` | unchanged |

  "Before" here is the current code rerun on the same 24-symbol tables, for a like-for-like comparison. It lacks
  A-Mark's records, so `GOLD`'s full-lake figure is higher (74 applied, −46.8; section 3). The five handovers get
  two ids each. Barrick's `close_tr` runs on across `ABX`→`GOLD` with the price (−3.25% on 2019-01-02, the price
  move).

### 4.5 Limits

- A segment no snapshot falls in keeps today's behavior. Annual snapshots miss holders that lasted under a year
  between two June 30ths. `build_holder_lines.py` reports them; a targeted per-ticker lookup fills them (one
  request each, via `--extra-asof`).
- A line with no FIGI and no record in today's table gets a `CIK__` id that does not join to
  `security_master.holder_id` today. It is a stable, correct key, but strategy-lab should keep joining on ticker.
- An exchange offer with a price break is split into two lines: VimpelCom's 2010 OJSC→Ltd exchange, 12% over 3
  missed sessions. The returns across it are lost, not wrong.
- `polygon_ingest.universe.classify_segments` still takes names and types from today's table, so
  `segments.parquet` keeps saying "Abacus" for `ABX`. That is informational, since membership joins on ticker,
  but it should read `holder_lines` too (follow-up).
- Massive's filing rule is inferred from these cases, not documented. The report lists every record not filed
  as-is, so the inference is checked on every run.

## 5. Rollout (blocked: the owner's license decision first, then the go-ahead for the pull)

**None of this may run until the owner lifts the gate on building or copying datasets** (see the status line).
Step 1 also writes a new table into `refdata/_market`, so it is gated along with the builds, not just by its
request count. When the gate lifts, the plan is: build beside, verify, swap, keep the prior build. Pin the code
to this branch: the editable install follows the main checkout. Export `POLYGON_API_KEY` first;
`scripts/pull_ref_data.sh` loads it from `.env`, but these steps call the Python entry points directly.

```bash
export POLYGON_MIN_INTERVAL_SEC=12.5 PYTHONPATH="$PWD/src"
L=/Users/mengren/local/parquet_lake
# 1. snapshots: 23 dates x ~11 pages, about an hour at 5 req/min; resumable
python legacy_scripts/run_pullers.py --bulk --tables "" --no-normalize --asof-dates annual \
  --tickers $L/universes/all_tickers.json --outdir $L/refdata/all.new --market-dir $L/refdata/_market
# 2. lines and re-filed actions (no requests); review _market/holder_lines_report.csv
python scripts/build_holder_lines.py --lake $L/day/all --market-dir $L/refdata/_market \
  --overrides data/refdata_overrides/action_holders.csv
# 3. derive beside the current refdata
python legacy_scripts/run_pullers.py --bulk --tables "" --no-normalize --holder-lines \
  --tickers $L/universes/all_tickers.json --outdir $L/refdata/all.new --market-dir $L/refdata/_market
# 4. adjusted day lake beside the current one (minutes)
python legacy_scripts/factor_builder.py --prices $L/day/all --refdir $L/refdata/all.new --granularity day \
  --outdir $L/day_adj/all_adjusted.new --adjust both --materialize ohlc --workers 12 --write-workers 8
# 5. accept
python scripts/audit_symbol_holders.py --lake $L/day/all --lake-adj $L/day_adj/all_adjusted.new \
  --refdata $L/refdata/all.new --market-dir $L/refdata/_market --universe $L/universes/top1000_cs_monthly --out /tmp/audit.new
```

Acceptance:

- No flagged segment carries today's holder id unless it is the same company.
- `A_segments_with_dividends_via_holder_id` and `A_segments_with_splits_via_holder_id` are 0.
- The five class-B handovers each have two ids.
- The 10 confirmed class-C segments carry no dividends but their own.
- Spot checks, in `close_tr`:
  - `ABX`: Barrick's 49 dividends.
  - `GOLD`: no +102% on 2022-06-07 and no −79.5% on 2019-01-02.
  - `BBT`/`TFC`: one Truist id, no Berkshire Hills dividends.
- `B`'s Barnes segment, `MRK`, `SGP` and `APC` are unchanged.

Then:

- Move `refdata/all` and `day_adj/all_adjusted` aside to `_prev_holderfix/`.
- Rename `.new` into place.
- Rebuild `minute_adj/all_adjusted` from the same refdata, about 13 minutes.

The universe needs no rebuild for membership, which is by symbol and segment.

## 6. For strategy-lab (`ML_Alpha_Research`, reads `day_adj/all_adjusted`)

Until the rebuild:

- **Treat these days as missing:**
  - `GOLD` 2022-06-07: +102.2%, a foreign split.
  - `GOLD` 2019-01-02: −79.5%, the Randgold→Barrick handover.
  - `ACH` 2010-04-01: +52.9%, a foreign split.
  - `T` 2005-12-01: +24.3%, the handover.

  They are single-day errors in member months, large enough to dominate a cross-sectional return.
- **Drop or flag the 781 member-months with a total-return error**: `ABX`, `ACH`, `BBT`, `GOLD`, `MICC`, `VIP`.
  Also `T` from 2003-11 to 2005-10, and `LSI#SEG0` and `RIO#SEG0`. Their `close_tr` drifts by 0.7 to 7 log % a
  year (35 for `LSI#SEG0`).
- **Do not group by `id` across these segments:** 490,110 rows carry today's company's id. `ABX`'s Barrick bars
  share Abacus's id, and `BBT`'s BB&T bars share Beacon's. Grouping by ticker within a segment is correct for
  them today.

After the rebuild:

- The `id` changes on every re-attributed segment. All 416 flagged ones where a snapshot names the holder, plus
  every class-B and class-C fix.
- Several series join up: Barrick becomes one id across `ABX`→`GOLD`→`B`, BB&T one with Truist across `BBT`→`TFC`.
- Any per-id state (factor histories, return caches) must be recomputed.
- New ids such as `CIK__0001176338` do not join to `security_master.holder_id` built from today's table.
  Keep joining on ticker and date, as the README already advises.
