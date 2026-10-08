# Expanding beyond US equities — options, futures, indices

*Revised 2026-10-05 to follow the owner's month plan
(`ML_Alpha_Research/strategy-lab/docs/plan-2026-09-30-month.md`): Stocks Starter plus Financials and
Benzinga Earnings on day 1, options and futures not this month, indices and currencies declined.
Changed: Stocks comes first, because the equity data ends 2025-08-13 (§1); greeks and IV can be
derived later, so open interest is the only options series that needs a daily snapshot, and Options
Starter is enough to take it (§3); futures need Advanced, not Developer, for full history (§4);
indices change to skip (§5); Benzinga follows the plan (§6); the order is rebuilt (§7). A re-read of
Massive's docs on 2026-10-05 corrected the futures tier and the indices depth wording.*

*Revised 2026-10-09: the account is Basic on every asset class, read from the dashboard, so the
day-1 purchase is needed (§1); per-day options quote sizes from the vendor's file browser (§2); a
2022-onward IV pilot from Massive's own quotes (§3); the live futures venue list (§4).*

**Status: a plan, not implemented.** Nothing in this repository ingests options, futures or indices
today. This document records what the vendor actually sells, what it costs, what the existing
pipeline would and would not reuse, and the order the work should happen in.

Researched 2026-09-21 against Massive's own documentation, with a second independent pass
re-checking every figure that a purchase decision rests on. Claims below are marked **(verified)**
where a second reader confirmed them on a primary docs page, **(single source)** where only one
reader saw them, and **(estimate)** where they are derived rather than read. Pricing changes;
re-check before buying.

Pricing, history-depth and entitlement claims were re-read on 2026-10-05 by two independent readers
of the pricing page and docs; they did not disagree on any claim recorded here. Where Massive's own
pages disagree with each other, the text says so and uses the docs' plan-history tables.

> **Massive is Polygon.io.** The rebrand completed 2025-10-30 — same company, same account, same
> endpoints, same S3 flat files. Existing SDKs and URLs keep working, so this is buying entitlements
> on the account this pipeline already uses, not integrating a new vendor.

---

## 1. What to buy

**Money is not the constraint.** Decide each purchase on two questions: (i) what engineering is
needed to use the data, and (ii) does waiting lose data that cannot be bought later? The all-history
tiers start on fixed dates: options aggregates and trades 2014-06-02, futures 2017-04-03, indices
2023-02-14. **(verified)** On the docs' all-history reading, waiting loses nothing there: the full
history stays available on the all-history tier (conflict note below). **(estimate)** The lower
tiers state their depth as N years, which reads as a trailing window that sheds its oldest days over
time; the docs never use the word "rolling". **(estimate)**

Individual (non-professional) monthly pricing, re-read 2026-10-05 for stocks, options, futures,
indices and Benzinga and unchanged **(verified)**; currencies were not re-read. 20% discount on
annual billing. Business tiers are separately priced and are for redistribution/commercial use,
which a single researcher does not need — individual plans are licensed personal/non-commercial and
forbid redistribution. **(verified)**

| asset class | tiers | recommendation | owner's month plan |
|---|---|---|---|
| **Stocks** (+ Financials add-on) | $0 / **$29** / $79 / $199; Financials & Ratios $29, or included in Advanced | **First: Starter $29 + Financials $29 = $58.** Developer (trades) or Advanced (quotes) only when that work exists. The account is Basic today (below). | **Day 1: Starter + Financials ($58) by default.** |
| **Options** | $0 / $29 / $79 / **$199** | **Advanced $199** for the archive: only Advanced reaches 2014-06-02 (Starter 2 years, Developer 4). Nothing forces it now if the docs' all-history reading holds (pricing page says 5+ years; conflict note below). **Starter $29** is enough for a snapshot collector (§3). | Not this month. |
| **Futures** | $0 / $29 / $79 / **$199** | **Advanced $199** for full 2017+ history; Developer $79 is a 5-year window. No deadline if the docs' all-history reading holds (pricing page says 7+ years); confirm the first file on the first listing. Caveats in §4. | Not this month. |
| **Indices** | $0 / $49 / $99 | **Skip** until a live use exists, then Starter $49. Reasoning in §5. | Declined. |
| **Currencies** (forex + crypto, one product) | $0 / $49 | **Skip.** Reasoning in §5. | Declined. |
| **Benzinga** (partner add-on) | $99/month **per dataset** | **Earnings now, News after a frozen text spec.** Open questions in §6. | **Earnings day 1; News after week 4.** |

Flat-file (S3) access is **bundled into the tier**, not a separate add-on. **(verified)** Within an
asset class the gating is by *data type*: for options, aggregates need Starter, trades need
Developer, quotes need Advanced. **(verified)** Stocks follows the same ladder; futures gives trades
and top-of-book quotes together on Developer; indices has aggregates and values only; Basic has no
flat files on stocks, options, futures or indices. **(verified)**

**Conflict note.** The pricing page labels Options Advanced "5+ Years" and Futures Advanced "7+
Years" where the docs say all history from 2014-06-02 and 2017-04-03, and the landing pages show
other start dates (stocks 2003-09-01, options 2014-07-18, futures 2017-05-13, indices 2023-03-09).
**(verified)** If the pricing labels are what a subscription delivers, the Advanced tiers are
windows too and waiting loses the oldest data, so the "no deadline" conclusions for options and
futures hold only on the docs' reading. **(estimate)** Confirm the first available file on the first
listing before relying on it.

### Stocks comes first

Every lake, the universe and every study stop at **2025-08-13**: the last flat file on disk is
`~/local/flatfiles/{day,minute}_aggs_v1/2025/08/2025-08-13.csv.gz`, and the day, adjusted-day and
minute lakes under `~/local/parquet_lake` end in 2025-08 — about 13.7 months before 2026-10-05.
Options and earnings work also need underlying prices for the same dates (implied volatility needs
the underlying; an event study needs returns around every announcement), so they inherit the gap.

- **Catching up reuses existing code unchanged:** `poly bars --layout market` on the new files, then
  the adjusted-lake build (README Steps 3 and 5), then the Step 2 point-in-time universe. The Step 4
  reference-data pull also needs a re-run for splits and dividends after 2025-08-13. **(estimate)**
- **The plan's precondition is already met.** The owner's plan reads the refreshed lake only once
  "the pipeline's September review findings, a total-return factor inverted among them" are closed.
  All five findings of that 2026-09-15 review are closed in this repository: the inverted
  total-return factor and the raw dividend divided by a split-adjusted base (PR #1, known-answer tests
  in `tests/test_adjustments.py`), UTC instead of ET date partitioning (PR #1), the missing FIGI and
  ticker-event stitching (holder ids, `pull_ticker_events`), and the survivorship-biased universe
  (the point-in-time `universes/top1000_cs_monthly`).
- **Tier.** The minimum tier whose flat-file history covers 2025-08-14 to the present is **Starter
  ($29)**: day and minute aggregates, 5 years. **(verified)** Five years reach back to about 2021-10,
  well before the gap opens. **(estimate)** Add
  the **Financials & Ratios** add-on ($29, also included in Advanced) **(verified)** and the Stocks
  line is **$58/month**, the owner's plan default. Massive's own pages disagree on whether that
  add-on is sold without a Stocks subscription (pricing page: yes; knowledge-base article: it extends
  one you hold) **(verified)** — moot when Starter is the base plan.
- **A higher tier only when trades or quotes work exists.** Trades need Developer ($79, 10 years);
  quotes need Advanced ($199, all history from 2003-09-10). **(verified)** The plan holds that work
  (primary-exchange close, adverse selection) behind an open question, so no month-one work needs
  either.
- **The account does not hold Stocks Advanced.** The plan says "Keep Advanced" and the 2026-09-15
  tier note says "keep Stocks Advanced", but the dashboard shows Basic on every asset class (below).
  The day-1 purchase is needed, and the refresh cannot start without it.
- **Financials are REST, not flat files, and `filing_date` is not a point-in-time date.**
  Statements run from 2009-03-29 with `period_end` and `filing_date`. **(verified)** The docs
  define `filing_date` as the most recent SEC filing that included the period's data, "not
  necessarily the date this period was originally filed": a 10-K restates three years, so a
  quarter's record carries the date of the latest filing that repeated it, and the values are
  the restated ones. **(verified)** Keyed on `filing_date`, a backtest sees each quarter late;
  keyed on `period_end` plus a lag, it sees restated values early. The original filing date
  comes from the EDGAR filings index (`/stocks/filings/vX/index`); nothing documented gives the
  values as originally reported. The ratios endpoint is current-day only, with no history: the
  docs give it no date parameter and plan history "Not applicable". **(verified)**
- **The 5 requests/minute limit is the Basic tier;** every paid tier on every asset class is
  unlimited. **(verified)** It is counted per asset class, so a paid Options plan would leave stock
  REST pulls at 5/minute. **(single source)**

### The account today

Read from the dashboard's Plans & Upgrades page on 2026-10-09: **Stocks, Options, Futures, Indices
and Currencies are all Basic, $0/month**, individual, with no add-ons — no Financials & Ratios and no
Benzinga dataset. That explains the two observations made earlier with the API key in the repo `.env`:

- It was rate-limited to 5 requests/minute during the September reference-data pulls (recorded
  2026-09-17): the Basic limit. **(verified)**
- A call to `/v3/snapshot/options/AAPL` returned `403 NOT_AUTHORIZED` on 2026-09-21: the snapshot
  endpoints are not included on Options Basic. **(verified)**

Basic includes **no flat files at all** **(verified)**, so the equity refresh needs at least Stocks
Starter plus S3 access keys from the dashboard, which are separate from the API key. The 22 years
of flat files on disk (ending 2025-08-13) reach back to 2003-09-10, which only Stocks Advanced
covers, so they came from an earlier paid plan or another route; the billing history, not yet
read, would say which. **(estimate)**

---

## 2. The number that sets the storage budget

Options **quotes** flat files, read from Massive's file browser: **(verified)**

| year | size |
|---|---|
| 2022 | 19.8 TB |
| 2023 | 22.3 TB |
| 2024 | 23.5 TB |
| 2025 | 30.1 TB |
| 2026 (partial) | 25.9 TB as of 2026-10-09 (24.3 TB on 2026-09-21) |

Roughly **120 TB**. Options **trades**, by contrast, are **9.0–13.5 GB/year**. **(verified)** Three
orders of magnitude apart.

**Per day**, each session is one whole-market `.csv.gz`. June 2025's 20 files run **91.7–132 GB**,
mean 105 GB, read from the file browser on 2026-10-09. **(verified)** The yearly totals average
about 90–95 GB/day for 2022–2024, about 120 GB/day for 2025 and about 130 GB/day for 2026 so far.
**(estimate)** There is no way to fetch only part of a day: a study that needs the close downloads
the whole file.

Day and minute aggregate sizes are **not published** — the docs pages carry schemas and history
dates but no file sizes. **(verified)** Measure them with `ListObjectsV2` over the prefix before
committing disk; that is the single highest-value hour of this whole plan.

Scaling constants measured from this repository's own lakes, for converting a row count into disk:

| lake | files | size | rows | bytes/row |
|---|---|---|---|---|
| `day/all` | 264 | 0.9 GB | 46.5 M | 18.3 |
| `minute/all` | 5,517 | 98.6 GB | 7.04 bn | **14.0** |
| `day_adj/all_adjusted` | 264 | 2.6 GB | 46.5 M | 55.8 |
| flat files (minute, `csv.gz`) | 5,517 | 75.2 GB | — | source |

Two consequences. Parquet here is **larger** than the gzipped CSV it came from (98.6 vs 75.2 GB), so
keeping both costs ~1.8×. And adjustment roughly quadruples per-row cost (14 → 56 bytes/row).

**Recommendation, when options work starts: buy Advanced for the history depth, ingest aggregates and
trades, and do not mirror quotes.** Pull quotes for a specific study, into scratch space, and delete
them after.

---

## 3. Options

### Datasets

Four flat-file datasets, no others. **(verified)**

| dataset | columns | history from |
|---|---|---|
| day aggregates | `ticker, volume, open, close, high, low, window_start, transactions` | 2014-06-02 |
| minute aggregates | identical 8 columns | 2014-06-02 |
| trades | `ticker, conditions, correction, exchange, participant_timestamp, price, sip_timestamp, size` | 2014-06-02 |
| quotes | `ticker, ask_exchange, ask_price, ask_size, bid_exchange, bid_price, bid_size, sequence_number, sip_timestamp` | **2022-03-07** |

Quotes start eight years later than everything else. **(verified)** The trades page documents seven
columns in its schema table but shows eight in its own example, the extra being
`participant_timestamp`. **(verified)** — treat the schema table as incomplete and detect columns
from the header, which this pipeline already does.

### What the existing pipeline already handles

Traced against the actual code, not assumed:

- **Options aggregates would ingest today, unmodified.** The day and minute aggregate schema is
  byte-identical to the stock aggregates this pipeline already reads. `detect_header`/`detect_col`
  scan for any of `TS_CANDS`/`TICKER_CANDS`; `usecols`/`dtypes` are built only from columns present
  in the header; `_write_bucket` filters `base_cols` to those that exist. `poly bars --tf day
  --layout market` works as-is.
- **ET trading-date partitioning is correct** for US options, which keep US equity market hours.
- **The case-collision guard never fires** — options symbols do not use the equity share-class
  letter-case convention, so `_clashes` is inert rather than wrong.
- **`lake_io`'s selection and loading are asset-class-agnostic** path and time-range plumbing.

### What does not transfer

- **`universe.py`** is equity-only end to end: share class, symbol recycling, common-stock filtering.
- **`security_type.py`** solves a problem that does not recur — options reference data carries
  explicit `contract_type`/`strike_price`/`expiration_date`.
- **`polygon_pullers`** calls equity-only endpoints and builds a company-identity security master.
- **`factor_builder`'s adjustment layer** assumes corporate actions applied to a *continuing
  security*. Its I/O half (reading prices, writing a partitioned lake) is reusable; everything
  downstream of holder identity is not.

### Four things to build

1. **A fetch layer.** There is no downloader in this repository — README Step 1 is "place the files
   on disk", and the equity flat files arrived by an out-of-band route it does not record. Endpoint
   `https://files.massive.com`, bucket `flatfiles`, prefix `us_options_opra/`, updated daily by
   ~11:00 ET. **(verified)** Needs S3 credentials from the dashboard, which are **separate from
   `POLYGON_API_KEY`** and are not configured on this machine.

2. **Partition by underlying, not by contract.** Parse `O:SPY230327P00390000` into underlying,
   expiry, right and strike at ingest, and bucket on the underlying — roughly 5,000 directories
   rather than ~1.5 M. The grammar is `O:` + root + `YYMMDD` + `C`/`P` + strike×1000 as an 8-digit
   zero-padded integer. **(verified)** This is the one substantial change to `ingest.py`, and the
   decision that is expensive to reverse once terabytes are written.

3. **A contract master.** No reference data exists in flat files; it is REST-only via
   `/v3/reference/options/contracts`, which supports `expired=true` and an `as_of` date for
   point-in-time lookups. **(verified)** Included on every options tier including free. Unlimited
   request rate on any paid tier makes backfilling it cheap.

4. **Adjusted-contract identity — the hard part, and the vendor does not help.** There is **no
   corporate-actions endpoint for options at all**. **(verified)** The only signal that a contract is
   non-standard is a non-empty `additional_underlyings` array — e.g. an AAPL contract also delivering
   44 shares of VMW and $6.53 cash. There is no `is_adjusted` flag, and the OCC alternate-root
   grammar (`SPY1`, `AAPL2`) is **undocumented anywhere on the vendor's site**. **(verified)** This
   is the options analog of the holder-id problem, and it will need the same treatment
   `security_type.py` gave the missing equity type: inference, validated against a known-answer
   sample, with a provenance column saying which rows were inferred.

### Do not build an adjusted lake for options

Each contract is its own instrument with its own terms. A split changes *which contract exists*, not
the price of a continuing one, so there is no split-adjusted series to construct and `factor_builder`
does not apply.

### Greeks and IV can be derived later, per study; open interest cannot

Greeks, implied volatility and open interest appear **only** on the three snapshot endpoints (chain,
single contract, unified): `greeks` (delta, gamma, theta and vega only **(single source)**),
`implied_volatility` and `open_interest`. None of the four flat-file datasets, the aggregates
endpoints or `/v3/reference/options/contracts` carries any of them. The snapshots take no as-of date
and have no plan history, and the docs index lists no historical endpoint for any of the three — an
absence in the documentation, not a test against the live API. **(verified)** The pricing card's
"Daily open interest" bullet is backed in the docs only by the snapshot's latest value, not by a
history.

**Greeks and IV need no stored snapshots.** They can be computed later from historical option prices
plus underlying prices, rates and dividends. **(estimate)** The caveats decide how good the result is:

- **Price source.** Clean IV needs synchronous bid/ask midpoints, which exist only in the quotes
  flat files (Advanced, from 2022-03-07), and §2 says never to mirror those. So "derivable later"
  means one of two things: per study, from quotes pulled into scratch space (§2) rather than a
  lake-wide mirror; or approximately, from the day and minute aggregates this plan does store. Those
  are trade-based, asynchronous with the underlying and thin on illiquid strikes, so IV backed out of
  them is noisy; before 2022-03-07 they are all that exists. **(estimate)**
- **American exercise.** Single-stock options are American-exercise, so Black-Scholes is the wrong
  model for them; use a binomial tree or Barone-Adesi–Whaley. Massive's own snapshot greeks come from
  a Cox-Ross-Rubinstein binomial model on the bid/ask midpoint, and the contract master carries an
  `exercise_style` field. **(single source)**
- **Dividends and the forward.** An implied forward from put-call parity (same expiry) avoids needing
  a dividend forecast, or, for index options, the index level. Parity is exact only for European
  options, such as SPX-style index options; for American single-stock options it holds only as an
  inequality, so the implied forward is a bounded approximation, biased where Black-Scholes is
  already wrong (dividend payers, deep in-the-money puts). **(estimate)** Rates and dividends come
  from outside the options product; this pipeline's reference-data step already pulls dividends, but
  those are realized, ex-post amounts, not the ex-ante yield Massive uses. That is fine for ex-post
  greeks but a look-ahead if used as the market's expectation. **(estimate)**

**A 2022-onward IV pilot from Massive's own quotes is viable** as a first options study, so Massive
is the wrong vendor only for implied volatility before 2022, or for IV that arrives already computed.
What it takes and what it lacks: **(estimate)**

- **Cost:** Options Advanced ($199/month) for as long as the pull runs, and about **122 TB** of
  download for 2022-03-07 to 2026-10 (§2), since each day comes whole. At about 100 MB/s that is
  roughly two weeks of continuous transfer; at 100 Mbit/s, months. Storage stays small if each day
  is reduced to a near-close snapshot and then deleted.
- **Work:** an IV engine built and validated here, with American exercise, dividends and rates, as
  above. `mcp_massive` does not provide one (below).
- **Limits:** about 4.6 years is roughly 55 monthly or 240 weekly cross-sections: enough for a large
  effect, thin for one that decayed after publication. No 2008 or 2020 stress regime, and no
  open-interest history at all.
- **An untested middle path:** Advanced also serves quotes per contract over REST. Querying only the
  contracts a signal uses (for example at-the-money and 25-delta near 30 days), in a window near the
  close, might avoid the full download. Its throughput has not been checked.

For IV history before 2022, or IV that is already computed and checked, the vendors listed under
open interest below are the sources.

**What `mcp_massive` ships** (read from source at commit `c58ec7e`, 2026-05-05 **(single source)**):
eleven closed-form **Black-Scholes** functions — price, delta, gamma, theta, vega, rho, vanna,
volga, charm, veta and color — callable through the `apply` parameter of its `call_api` and
`query_data` tools (its README lists only six). Each takes spot, strike, time to expiry in years,
a rate and **a volatility you supply**, so it computes greeks *given* an IV. There is **no IV
solver** (no root-finder, no scipy dependency), **no dividend-yield input** (charm, veta and color
state they assume none; the others have no such parameter) and **no American-exercise handling**. It
cannot turn a price into an IV, and for American options it is the wrong model; it does not replace
the derivation above.

**Open interest cannot be derived from prices.** **(estimate)** Within Massive it exists only in the
snapshot, as the quantity held at the end of the last trading day, so a history is only what was
snapshotted on the day. Every day without a stored snapshot is a day of Massive-sourced open
interest that cannot later be bought. **Options Starter ($29)** is the first tier with the snapshot
endpoints — 15-minute delayed on Starter and Developer, real-time only on Advanced, whatever the
pricing card's "Real-time Greeks and IV" bullet implies **(verified)** — so the collector needs only
Starter and is **decoupled from the archive purchase**. The account is Options Basic today, so it
cannot call the snapshot (§1). Start the collector before the ingestion work only *if open interest
history is wanted from Massive*.

The collector is a small service, not a script: a daily history means one paginated chain snapshot
per live underlying (up to ~5,000, the all-history count above) every trading day, with storage and
the same incremental-state REST pattern as Benzinga (§6, §7 phase 1). Starter's unlimited request
rate makes it feasible. **(estimate)**

Other vendors sell historical end-of-day open interest: Cboe DataShop's Option EOD Summary (from
January 2012, with IV and greeks as an optional add-on), OptionMetrics IvyDB US (from 1996,
institutional) and ORATS (from 2007; its open interest is the prior night's OCC figure).
**(single source)** Prices were not checked. The urgency above therefore holds only if Massive is to
be the open-interest source.

---

## 4. Futures

- Four exchanges (CME, CBOT, COMEX, NYMEX), each with minute aggregates, session aggregates, quotes
  and trades. **(verified)**
- **Cboe's VX futures (CFE) are not covered.** Flat files exist only for the four CME Group
  exchanges, and no docs page names CFE or VX. The live `GET /futures/v1/exchanges` (free on
  Futures Basic, called 2026-10-09) lists 16 venues, not 4: the four, plus CME Globex partner and
  spread venues such as MGEX, BrokerTec, KRX, Bursa Malaysia and CME Amsterdam. None of them is CFE
  (MIC `XCBF`). **(verified)** No page states the exclusion outright; this is coverage by absence.
- Minute aggregates carry `session_end_date`, `exchange` and `dollar_volume` — so **the vendor has
  already solved the 23-hour-session date problem**; this pipeline does not need to invent a session
  rule, only to stop using the equity ET-calendar-date rule, which would silently misfile a Sunday
  evening open. **(verified)**
- History from **2017-04-03**. **(verified)** On the docs' all-history reading that is a fixed
  start, so **there is no deadline**: waiting loses nothing on Advanced, whereas a Developer window
  sheds its oldest day each day. If the pricing page's "7+ Years" label is what Advanced delivers, it
  is a window too and waiting does lose the oldest data (§1 conflict note); confirm the first file
  on the first listing. **(estimate)**
- **Tier: Advanced $199 for the full history.** Starter ($29) has minute and session aggregates only,
  2 years. Developer ($79) adds trades and top-of-book quotes but is a **5-year** window. Only
  Advanced has all history from 2017-04-03. **(verified)** Today the Developer window reaches back
  to about 2021-10. **(estimate)** An earlier draft of this document recommended Developer; that
  missed the window.
- **No open interest anywhere in the product.** **(verified)** A real gap: OI drives roll timing and
  positioning work.
- Settlement prices only on session-and-longer candles, not intraday. **(verified)**
- **Continuous contracts are marked "coming soon"** — rolling is the consumer's job today.
  **(verified)** That means a roll rule (volume or open-interest crossover, or N days before expiry)
  and a back-adjustment (ratio or difference), structurally parallel to the existing unadjusted →
  adjusted lake pattern.
- Docs are internally inconsistent on the ticker year-digit convention (`ESZ5` vs `ESZ25`).
  **(verified)** Detect from data rather than trusting either.

---

## 5. Indices, forex and crypto

**Indices — skip until a live use exists.** ~13,335 tickers from Nasdaq, Cboe and CME. `I:SPX`,
`I:DJI` and `I:VIX` require a paid tier; `I:NDX` and the Nasdaq Composite are free. **(verified)** Day
and minute aggregates carry no volume or transactions columns — which this pipeline already
tolerates, since `usecols` is built from the header. Four reasons not to buy now:

1. **History starts 2023-02-14 on every tier**, flat files included on Starter ($49) and Advanced
   ($99) alike. **(verified)** The pricing page's "1+ Year Historical Data" label understates this
   (about 3.6 years today), but for a 2003–2025 backtest an index series beginning in 2023 is still
   not a usable benchmark.
2. **SPY/IVV in the existing equity lake give 22 years** as the benchmark instead.
3. **VIX daily history is free.** Cboe's VIX history CSV (open, high, low, close) and FRED's
   `VIXCLS` (close only) both begin on 1990-01-02, and their first closes agree. **(single source)**
   FRED carries a copyright notice (the data are Cboe's); Cboe's terms were not checked.
4. **For European-style index options (SPX-style), the underlying level can be replaced** by the
   implied forward from put-call parity, which is exact there (§3). **(estimate)**

Free sources are daily; intraday index levels (minute aggregates) are the only index data a live use
would need Massive for, and indices has a fixed start on every tier (2023-02-14), so by criterion
(ii) waiting loses nothing. **(estimate)** Buy Starter ($49) when a live use exists; Advanced ($99)
adds no depth. **(verified)**

**Forex and crypto** are sold together as one "Currencies" product, $0 or $49, with no higher
individual tier. **(verified)** Recommendation: skip.

- Forex has **no consolidated tape** — every quote is tagged to a single synthetic exchange
  ("Currency Banks 1"). **(verified)** Coverage is stated inconsistently: 1,750+ pairs on the
  flat-file docs against ~1,200 on the FAQ. **(verified)**
- Crypto comes from five exchanges; trades carry a venue id but aggregates do not. **(verified)**
- Neither answers a research question that equities, options, futures and indices do not answer
  better. The cost is not the $49 — it is a third session model and a fourth symbology for data with
  no thesis behind it.

---

## 6. Partner data: Benzinga

Sold as a **partner add-on at $99/month per dataset**, six products priced separately. **(verified)**
Consensus Ratings is bundled into Analyst Ratings rather than sold on its own. **(verified)** Whether
a base Stocks subscription is *also* required is **still unresolved on Massive's own pages** — the
"no base subscription required" line on the pricing page belongs to the More Data section (NYSE
Order Imbalances, European Consumer Spending), not to partner data; the knowledge base calls it a
commercial question per dataset; the 2025 launch blog says only that an existing Massive account is
needed; and the docs list each Benzinga dataset as its own plan row, which leans standalone.
**(verified as unresolved)**
Checkout needs sign-in, which neither reader did. The owner's plan reads "no base plan needed" from
the secondary tier note, which neither reader could confirm. It is **moot for the plan**: Stocks
Starter comes first in it, so the likeliest form of any such requirement, a Stocks plan underneath,
would be met. **(estimate)**

### Coverage against this repository's point-in-time universe

History is **not gated by tier** — the $99 individual plan gets "all history" on both individual and
business tiers. **(verified)** Coverage below is the share of the 262,000 member-months in
`universes/top1000_cs_monthly` (2003-11 to 2025-08) that falls after each archive's start date.

| dataset | history from | covers | note |
|---|---|---|---|
| News | 2009-04-27 | **75.2%** | `GET /benzinga/v2/news`; `limit` max 50,000 |
| Earnings | 2010-04-30 | 70.6% | actuals **and** estimates, with surprise fields |
| Corporate Guidance | 2011-09-12 | 64.1% | updated every 2 hours, not real time |
| Analyst Ratings | 2011-12-08 | 63.0% | includes price targets and rating actions |
| Analyst Insights | 2020-01-02 **or** 2023 | 26.0% or 12.2% | Massive's docs and marketing page disagree by ~3 years **(verified discrepancy)** |
| Bulls / Bears Say | none | — | current snapshot per ticker; not a dated series |

Archive depth is therefore **not** the obstacle. Two other things are.

### Blocker 1 — REST only, no flat files

There is no S3/flat-file delivery for any Benzinga dataset. **(verified** — all nine partner doc
pages grepped for `flat file`/`s3`/`bulk` with zero matches, and none of the 33 entries in the
flat-files index is a Benzinga path.**)** Every other dataset this pipeline consumes arrives as a
daily flat file; Benzinga would need a second ingestion architecture — paginated REST with
incremental state and its own resume logic. The 50,000-row `limit` on News makes that more tractable
than it first appears, but it is still a pattern this repository does not have.

### Blocker 2 — no revision history, and point-in-time accuracy is undocumented

No Benzinga schema exposes a revision, correction or audit-trail field, and nothing in the docs
states whether a historical query returns a record **as it stood then** or **as it stands now**.
**(verified** — zero matches for "point-in-time" or "latency" across all nine pages; the only
timestamps are `published`/`last_updated`.**)**

This is decisive. If the API returns current state, a revised price target or a corrected article
reads back as though it had always said that, and every backtest built on it carries silent
look-ahead. That is the exact failure mode the rest of this pipeline is constructed to prevent —
see the security-type inference, where the missing field correlated with delisting and therefore
with survival.

### The unanswered question: does the archive cover dead names?

**51% of the universe's members are symbols that are no longer active** — 2,134 of 4,160, including
`AABA`, `ABC`, `ABGX` and `ABFS`. News vendors index what they currently cover, and coverage of a
company tends to stop at delisting without backfill. If that holds here, the half of the universe
carrying the survivorship signal is the half the news is thinnest on — and the join would *succeed*,
just with systematically fewer articles on the names that died. Nothing in the documentation
addresses this either way.

### Recommendation

**Earnings now, News after week 4, nothing else yet.** The owner's plan
(`ML_Alpha_Research/strategy-lab/docs/plan-2026-09-30-month.md`) lists **Earnings ($99)** for day 1
and **News ($99)** after its text specification is frozen in week 4; this replaces the earlier
advice to hold Benzinga out of the first round. Earnings was already this document's better first
purchase for event studies: structured actuals and estimates with surprise fields, 70.6% coverage,
and no text-processing layer to build. It is still a new ingestion pattern (Blocker 1), and the two
correctness questions are still open:

1. **Does the archive cover delisted names?** With monthly billing, **a month of Earnings is the
   cheapest test**: a handful of REST calls against `AABA`, `ABGX` and `ABFS` in the first week.
2. **Does a historical query return the original record or the current one?** Still needs an answer
   from support — the docs are silent and it is not inferable. Ask in week 1; until it comes back,
   treat the estimate and surprise fields as possibly carrying look-ahead. **(estimate)**

**The owner's day-3 check** adds a third: the share of announcements carrying the `time` field,
which decides the day-zero convention (the plan falls back to two-day windows if many lack it).
Earnings records carry `time` as a 24-hour HH:MM:SS string labeled EST, beside `date`, `date_status`
(projected or confirmed) and `last_updated`. The docs say nothing about how often `time` is
populated, and "EST" is ambiguous in summer. **(single source)**

**News** ($99) has the deepest history and widest coverage, and goes second because the plan freezes
its text specification before buying it. Skip Analyst Insights until Massive reconciles its own two
pages, and skip Bulls/Bears Say outright — a current snapshot cannot be backtested.

Note the cost shape: at $99 *per dataset*, News + Earnings + Ratings is $297/month, about five times
the $58 day-1 Stocks line. The plan budgets $157/month from day 1 (Stocks $58, Earnings $99) and
$256 once News is added. And the individual tier is licensed personal/non-commercial, "display use
only". **(verified)**

---

## 7. Suggested order

| phase | work | why here |
|---|---|---|
| 0 | **Stocks purchase** (Starter plus Financials; the account is Basic everywhere today, §1) and the **equity refresh, 2025-08-14 onward**: day and minute lakes, adjusted lake, universe. Needs S3 access keys from the dashboard | Every lake, the universe and every study end 2025-08-13; options and earnings work need underlying prices through the present; existing code, unchanged, and the September review findings the plan gates it on are closed (§1) |
| 1 | **Financials and Benzinga Earnings ingestion** (REST) | A new pattern for this repo — paginated REST with incremental state and resume, no flat files. Ingestion does not wait on phase 0; the earnings rows use phase 0's prices if the refresh lands by 7 October, otherwise they run on the stale panel and are re-run in month two (plan Decision 2; §6) |
| 2 | Options snapshot collector — **if open interest history is wanted from Massive** (Options Starter, $29) | Open interest is the one options series that cannot be derived or bought back from Massive; every day not snapshotted is lost. Greeks and IV are not (§3). A small service, not a script: one paginated chain call per underlying per day, with phase 1's incremental-state REST pattern. Independent of the phases below |
| 3 | When options work starts: Advanced; measure the aggregate prefixes | Sizes are unpublished; nothing forces the purchase earlier on the docs' all-history reading (§1) |
| 4 | Fetch layer | Options and futures cannot run without it, and it does not exist for any asset class; phase 0's catch-up is a one-off pull by hand (README Step 1) |
| 5 | Options **day** aggregates end to end | Proves symbology parsing, underlying-partitioning and the contract master against small data |
| 6 | Options **minute** aggregates | Proves scale |
| 7 | Options **trades** | ~10 GB/year; cheap once the layout is settled |
| 8 | Futures (Advanced) | No deadline on the docs' all-history reading (§1, §4). Session handling and roll construction — the most genuinely new code |
| — | Benzinga News | Same REST client as phase 1; after the plan's text specification is frozen (week 4) (§6) |
| — | Indices | **Skip until a live use exists** (§5) |
| — | Options **quotes** | Never mirrored. Pull per study, into scratch, and delete |

## 8. The architectural decision

"Stocks" is currently implicit everywhere. The cheap generalization is an **asset-class config
object** threaded through — carrying the symbol parser, the session/trading-date rule, whether
corporate actions apply, and the expected columns — with lake roots becoming
`lake/<asset_class>/<tf>/<collection>`. The alternative, `if asset_class == "options"` scattered
across ingest, `lake_io` and the builders, will rot.

Equally important is being explicit about what stays **equity-only**: the point-in-time universe,
`factor_builder`, holder ids, and the security-type inference. Those are about *companies*. They
should not be generalized.
