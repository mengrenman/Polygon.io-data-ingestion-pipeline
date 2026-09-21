# Expanding beyond US equities — options, futures, indices

**Status: a plan, not implemented.** Nothing in this repository ingests options, futures or indices
today. This document records what the vendor actually sells, what it costs, what the existing
pipeline would and would not reuse, and the order the work should happen in.

Researched 2026-09-21 against Massive's own documentation, with a second independent pass
re-checking every figure that a purchase decision rests on. Claims below are marked **(verified)**
where a second reader confirmed them on a primary docs page, **(single source)** where only one
reader saw them, and **(estimate)** where they are derived rather than read. Pricing changes;
re-check before buying.

> **Massive is Polygon.io.** The rebrand completed 2025-10-30 — same company, same account, same
> endpoints, same S3 flat files. Existing SDKs and URLs keep working, so this is buying entitlements
> on the account this pipeline already uses, not integrating a new vendor.

---

## 1. What to buy

Individual (non-professional) monthly pricing. 20% discount on annual billing. Business tiers are
separately priced and are for redistribution/commercial use, which a single researcher does not
need — individual plans are licensed personal/non-commercial and forbid redistribution. **(verified)**

| asset class | tiers | recommendation |
|---|---|---|
| **Options** | $0 / $29 / $79 / **$199** | **Advanced $199.** Only Advanced unlocks full history to 2014-06-02; Starter caps at 2 years, Developer at 4. |
| **Stocks** | $0 / **$29** / $79 / $199 | **Starter $29 minimum.** The current 5 requests/minute limit is the Basic tier; every paid tier is unlimited. |
| **Futures** | $0 / $29 / **$79** / $199 | **Developer $79** — adds trades and quotes over Starter's minute aggregates. Caveats in §4. |
| **Indices** | $0 / **$49** / $99 | **Starter $49**, with low expectations. History starts 2023-02-14 even on the top tier. |
| **Currencies** (forex + crypto, one product) | $0 / $49 | **Skip.** Reasoning in §5. |
| **Benzinga** (partner add-on) | $99/month **per dataset** | **Not yet** — two correctness questions to resolve first. See §6. |

Flat-file (S3) access is **bundled into the tier**, not a separate add-on. **(verified)** Within an
asset class the gating is by *data type*: for options, aggregates need Starter, trades need
Developer, quotes need Advanced. **(verified)**

### Worth checking on the account first

This repository's lake holds 22 years of equity flat files, but the API key is rate-limited to 5
requests/minute, which is the Basic-tier signature. Those two facts do not sit together — either the
plan was downgraded after the bulk download, or the entitlement differs from what the tier implies.
Resolve this before buying, because it changes what the stocks line needs to be.

---

## 2. The number that sets the storage budget

Options **quotes** flat files, read from Massive's file browser: **(verified)**

| year | size |
|---|---|
| 2022 | 19.8 TB |
| 2023 | 22.3 TB |
| 2024 | 23.5 TB |
| 2025 | 30.1 TB |
| 2026 (partial) | 24.3 TB |

Roughly **120 TB**. Options **trades**, by contrast, are **9.0–13.5 GB/year**. **(verified)** Three
orders of magnitude apart.

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

**Recommendation: buy Advanced for the history depth, ingest aggregates and trades, and do not
mirror quotes.** Pull quotes for a specific study, into scratch space, and delete them after.

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
   is the options analogue of the holder-id problem, and it will need the same treatment
   `security_type.py` gave the missing equity type: inference, validated against a known-answer
   sample, with a provenance column saying which rows were inferred.

### Do not build an adjusted lake for options

Each contract is its own instrument with its own terms. A split changes *which contract exists*, not
the price of a continuing one, so there is no split-adjusted series to construct and `factor_builder`
does not apply.

### Time-sensitive: greeks, IV and open interest cannot be backfilled

Greeks, implied volatility and open interest appear **only** on the three snapshot endpoints, as an
on-demand calculation with no historical series and no backfill offered. **(verified)** Every day
without a stored snapshot is a day of that history that cannot later be bought. If those series
matter, the collector should start **before** the ingestion work, not after.

---

## 4. Futures

- Four exchanges (CME, CBOT, COMEX, NYMEX), each with minute aggregates, session aggregates, quotes
  and trades. **(verified)**
- Minute aggregates carry `session_end_date`, `exchange` and `dollar_volume` — so **the vendor has
  already solved the 23-hour-session date problem**; this pipeline does not need to invent a session
  rule, only to stop using the equity ET-calendar-date rule, which would silently misfile a Sunday
  evening open. **(verified)**
- History from **2017-04-03**. **(verified)**
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

**Indices.** ~13,335 tickers from Nasdaq, Cboe and CME. `I:SPX`, `I:DJI` and `I:VIX` require a paid
tier; `I:NDX` and the Nasdaq Composite are free. **(verified)** Day and minute aggregates carry no
volume or transactions columns — which this pipeline already tolerates, since `usecols` is built from
the header. The decisive limitation: **history starts 2023-02-14 even on the "all history" tier**,
shallower than the pricing page's "1+ Year Historical Data" language implies. **(verified)** For a
2003–2025 backtest, an index series beginning in 2023 is not a usable benchmark; SPY/IVV in the
existing equity lake give 22 years instead. Buy it for forward-looking work.

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
a base Stocks subscription is *also* required is **unresolved** — the "no base subscription required"
line on the pricing page belongs to a structurally different section (specialized datasets), not to
partner data. **(verified as unresolved)**

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

**Not in the same round as the market-data purchase.** Options is a clean extension of machinery
that already exists and is already trusted; Benzinga is a new ingestion pattern, new licensing, and
two open correctness questions. Resolve these first, both cheap:

1. **Does the archive cover delisted names?** Testable with a handful of REST calls against `AABA`,
   `ABGX`, `ABFS` during a trial.
2. **Does a historical query return the original record or the current one?** Ask support — the docs
   are silent and it is not inferable.

If both come back clean, buy **one** dataset, not the suite. **News** ($99) has the deepest history
and widest coverage. But if the real goal is event studies rather than NLP, **Earnings** is the
better first purchase: structured actuals and estimates with surprise fields, 70.6% coverage, and no
text-processing layer to build. Skip Analyst Insights until Massive reconciles its own two pages,
and skip Bulls/Bears Say outright — a current snapshot cannot be backtested.

Note the cost shape: at $99 *per dataset*, News + Earnings + Ratings is $297/month, comparable to
the entire market-data stack. And the individual tier is licensed personal/non-commercial,
"display use only". **(verified)**

---

## 7. Suggested order

| phase | work | why here |
|---|---|---|
| 0 | S3 credentials; measure the aggregate prefixes; start the greeks/IV/OI snapshot collector | Sizes are unpublished, and snapshot history is unrecoverable once a day passes |
| 1 | Fetch layer | Nothing else can run without it, and it does not exist for any asset class |
| 2 | Options **day** aggregates end to end | Proves symbology parsing, underlying-partitioning and the contract master against small data |
| 3 | Options **minute** aggregates | Proves scale |
| 4 | Options **trades** | ~10 GB/year; cheap once the layout is settled |
| 5 | Futures | Session handling and roll construction — the most genuinely new code |
| — | Indices | Small and independent; slot in anywhere |
| — | Options **quotes** | Never mirrored. Pull per study, into scratch, and delete |

## 8. The architectural decision

"Stocks" is currently implicit everywhere. The cheap generalisation is an **asset-class config
object** threaded through — carrying the symbol parser, the session/trading-date rule, whether
corporate actions apply, and the expected columns — with lake roots becoming
`lake/<asset_class>/<tf>/<collection>`. The alternative, `if asset_class == "options"` scattered
across ingest, `lake_io` and the builders, will rot.

Equally important is being explicit about what stays **equity-only**: the point-in-time universe,
`factor_builder`, holder ids, and the security-type inference. Those are about *companies*. They
should not be generalised.
