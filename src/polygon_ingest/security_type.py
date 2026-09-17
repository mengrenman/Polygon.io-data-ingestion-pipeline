#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Security type for the rows Polygon leaves untyped.

`/v3/reference/tickers` returns no `type` for 6,747 of its 36,594 stock records — 28.8% of the
delisted ones, and **none** of the active ones. The gap therefore tracks survival: filtering a
universe on `type == "CS"` drops companies that were acquired or went bankrupt, hardest early.
Liquid June name-days (close >= $5, dollar volume >= $1 M) with no usable type run 22.8% in 2004,
15.6% in 2008, 9.8% in 2012, 4.1% in 2016, 1.8% in 2020 and 0.0% in 2024.

**Polygon does not hold the field.** Probed 2026-09-17: the list endpoint omits `type` for these
records with or without a point-in-time `date` (`?ticker=COWNL&date=2023-01-03` returns the row,
marked active, with no type), and the per-ticker details endpoint returns `type: None` for them too
(`ABGX`, Abgenix, delisted 2006; `ABFS`, Arkansas Best, delisted 2014). Details does return
`sic_code`, but that is the *issuer's* industry and is present on derivative lines as well — a
delisted preferred (`NHPAP`), a warrant (`TMCWW`) and a senior note (`BWNB`) all carry one — so it
cannot separate a common share from a note on the same company. A larger API budget buys request
throughput, not the missing field. Inference from the fields we hold is the only route.

## Accuracy

Measured against the 16,665 delisted records that *do* carry a type, which is the population
closest to the untyped ones (every untyped record is delisted):

    is this common stock?   precision 91.6%   recall 88.6%   accuracy 92.3%

Applied to the 6,747 untyped records: 4,570 common stock, 1,044 something else, 1,133 (16.8%)
still unresolved.

The residual errors are structured notes whose name is just the issuer's: `AESC` is "The AES
Corporation" and `AEUA` is "Anadarko Petroleum Corporation", both senior notes, both indistinguishable
from the common stock by name. More rules do not reach them.

## A caveat about the labels

Polygon's own `type` is inconsistent where it exists, so the figures above are measured against noisy
truth and understate the rules. Closed-end funds are typed `CS` (AllianceBernstein Income Fund,
BlackRock Muni Fund); ADRs are split between `CS` and `ADRC`; and when-issued lines are typed `CS`
(`AANw` is Aaron's when-issued, `ABTw` Abbott ex-distribution — *not* warrants, which is why a
lowercase `w` in a symbol is not treated as one here, unlike `p` and `r`).
"""

from __future__ import annotations

import re
from typing import Iterable, Optional, Tuple

import pandas as pd

__all__ = ["infer_type", "infer_types", "UNTYPED_SOURCES"]

# A class suffix after a dot, or the NASDAQ fifth letter: the symbol says "derivative" but not which.
_DOT_CLASS = re.compile(r"\.(U|UN|WS|W|WT|R|RT|RTS|P[A-Z]?|PR[A-Z]?|CL|CV|EC|PP|TT)$", re.I)
_FIFTH_LETTER = re.compile(r"^[A-Z]{4}[WUR]$")

# Name markers, most specific first: a name that says "WARRANTS" is a warrant whatever else it says.
_RULES: list[tuple[str, str]] = [
    ("WARRANT", r"\bWARRANTS?\b|\bWTS\b|EXERCISABLE"),
    ("RIGHT",   r"\bRIGHTS?\b|\bRTS\b|EXPIRING"),
    ("UNIT",    r"\bUNITS?\b|CONSISTING OF"),
    ("PFD",     r"\bPFD\b|PREFERRED|PREFERENCE|DEPOSITARY|DEPOSITORY|CUMULATIVE|PERPETUAL|\bPRF\b"),
    ("SP",      r"\bNOTES?\b|\bNTS?\b|DEBENTURE|TRUPS|CAP(ITAL)? SEC|\bDUE\b|SENIOR\b|\bSUB\b"),
    ("ETN",     r"\bETN\b|IPATH|ETRACS"),
    ("ETF",     r"\bETF\b|ISHARES|SPDR|PROSHARES|POWERSHARES|NEXTSHARES"),
    # Polygon types closed-end funds CS, so only an unambiguous fund name is called FUND here.
    ("FUND",    r"\bFUND\b|\bFD\b|\bPORTFOLIO\b"),
    ("ADRC",    r"\bADRS?\b|\bADSS?\b|AMERICAN DEPOS"),
    # An operating company. CORPORATION and INCORPORATED need a prefix match, not a word boundary:
    # requiring \bCORP\b left 771 delisted common stocks unlabelled, Ares Acquisition Corporation
    # and Actua Corporation among them.
    ("CS",      r"COMMON STOCK|\bCOM STK\b|\bCOM\b|ORDINARY|\bINC\b|INCORPORATED|\bCORP|COMPAN(Y|IES)|"
                r"\bCO\b|\bLTD\b|LIMITED|\bPLC\b|HOLDINGS?|\bGROUP\b|BANCORP|BANCSHARES|BANK|TRUST|"
                r"\bSA\b|\bNV\b|\bAG\b|\bLP\b|PARTNERS|\bSHS?\b|CLASS [A-Z]\b"),
]
_COMPILED = [(t, re.compile(p)) for t, p in _RULES]

#: `type_source` values that mean "still no type". Everything else names the evidence used.
UNTYPED_SOURCES = frozenset({"symbol-derivative", "unmatched", "no-name"})


def infer_type(ticker, name) -> Tuple[Optional[str], str]:
    """Infer a Polygon-style security type from a symbol and a name.

    Returns `(type, source)`. `type` is None when the evidence does not settle it, and `source`
    then says why: `symbol-derivative` (the symbol marks a class but not which), `unmatched` (no
    rule fired) or `no-name`. Otherwise `source` is `symbol` or `name`.
    """
    t = str(ticker or "").strip()
    n = str(name or "").upper()
    derivative_symbol = False
    if any(c.islower() for c in t):
        low = "".join(c for c in t if c.islower())
        # `p` and `r` are reliable class codes; `w` is not - it marks warrants AND when-issued
        # lines, which are common stock, so a `w` symbol falls through to the name rules.
        if "p" in low and "WARRANT" not in n:
            return "PFD", "symbol"
        if "r" in low and "p" not in low and "w" not in low:
            return "RIGHT", "symbol"
    elif _DOT_CLASS.search(t) or _FIFTH_LETTER.match(t):
        # The symbol proves it is a derivative but not which one, so the name is still worth
        # reading - `TMCWW` is "TMC the metals company Inc. Warrants". What the name may not do
        # is talk us out of it: an issuer-shaped name on such a symbol is the issuer's, not a
        # common share.
        derivative_symbol = True
    if not n:
        return (None, "symbol-derivative") if derivative_symbol else (None, "no-name")
    for typ, rx in _COMPILED:
        if rx.search(n):
            if derivative_symbol and typ == "CS":
                break
            return typ, "name"
    return (None, "symbol-derivative") if derivative_symbol else (None, "unmatched")


def infer_types(tickers: Iterable, names: Iterable) -> pd.DataFrame:
    """`infer_type` over two aligned sequences -> a frame with `type_inferred` and `type_source`."""
    pairs = [infer_type(t, n) for t, n in zip(tickers, names)]
    return pd.DataFrame({"type_inferred": [p[0] for p in pairs],
                         "type_source": [p[1] for p in pairs]})


def fill_type(df: pd.DataFrame, *, type_col: str = "type", ticker_col: str = "ticker",
              name_col: str = "name") -> pd.DataFrame:
    """Add `type_inferred` and `type_source` to `df`, inferring only where `type_col` is null.

    Polygon's own value always wins and is never overwritten; `type_source` is `polygon` for those
    rows, so a caller can always tell an observed type from an inferred one.
    """
    out = df.copy()
    have = out[type_col].notna() if type_col in out.columns else pd.Series(False, index=out.index)
    inferred = infer_types(out[ticker_col], out[name_col] if name_col in out.columns else "")
    inferred.index = out.index
    out["type_inferred"] = out[type_col].where(have, inferred["type_inferred"]) if type_col in out.columns \
        else inferred["type_inferred"]
    out["type_source"] = pd.Series("polygon", index=out.index).where(have, inferred["type_source"])
    return out
