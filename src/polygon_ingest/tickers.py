#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Ticker spelling: in Polygon's data the letter case of a symbol is *information*, not noise.

`AAP` is Advance Auto Parts (common stock); `AAp` is the Alcoa Inc. $3.75 preferred. A lowercase
letter in the suffix is a class code - `p` a preferred series (`AAGpT`), `r` a right, and `w` either
a warrant (`TMCWW`) or a when-issued line (`AANw` is Aaron's when-issued, not a warrant; Polygon types
those as common stock). The day flat files hold 33,833 distinct symbols, 4,041 of them with a lowercase
letter, and 125 of those collide with another symbol once upper-cased. Upper-casing therefore
merges two securities' bars into one series; it cost the day lake 29,258 duplicated ticker-days.

The rules, applied everywhere in this package:

* **Storage keeps the source spelling.** A lake file, a security master row, a splits row all
  carry the symbol exactly as Polygon wrote it.
* **Matching between Polygon's own datasets is exact.** Prices, security master, splits and
  dividends all use Polygon's spelling, so an exact join is both correct and what keeps `AAp`
  adjusting on Alcoa's preferred actions instead of Advance Auto Parts'.
* **Matching a user-supplied list is exact first, case-insensitive only as a fallback.** A list
  typed in capitals means the capitalised symbols, so `AAP` must not drag in `AAp`. But a list
  typed in the wrong case should still work, so `resolve()` falls back to a case-insensitive
  match for entries that have no exact counterpart - and refuses to guess when that is ambiguous.

The streaming ingester cannot use `resolve()` (it never sees the whole symbol universe at once),
so it matches exactly and reports the symbols it dropped that differ from a watchlist entry only
in case. See `polygon_ingest.ingest.run_ingest`.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

__all__ = ["clean", "clean_list", "case_collisions", "resolve", "fs_folds_case"]


def clean(value) -> str:
    """A symbol as written, with surrounding whitespace removed. Never changes letter case."""
    return str(value).strip()


def clean_list(values: Iterable) -> List[str]:
    """`clean` over an iterable, dropping blanks and duplicates, preserving order and case."""
    out: List[str] = []
    seen: set[str] = set()
    for v in values:
        t = clean(v)
        if t and t not in seen:
            seen.add(t)
            out.append(t)
    return out


def case_collisions(values: Iterable[str]) -> Dict[str, List[str]]:
    """Spellings in `values` that differ only in case, keyed by their upper-cased form.

    `["AAP", "AAp", "MSFT"] -> {"AAP": ["AAP", "AAp"]}`. Used to refuse configurations that
    would have to store two securities under one name.
    """
    by_upper: Dict[str, List[str]] = {}
    for t in clean_list(values):
        by_upper.setdefault(t.upper(), []).append(t)
    return {k: sorted(v) for k, v in by_upper.items() if len(v) > 1}


def resolve(requested: Iterable[str], available: Iterable[str]
            ) -> Tuple[Dict[str, str], List[str], Dict[str, List[str]]]:
    """Map each requested symbol onto a spelling that exists in `available`.

    Exact match wins. An entry with no exact match falls back to a case-insensitive match, but
    only when that match is unique: `aapl` resolves to `AAPL`, while `AAp` resolves to `AAp`
    itself and never to `AAP`. Returns `(mapping, unresolved, ambiguous)`, where `ambiguous` maps
    a requested symbol to the several spellings it could have meant - caller's problem to report,
    because guessing between two securities is exactly the bug this module exists to prevent.
    """
    avail = clean_list(available)
    exact = set(avail)
    by_upper: Dict[str, List[str]] = {}
    for t in avail:
        by_upper.setdefault(t.upper(), []).append(t)

    mapping: Dict[str, str] = {}
    unresolved: List[str] = []
    ambiguous: Dict[str, List[str]] = {}
    for r in clean_list(requested):
        if r in exact:
            mapping[r] = r
            continue
        cands = by_upper.get(r.upper(), [])
        if len(cands) == 1:
            mapping[r] = cands[0]
        elif len(cands) > 1:
            ambiguous[r] = sorted(cands)
        else:
            unresolved.append(r)
    return mapping, unresolved, ambiguous


def fs_folds_case(path: str | Path) -> bool:
    """Whether `path`'s filesystem treats `AAP` and `AAp` as the same name.

    True on a stock macOS (APFS/HFS+) or Windows volume. A lake laid out one directory per ticker
    can hold only one spelling of a symbol on such a volume, so the ingester has to notice before
    one security's file replaces another's.
    """
    p = Path(path)
    try:
        p.mkdir(parents=True, exist_ok=True)
        probe = p / ".polygon_ingest_case_probe_A"
        probe.touch()
    except OSError:
        return False
    try:
        return (p / ".polygon_ingest_case_probe_a").exists()
    finally:
        try:
            os.unlink(probe)
        except OSError:
            pass
