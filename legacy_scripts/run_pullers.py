# run_pullers.py
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import List, Dict

import pandas as pd
from polygon import RESTClient
from polygon.exceptions import BadResponse

from polygon_pullers import (
    pull_security_master,
    pull_dividends,
    pull_splits,
    pull_ticker_events,
    load_api_key,
)


# -----------------------------
# Normalization & preflight
# -----------------------------
def normalize_guess(tk: str) -> str:
    """Safe normalization: uppercase; replace / - ^ and spaces with '.'."""
    t = tk.strip().upper()
    # Replace common separators with dot (Polygon uses dot for classes)
    for ch in ("/", "-", "^", " "):
        t = t.replace(ch, ".")
    # Collapse consecutive dots (e.g., "ABC..B" -> "ABC.B")
    while ".." in t:
        t = t.replace("..", ".")
    # Strip leading/trailing dots
    t = t.strip(".")
    return t

def candidate_variants(original: str) -> List[str]:
    """
    Build a small set of likely variants to probe:
    1) normalized dot form (preferred)
    2) dash form
    3) slash form
    4) original (as-is)
    """
    dot = normalize_guess(original)
    variants = [dot]
    if "." in dot:
        variants.append(dot.replace(".", "-"))
        variants.append(dot.replace(".", "/"))
    else:
        # if no dot, still try adding alternatives (no-op -> still fine)
        variants.append(dot.replace(".", "-"))
        variants.append(dot.replace(".", "/"))
    # Original as last fallback
    variants.append(original.strip().upper())
    # De-dup, preserve order
    seen, uniq = set(), []
    for v in variants:
        if v not in seen:
            seen.add(v)
            uniq.append(v)
    return uniq

def preflight_validate(
    tickers: List[str],
    api_key: str,
    outdir: Path,
    enable: bool = True,
) -> Dict[str, str]:
    """
    Returns a mapping original -> resolved (accepted by Polygon).
    If a ticker can't be resolved, it won't appear in the returned dict.
    Writes:
      - _ticker_normalization_map.csv
      - _missing_tickers.txt
    """
    if not enable:
        # Return identity mapping (normalized guess) without API probing
        mapping = {tk: normalize_guess(tk) for tk in tickers}
        df = pd.DataFrame(
            [{
                "original": tk,
                "normalized_guess": mapping[tk],
                "resolved": mapping[tk],
                "status": "ASSUMED",
                "tried_variants": str([mapping[tk]]),
                "message": "",
            } for tk in tickers]
        )
        df.to_csv(outdir / "_ticker_normalization_map.csv", index=False)
        (outdir / "_missing_tickers.txt").write_text("")
        return mapping

    client = RESTClient(api_key)
    rows = []
    resolved: Dict[str, str] = {}
    missing: List[str] = []

    print("[preflight] Validating tickers with Polygon.get_ticker_details ...")
    for tk in tickers:
        tried = candidate_variants(tk)
        ok = None
        last_msg = ""
        for cand in tried:
            try:
                # probe quickly with details endpoint
                _ = client.get_ticker_details(cand)
                ok = cand  # success
                break
            except BadResponse as e:
                msg = str(e)
                last_msg = "Ticker not found" if ("Ticker not found" in msg or '"status":"NOT_FOUND"' in msg) else msg
                continue
            except Exception as e:
                last_msg = str(e)
                continue

        norm = normalize_guess(tk)
        if ok:
            resolved[tk] = ok
            rows.append({
                "original": tk,
                "normalized_guess": norm,
                "resolved": ok,
                "status": "OK",
                "tried_variants": str(tried),
                "message": "",
            })
        else:
            missing.append(tk)
            rows.append({
                "original": tk,
                "normalized_guess": norm,
                "resolved": "",
                "status": "MISSING",
                "tried_variants": str(tried),
                "message": last_msg,
            })

    # Write mapping + missing files
    map_df = pd.DataFrame(rows)
    map_df.to_csv(outdir / "_ticker_normalization_map.csv", index=False)
    (outdir / "_missing_tickers.txt").write_text("\n".join(missing))

    # Summary
    print(f"[preflight] total={len(tickers)}, ok={len(resolved)}, missing={len(missing)}")
    print(f"[preflight] map: {outdir / '_ticker_normalization_map.csv'}")
    if missing:
        print(f"[preflight] missing: {outdir / '_missing_tickers.txt'} (count={len(missing)})")

    return resolved


# -----------------------------
# Main
# -----------------------------
def main():
    ap = argparse.ArgumentParser(description="Pull Polygon reference data for a ticker universe (with preflight normalization).")
    ap.add_argument("--tickers", type=Path, required=True,
                    help="Path to JSON file containing a list of tickers.")
    ap.add_argument("--outdir", type=Path, required=True,
                    help="Directory to write parquet outputs.")
    ap.add_argument("--api-key", type=str, default=None,
                    help="Polygon API key (overrides file/env if provided).")
    ap.add_argument("--api-key-file", type=Path, default=Path('.polygon_api_key'),
                    help="File containing the API key (first line). Fallback: env POLYGON_API_KEY.")
    ap.add_argument("--fail-on-missing", action="store_true",
                    help="Stop on the first missing/unknown ticker. Default: skip & log.")
    ap.add_argument("--missing-out", type=Path, default=None,
                    help="Optional path to write one-per-line list of missing/unknown tickers. "
                         "Default: <outdir>/_missing_tickers.txt (preflight also writes this).")
    ap.add_argument("--skip-events", action="store_true",
                    help="Skip pulling ticker events.")
    ap.add_argument("--no-normalize", action="store_true",
                    help="Disable normalization (still uppercases).")
    ap.add_argument("--no-preflight", action="store_true",
                    help="Disable API validation pass; use normalized guesses directly.")
    args = ap.parse_args()

    outdir = args.outdir.resolve()
    outdir.mkdir(parents=True, exist_ok=True)

    # Resolve API key early
    api_key = load_api_key(args.api_key, args.api_key_file)

    # Load & sanitize incoming tickers
    raw_tickers = json.loads(Path(args.tickers).read_text())
    if not isinstance(raw_tickers, list):
        raise SystemExit("--tickers JSON must be a list of tickers")
    raw_tickers = [str(t).strip() for t in raw_tickers if str(t).strip()]

    print(f"Tickers (input): {len(raw_tickers)}")

    # Preflight normalize + validate
    if args.no_normalize and args.no_preflight:
        # Barebones: only uppercase originals
        resolved_map = {tk: tk.strip().upper() for tk in raw_tickers}
        pd.DataFrame([{
            "original": k, "normalized_guess": k.strip().upper(), "resolved": k.strip().upper(),
            "status": "ASSUMED", "tried_variants": str([k.strip().upper()]), "message": ""
        } for k in raw_tickers]).to_csv(outdir / "_ticker_normalization_map.csv", index=False)
        (outdir / "_missing_tickers.txt").write_text("")
    else:
        # Do normalization + (optional) preflight validate via API
        if args.no_preflight:
            print("[preflight] Skipped API validation; using normalized guesses directly.")
            resolved_map = {tk: normalize_guess(tk) for tk in raw_tickers}
            pd.DataFrame([{
                "original": k, "normalized_guess": normalize_guess(k), "resolved": normalize_guess(k),
                "status": "ASSUMED", "tried_variants": str([normalize_guess(k)]), "message": ""
            } for k in raw_tickers]).to_csv(outdir / "_ticker_normalization_map.csv", index=False)
            (outdir / "_missing_tickers.txt").write_text("")
        else:
            resolved_map = preflight_validate(raw_tickers, api_key, outdir, enable=True)

    # Build the universe we will actually pull
    valid_tickers = list(dict.fromkeys(resolved_map.values()))  # de-dup while preserving order
    print(f"Tickers (resolved & unique): {len(valid_tickers)}")
    print(f"Output directory: {outdir}")

    # Where to save missing list during pulls (post-preflight skip list is usually small)
    missing_out = args.missing_out or (outdir / "_missing_tickers_pull_phase.txt")

    # [1/3] Security master
    print("[1/3] Building security master...")
    SECMASTER_PARQUET = outdir / "security_master.parquet"
    sm_df = pull_security_master(
        valid_tickers,
        out_parquet=str(SECMASTER_PARQUET),
        api_key=api_key,
        api_key_file=None,
        fail_on_missing=args.fail_on_missing,
        missing_out=missing_out,
    )
    print(f"Security master rows: {len(sm_df)} → {SECMASTER_PARQUET}")

    # [2/3] Dividends
    print("[2/3] Pulling dividends...")
    DIV_PARQUET = outdir / "cash_dividends.parquet"
    div_df = pull_dividends(
        valid_tickers,
        out_parquet=str(DIV_PARQUET),
        api_key=api_key,
        api_key_file=None,
    )
    print(f"Dividends rows: {len(div_df)} → {DIV_PARQUET}")

    # [3/3] Splits
    print("[3/3] Pulling splits...")
    SPL_PARQUET = outdir / "stock_splits.parquet"
    spl_df = pull_splits(
        valid_tickers,
        out_parquet=str(SPL_PARQUET),
        api_key=api_key,
        api_key_file=None,
    )
    print(f"Splits rows: {len(spl_df)} → {SPL_PARQUET}")

    # (Optional) Ticker events
    if not args.skip_events:
        print("[extra] Pulling ticker events (optional)...")
        EVENTS_PARQUET = outdir / "ticker_events.parquet"
        ev_df = pull_ticker_events(
            valid_tickers,
            out_parquet=str(EVENTS_PARQUET),
            api_key=api_key,
            api_key_file=None,
        )
        print(f"Ticker events rows: {len(ev_df)} → {EVENTS_PARQUET}")

    # Final summary
    print("\nDone.")
    # Preflight files:
    map_csv = outdir / "_ticker_normalization_map.csv"
    miss_txt = outdir / "_missing_tickers.txt"
    if map_csv.exists():
        print(f"Preflight map: {map_csv}")
    if miss_txt.exists():
        miss_count = len([l for l in miss_txt.read_text().splitlines() if l.strip()])
        print(f"Preflight missing: {miss_txt} (count={miss_count})")
    # Pull-phase missing (should be small if preflight ran)
    if (args.missing_out or (outdir / "_missing_tickers_pull_phase.txt")).exists():
        mp = args.missing_out or (outdir / "_missing_tickers_pull_phase.txt")
        miss2_count = len([l for l in Path(mp).read_text().splitlines() if l.strip()])
        print(f"Pull-phase missing: {mp} (count={miss2_count})")


if __name__ == "__main__":
    main()
