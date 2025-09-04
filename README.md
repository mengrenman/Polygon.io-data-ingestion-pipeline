# Polygon.io Lake Builder

A small, batteries-included pipeline to turn **Polygon.io flat files** into a local **Parquet lake**, pull **refdata** (splits/dividends/security master), and build **adjusted** lakes (split-adjusted + total-return). Scripts are reproducible and notebook-friendly.

---

## Features

- Reproducible **unadjusted** lakes (minute/day).
- **Refdata** pullers (security master, splits, dividends).
- **Adjusted** lakes (split-adjusted OHLC/VWAP/Volume + total-return).
- Helper scripts to build **ticker lists** (SPX, NDX, combined) or extract from flatfiles.
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
- `pandas`, `pyarrow`, `tqdm`, `typer`, `polygon` (Polygon API client)

Install in editable mode:

```bash
python -m venv .venv && source .venv/bin/activate
pip install -U pip
pip install -e .
