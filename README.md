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

---

## 📁 Directory Structure

```
repo_polygonio/
├── README.md
├── data/
│   └── ticker_lists/
│       └── spx_ndx_combined.json         # Ticker list for lake construction
├── lake/
│   ├── day/
│   └── minute/                           # Final unadjusted parquet lakes
├── lake_adj/
│   ├── day/
│   └── minute/                           # Final adjusted lakes (split- and/or TR-adjusted)
├── notebooks/
│   └── 03_load_data_inspect_adjustment.ipynb  # Sanity check for adjustments
├── refdata/
│   └── spx_ndx_combined/                 # Corporate actions (splits/dividends) per ticker
├── scripts/
│   ├── build_unadjusted_lake.sh         # Builds unadjusted parquet lakes from flat files
│   ├── download_refdata.sh              # Pulls splits/dividends refdata from Polygon
│   └── build_adjusted_lake.sh           # Builds adjusted lakes from parquet lakes
├── legacy_scripts/
│   └── polygon_lake_loader.py           # (Legacy) lake loader used for data exploration
├── src/
│   └── polygon_ingest/
│       ├── __init__.py
│       ├── factor_builder.py            # Core logic to create adjusted lakes
│       ├── lake_io.py                   # Utilities to load parquet lakes
│       └── refdata.py                   # Utilities to download and parse corporate actions
```

---

## ✅ Pipeline Overview

### 1. 📥 Download Flat Files from Polygon.io

Get daily or minute-level `.csv.gz` flat files from Polygon's bulk data archive.

Organize them by ticker under:
```
~/data/polygonio_data/flat_files/{day,minute}/{TICKER}/YYYY-MM-DD.csv.gz
```

---

### 2. 🧾 Prepare Ticker List

Use a curated ticker list to control which tickers are ingested.

Example: `data/ticker_lists/spx_ndx_combined.json`

---

### 3. 📦 Create Unadjusted Parquet Lakes

Use the script:
```bash
bash scripts/build_unadjusted_lake.sh -t day -c spx_ndx_combined
```

This step:
- Reads flat `.csv.gz` files
- Outputs partitioned parquet files at:
  ```
  lake/day/{TICKER}/{YYYY}/{MM}.parquet
  ```

---

### 4. 🏛 Download Refdata (Splits, Dividends)

Use the script:
```bash
bash scripts/download_refdata.sh -c spx_ndx_combined
```

This pulls:
- `splits.csv`
- `dividends.csv`

And saves to:  
```
refdata/spx_ndx_combined/{TICKER}/
```

---

### 5. 🔄 Create Adjusted Parquet Lakes

Use the script:
```bash
bash scripts/build_adjusted_lake.sh -t day -c spx_ndx_combined -m ohlc
```

Options:
- `-m minimal`: only `close_tr` (total return adjusted)
- `-m close`  : adds `close_sa` (split-adjusted close)
- `-m ohlc`   : adds `*_sa` columns for open/high/low/close/volume

Output goes to:
```
lake_adj/day/{TICKER}/{YYYY}/{MM}.parquet
```

---

## 🧪 Validation & Visualization

Notebook: [`03_load_data_inspect_adjustment.ipynb`](notebooks/03_load_data_inspect_adjustment.ipynb)

This notebook:
- Loads both unadjusted and adjusted data
- Overlays corporate events
- Visually checks for proper application of split/dividend adjustments

---

## 🛠️ Requirements

Install dependencies using `conda` or `pip`.

Example `conda` environment:
```bash
conda create -n poly_ingest python=3.12 pandas pyarrow requests tqdm
conda activate poly_ingest
```

---

## 📌 Notes

- All lake creation scripts require a ticker list
- Flat files and refdata are expected to be downloaded manually or via script
- Adjustment is done via `src/polygon_ingest/factor_builder.py` using both splits and dividends

---
