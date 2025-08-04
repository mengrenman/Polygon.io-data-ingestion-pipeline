# Polygon.io Data Ingestion Pipeline

Efficient ingestion pipeline for historical Polygon.io market data, including CSV parsing, Parquet conversion, and optional data lake storage.

This repository contains a high-performance, modular pipeline for ingesting historical market data from [Polygon.io](https://polygon.io/). It is designed to process flat file dumps (e.g., `2023-06-05.csv.gz`), convert them into analysis-friendly formats like Parquet, and store them in a structured data lake.

## 📦 Features

- 🗃️ Batch ingestion of compressed CSV files
- 🧱 Conversion to columnar Parquet format
- 🧪 Schema enforcement and file validation
- ⚡ Optional multi-threaded processing
- 🪣 Compatible with local and cloud (S3) storage
- 🧩 Modular design for extensibility (e.g., DuckDB, Snowflake, BigQuery)
- 🧭 For use in quantitative research and trading systems

## 📂 Example Directory Structure

polygonio-data-ingestion/
├── data/                          # Input & Output Data Folder
│   ├── raw/                       # Original compressed CSVs from Polygon.io
│   │   ├── trades/
│   │   └── quotes/
│   └── parquet/                   # Transformed Parquet output
│       ├── trades/
│       └── quotes/
├── ingest/                        # Core ingestion logic
│   ├── __init__.py
│   ├── loader.py                  # Load raw files
│   ├── parser.py                  # Parse and clean data
│   ├── writer.py                  # Write to Parquet
│   └── pipeline.py                # End-to-end pipeline coordination
├── config/
│   └── config.yaml                # Source paths, schema, etc.
├── scripts/                       # CLI or automation scripts
│   ├── run_ingestion.py
│   └── cron_wrapper.sh            # (Optional) for cron jobs
├── notebooks/                     # Optional notebooks for demos
│   └── visualize_sample.ipynb
├── tests/                         # Unit tests
│   ├── test_loader.py
│   ├── test_parser.py
│   └── test_writer.py
├── .gitignore
├── LICENSE
├── README.md
├── requirements.txt
└── setup.py                       # (Optional) to make this installable as a package

## 🚀 Quick Start

```bash
# Clone the repository
git clone https://github.com/mengren1942/polygonio-data-ingestion.git
cd polygonio-data-ingestion

# (Optional) Create environment
conda create -n polygon-ingest python=3.11
conda activate polygon-ingest
pip install -r requirements.txt

# Run the ingestion script
# Usage (Linux/macOS):
# (optional) raise FD limit
ulimit -n 16384

python polygon_ingest_monthslice.py \
  --src minute_aggs_v1 \
  --out parquet_lake \
  --workers 16 \
  --chunk 5000000 \
  --watch ticker_lists/nasdaq100.json
