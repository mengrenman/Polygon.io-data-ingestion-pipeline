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
├── data/                            # 🔽 Input & Output Data Folder
│   ├── raw/                         # Original compressed CSVs from Polygon.io
│   │   ├── trades/
│   │   │   ├── 2023-06-01.csv.gz
│   │   │   └── 2023-06-02.csv.gz
│   │   └── quotes/
│   │       ├── 2023-06-01.csv.gz
│   │       └── 2023-06-02.csv.gz
│   └── parquet/                     # Transformed Parquet output
│       ├── trades/
│       │   └── 2023/06/2023-06-01.parquet
│       └── quotes/
│           └── 2023/06/2023-06-01.parquet

├── ingest/                          # 🛠 Core Ingestion Logic
│   ├── __init__.py
│   ├── loader.py                    # Load raw files
│   ├── parser.py                    # Parse and clean data
│   ├── writer.py                    # Write to Parquet
│   └── pipeline.py                  # End-to-end ingestion flow

├── config/                          # ⚙ Config files
│   └── config.yaml                  # Source paths, output paths, schema, etc.

├── scripts/                         # 📜 CLI or helper scripts
│   ├── run_ingestion.py             # Example entrypoint for ingestion
│   └── cron_wrapper.sh              # (Optional) For scheduled execution

├── notebooks/                       # 📓 Optional analysis or demo notebooks
│   └── visualize_sample.ipynb

├── tests/                           # ✅ Unit tests
│   ├── test_loader.py
│   ├── test_parser.py
│   └── test_writer.py

├── .gitignore                       # Ignore data, logs, etc.
├── LICENSE
├── README.md
├── requirements.txt
└── setup.py                         # (Optional) To make package installable

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
