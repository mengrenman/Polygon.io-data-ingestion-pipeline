# src/polygon_ingest/cli.py
import typer
from pathlib import Path
from .ingest import run_ingest
from .corp_actions import fetch_splits, fetch_dividends

app = typer.Typer(help="Polygon.io CSV.GZ → Parquet lake (Route B: direct functions)")

@app.command()
def bars(
    tf: str = typer.Option(..., help="minute | day"),
    src: Path = typer.Option(..., help="Root folder with .csv.gz (supports nested YYYY/MM)"),
    out: Path = typer.Option(..., help="Destination parquet lake"),
    watch: Path | None = typer.Option(None, help="Ticker list (json/txt), case-insensitive"),
    only: str | None = typer.Option(None, help="Single ticker, e.g., NVDA"),
    workers: int = typer.Option(40, help="Process workers"),
    chunk: int = typer.Option(5_000_000, help="read_csv chunksize"),
    log_file: Path | None = typer.Option(None, help="Log file"),
    quiet_console: bool = typer.Option(False, help="Quiet console (keep progress bar)"),
    write_manifest: bool = typer.Option(False, help="Write manifest JSON after ingest"),
    manifest_out: Path | None = typer.Option(None, help="Manifest path (default: <out>/manifest_<tf>.json)"),
    manifest_workers: int = typer.Option(8, help="Threads for manifest scan"),
):
    run_ingest(
        tf=tf, src_root=src, out_root=out, watch=watch, only=only,
        workers=workers, chunk=chunk, log_file=log_file, quiet_console=quiet_console,
        write_manifest=write_manifest, manifest_out=manifest_out, manifest_workers=manifest_workers,
    )

@app.command()
def actions(
    ticker: str = typer.Option(..., help="Ticker symbol, e.g., AAPL"),
    outdir: Path = typer.Option(Path("data/ref_data"), help="Output dir for parquet"),
):
    outdir.mkdir(parents=True, exist_ok=True)
    fetch_splits(ticker).to_parquet(outdir / f"{ticker}_splits.parquet", index=False)
    fetch_dividends(ticker).to_parquet(outdir / f"{ticker}_dividends.parquet", index=False)
    typer.echo(f"Saved splits & dividends for {ticker} → {outdir}")

if __name__ == "__main__":
    app()
