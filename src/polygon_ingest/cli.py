# src/polygon_ingest/cli.py
import typer
from pathlib import Path
from .ingest import run_ingest

app = typer.Typer(help="Polygon.io CSV.GZ → Parquet lake. Reference data (splits/dividends/tickers) is pulled by scripts/pull_ref_data.sh.")

@app.callback()
def _root() -> None:
    """Polygon.io flat files → Parquet lake. `poly bars` ingests; reference data is pulled by scripts/pull_ref_data.sh."""
    # A Typer app with exactly one command becomes that command itself (`poly --tf ...`); this callback keeps
    # `poly bars ...` as the documented invocation now that `poly actions` is gone.


@app.command()
def bars(
    tf: str = typer.Option(..., help="minute | day"),
    src: Path = typer.Option(..., help="Root folder with .csv.gz (supports nested YYYY/MM)"),
    out: Path = typer.Option(..., help="Destination parquet lake"),
    watch: Path | None = typer.Option(None, help="Ticker list (json/txt), matched exactly: Polygon spells share class in letter case, so AAP selects Advance Auto Parts and not the AAp preferred"),
    only: str | None = typer.Option(None, help="Single ticker, matched exactly, e.g., NVDA"),
    workers: int = typer.Option(40, help="Process workers"),
    chunk: int = typer.Option(5_000_000, help="read_csv chunksize"),
    log_file: Path | None = typer.Option(None, help="Log file"),
    quiet_console: bool = typer.Option(False, help="Quiet console (keep progress bar)"),
    write_manifest: bool = typer.Option(False, help="Write manifest JSON after ingest"),
    manifest_out: Path | None = typer.Option(None, help="Manifest path (default: <out>/manifest_<tf>.json)"),
    manifest_workers: int = typer.Option(8, help="Threads for manifest scan"),
    # (no square brackets in help strings: Typer renders help with rich markup, which reads "[/<DD>]" as a closing tag)
    ignore_case: bool = typer.Option(False, help="Match --watch/--only on letters alone, so AAP also selects AAp, AAPw, ... (different securities; not safe for the ticker layout on a case-folding filesystem)"),
    layout: str = typer.Option("ticker", help="ticker: <out>/<TICKER>/<YYYY>/<MM>/<DD>.parquet (day: <MM>.parquet) | market: <out>/<YYYY>/<MM>/<DD>.parquet with all tickers (whole universe)"),
    replace_month: bool = typer.Option(False, help="Rewrite each day month file from --src even where --src lacks sessions the file already holds (they are dropped), and let rows a flat file stamps into a neighboring month or session replace that file. Without it such a run is refused before anything is written."),
    replace_with_subset: bool = typer.Option(False, help="With --layout market and --watch/--only, rewrite existing period files with the selected tickers alone (every other ticker in them is dropped). Without it a run that would rewrite a file holding tickers outside the selection is refused before anything is written."),
):
    if layout not in ("ticker", "market"):
        raise typer.BadParameter("--layout must be 'ticker' or 'market'")
    run_ingest(
        tf=tf, src_root=src, out_root=out, watch=watch, only=only,
        workers=workers, chunk=chunk, log_file=log_file, quiet_console=quiet_console,
        write_manifest=write_manifest, manifest_out=manifest_out, manifest_workers=manifest_workers,
        layout=layout, ignore_case=ignore_case, replace_month=replace_month,
        replace_with_subset=replace_with_subset,
    )

if __name__ == "__main__":
    app()
