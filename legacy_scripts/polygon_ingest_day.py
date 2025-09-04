#!/usr/bin/env python
import argparse
from pathlib import Path
from polygon_ingest.ingest import run_ingest

p = argparse.ArgumentParser()
p.add_argument("--src", required=True, type=Path)
p.add_argument("--out", required=True, type=Path)
p.add_argument("--watch", type=Path)
p.add_argument("--only", type=str)
p.add_argument("--workers", type=int, default=40)
p.add_argument("--chunk", type=int, default=5_000_000)
p.add_argument("--log-file", type=Path)
p.add_argument("--quiet-console", action="store_true")
p.add_argument("--write-manifest", action="store_true")
p.add_argument("--manifest-out", type=Path)
p.add_argument("--manifest-workers", type=int, default=8)
a = p.parse_args()

run_ingest(
    tf="day",
    src_root=a.src, out_root=a.out,
    watch=a.watch, only=a.only,
    workers=a.workers, chunk=a.chunk,
    log_file=a.log_file, quiet_console=a.quiet_console,
    write_manifest=a.write_manifest,
    manifest_out=a.manifest_out,
    manifest_workers=a.manifest_workers,
)
