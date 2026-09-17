#!/usr/bin/env python3
"""Measures what sorting a TPC-H table after generating it costs.

The GPU post-process in tpchgen-rs#385 reads the source table once per output
partition, filtered to that partition's date range. Row group statistics cannot
prune those reads, because an unsorted table scatters every date across every
row group. This script measures the pieces of that cost on a dataset that is
already on disk:

* how much of the source each partition's read has to touch
* how long one full pass over the source takes
* how long sorting the table takes on the CPU

and reports the resulting lower bound: partitions x full pass.

Usage:
    post_process_cost.py SOURCE_TABLE_DIR [--partitions N] [--key COLUMN]
"""

import argparse
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


def row_group_spans(files, key):
    """Returns the date span of every row group, in days."""
    spans = []
    domain_min = None
    domain_max = None
    for path in files:
        metadata = pq.ParquetFile(path).metadata
        index = pq.ParquetFile(path).schema_arrow.names.index(key)
        for group in range(metadata.num_row_groups):
            statistics = metadata.row_group(group).column(index).statistics
            spans.append((statistics.min, statistics.max))
            domain_min = min(domain_min or statistics.min, statistics.min)
            domain_max = max(domain_max or statistics.max, statistics.max)
    return spans, domain_min, domain_max


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("--partitions", type=int, default=8)
    parser.add_argument("--key", default="l_shipdate")
    args = parser.parse_args()

    files = sorted(args.source.glob("*.parquet"))
    if not files:
        raise SystemExit(f"no parquet files in {args.source}")
    size = sum(path.stat().st_size for path in files) / 1024**3

    spans, domain_min, domain_max = row_group_spans(files, args.key)
    domain_days = (domain_max - domain_min).days + 1
    partition_days = domain_days / args.partitions

    # A read for one partition must touch every row group whose span overlaps
    # that partition's date range.
    covering = sum(1 for low, high in spans if (high - low).days + 1 >= partition_days)
    print(f"source: {len(files)} files, {size:.2f} GiB, {len(spans)} row groups")
    print(f"{args.key} domain: {domain_min} .. {domain_max} ({domain_days} days)")
    print(
        f"row groups spanning a whole {partition_days:.0f}-day partition: "
        f"{covering}/{len(spans)} ({100 * covering / len(spans):.1f}%)"
    )

    start = time.perf_counter()
    table = pq.read_table(args.source)
    read = time.perf_counter() - start
    print(f"one full pass over the source: {read:.2f}s ({size / read:.2f} GiB/s)")

    # string_view has no take kernel in pyarrow 21, so widen those columns
    fields = [
        field.with_type(pa.string()) if field.type == pa.string_view() else field
        for field in table.schema
    ]
    table = table.cast(pa.schema(fields))

    start = time.perf_counter()
    indices = pc.sort_indices(
        table,
        sort_keys=[(args.key, "ascending"), (table.column_names[0], "ascending")],
    )
    table.take(indices)
    sort = time.perf_counter() - start
    print(f"one CPU sort of {table.num_rows} rows: {sort:.2f}s")

    print()
    print(
        f"post-process lower bound for {args.partitions} partitions: "
        f"{args.partitions} x {read:.2f}s = {args.partitions * read:.2f}s of reads alone, "
        f"before sorting or writing"
    )


if __name__ == "__main__":
    main()
