#!/usr/bin/env python3
"""
Benchmark Parquet string encoding performance using pylibcudf.

This script compares the performance of different Parquet string encodings
(e.g., PLAIN, DELTA_LENGTH_BYTE_ARRAY, DICTIONARY) across three operations:
1. Reading data from disk into GPU memory
2. Reading + filtering to a specific value
3. Reading + performing a "contains" substring search

Usage:
    python benchmark_string_encodings.py input.parquet --filter-value "example" --contains-pattern "abc"
"""

import argparse
import json
import statistics
import sys
import time
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from rmm.pylibrmm.stream import DEFAULT_STREAM
import pylibcudf as plc


def get_column_sizes(filepath: Path) -> dict[str, int]:
    """Get the compressed size on disk for each column in a parquet file."""
    pf = pq.ParquetFile(filepath)
    metadata = pf.metadata
    
    column_sizes: dict[str, int] = {}
    
    for rg_idx in range(metadata.num_row_groups):
        rg = metadata.row_group(rg_idx)
        for col_idx in range(rg.num_columns):
            col = rg.column(col_idx)
            col_name = col.path_in_schema
            if col_name not in column_sizes:
                column_sizes[col_name] = 0
            column_sizes[col_name] += col.total_compressed_size
    
    return column_sizes


def format_bytes(size: int) -> str:
    """Format bytes in human-readable format."""
    for unit in ["B", "KB", "MB", "GB"]:
        if abs(size) < 1024:
            return f"{size:,.1f} {unit}"
        size /= 1024
    return f"{size:,.1f} TB"


def get_column_names(filepath: Path) -> list[str]:
    """Get column names from a parquet file."""
    # Read just metadata to get column names
    source = plc.io.SourceInfo([filepath])
    reader_options = plc.io.parquet.ParquetReaderOptions.builder(source).build()
    # Read just 1 row to get schema
    reader_options.set_num_rows(1)
    table_with_meta = plc.io.parquet.read_parquet(reader_options)
    return table_with_meta.column_names(include_children=False)


def read_column(filepath: Path, column_name: str) -> plc.Column:
    """Read a single column from a parquet file."""
    source = plc.io.SourceInfo([filepath])
    reader_options = plc.io.parquet.ParquetReaderOptions.builder(source).build()
    reader_options.set_columns([column_name])
    table_with_meta = plc.io.parquet.read_parquet(reader_options)
    return table_with_meta.tbl.columns()[0]


def warmup_gpu(filepath: Path, column_name: str) -> None:
    """Perform a warmup read to initialize GPU context."""
    _ = read_column(filepath, column_name)
    # Sync to ensure completion
    DEFAULT_STREAM.synchronize()
    


def benchmark_read(
    filepath: Path, column_name: str, iterations: int
) -> list[float]:
    """Benchmark reading a column from parquet into GPU memory."""
    times = []
    for _ in range(iterations):
        start = time.perf_counter()
        col = read_column(filepath, column_name)
        DEFAULT_STREAM.synchronize()
        end = time.perf_counter()
        times.append(end - start)
        del col
    return times


def benchmark_filter(
    filepath: Path, column_name: str, filter_value: str, iterations: int
) -> list[float]:
    """Benchmark reading + filtering to rows matching a specific value."""
    times = []
    filter_scalar = plc.Scalar.from_arrow(pa.scalar(filter_value))

    for _ in range(iterations):
        start = time.perf_counter()

        # Read the column
        col = read_column(filepath, column_name)

        # Create equality mask: col == filter_value
        mask = plc.binaryop.binary_operation(
            col,
            filter_scalar,
            plc.binaryop.BinaryOperator.EQUAL,
            plc.DataType(plc.TypeId.BOOL8),
        )

        # Apply the boolean mask to filter
        table = plc.Table([col])
        filtered_table = plc.stream_compaction.apply_boolean_mask(table, mask)

        DEFAULT_STREAM.synchronize()
        end = time.perf_counter()
        times.append(end - start)
        del col, mask, table, filtered_table

    return times


def benchmark_contains(
    filepath: Path, column_name: str, pattern: str, iterations: int
) -> list[float]:
    """Benchmark reading + performing a contains substring search."""
    times = []
    pattern_scalar = plc.Scalar.from_arrow(pa.scalar(pattern))

    for _ in range(iterations):
        start = time.perf_counter()

        # Read the column
        col = read_column(filepath, column_name)

        # Perform contains operation
        contains_result = plc.strings.find.contains(col, pattern_scalar)

        DEFAULT_STREAM.synchronize()
        end = time.perf_counter()
        times.append(end - start)
        del col, contains_result

    return times


def compute_stats(times: list[float]) -> dict[str, float]:
    """Compute statistics from timing results."""
    return {
        "min": min(times),
        "max": max(times),
        "mean": statistics.mean(times),
        "median": statistics.median(times),
        "stdev": statistics.stdev(times) if len(times) > 1 else 0.0,
    }


def format_time(seconds: float) -> str:
    """Format time in human-readable format."""
    if seconds < 0.001:
        return f"{seconds * 1_000_000:.2f} µs"
    elif seconds < 1:
        return f"{seconds * 1_000:.2f} ms"
    else:
        return f"{seconds:.3f} s"


def run_benchmarks(
    filepath: Path,
    filter_value: str,
    contains_pattern: str,
    iterations: int,
    warmup_iterations: int,
) -> dict[str, dict[str, Any]]:
    """Run all benchmarks for all columns in the parquet file."""
    column_names = get_column_names(filepath)
    column_sizes = get_column_sizes(filepath)
    print(f"Found {len(column_names)} columns: {column_names}")
    print(f"Running {iterations} iterations per benchmark (warmup: {warmup_iterations})")
    print()

    results: dict[str, dict[str, Any]] = {}

    for col_name in column_names:
        print(f"Benchmarking column: {col_name}")

        # Warmup
        for _ in range(warmup_iterations):
            warmup_gpu(filepath, col_name)

        # Read benchmark
        read_times = benchmark_read(filepath, col_name, iterations)

        # Filter benchmark
        filter_times = benchmark_filter(filepath, col_name, filter_value, iterations)

        # Contains benchmark
        contains_times = benchmark_contains(
            filepath, col_name, contains_pattern, iterations
        )

        compressed_size = column_sizes.get(col_name, 0)
        results[col_name] = {
            "compressed_size": compressed_size,
            "read": {"times": read_times, "stats": compute_stats(read_times)},
            "filter": {"times": filter_times, "stats": compute_stats(filter_times)},
            "contains": {
                "times": contains_times,
                "stats": compute_stats(contains_times),
            },
        }

        # Print progress
        print(f"  size:     {format_bytes(compressed_size)}")
        print(f"  read:     {format_time(results[col_name]['read']['stats']['median'])}")
        print(f"  filter:   {format_time(results[col_name]['filter']['stats']['median'])}")
        print(f"  contains: {format_time(results[col_name]['contains']['stats']['median'])}")
        print()

    return results


def print_results_table(results: dict[str, dict[str, Any]]) -> None:
    """Print results as a formatted table."""
    # Header
    col_width = max(len(name) for name in results.keys()) + 2
    print("\n" + "=" * 100)
    print("BENCHMARK RESULTS (median times)")
    print("=" * 100)

    header = f"{'Encoding':<{col_width}} {'Size':>12} {'Read':>12} {'Filter':>12} {'Contains':>12}"
    print(header)
    print("-" * 100)

    for col_name, col_results in results.items():
        size = format_bytes(col_results["compressed_size"])
        read_time = format_time(col_results["read"]["stats"]["median"])
        filter_time = format_time(col_results["filter"]["stats"]["median"])
        contains_time = format_time(col_results["contains"]["stats"]["median"])
        print(f"{col_name:<{col_width}} {size:>12} {read_time:>12} {filter_time:>12} {contains_time:>12}")

    print("=" * 100)

    # Also print relative performance (compared to first column)
    if len(results) > 1:
        print("\nRELATIVE PERFORMANCE (compared to first column)")
        print("-" * 100)

        first_col = list(results.keys())[0]
        base_size = results[first_col]["compressed_size"]
        base_read = results[first_col]["read"]["stats"]["median"]
        base_filter = results[first_col]["filter"]["stats"]["median"]
        base_contains = results[first_col]["contains"]["stats"]["median"]

        print(f"{'Encoding':<{col_width}} {'Size':>12} {'Read':>12} {'Filter':>12} {'Contains':>12}")
        print("-" * 100)

        for col_name, col_results in results.items():
            size_ratio = col_results["compressed_size"] / base_size if base_size else 1.0
            read_ratio = col_results["read"]["stats"]["median"] / base_read
            filter_ratio = col_results["filter"]["stats"]["median"] / base_filter
            contains_ratio = col_results["contains"]["stats"]["median"] / base_contains
            print(
                f"{col_name:<{col_width}} {size_ratio:>11.2f}x {read_ratio:>11.2f}x {filter_ratio:>11.2f}x {contains_ratio:>11.2f}x"
            )

        print("=" * 100)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Benchmark Parquet string encoding performance using pylibcudf"
    )
    parser.add_argument(
        "input_file",
        type=Path,
        help="Path to the Parquet file with multiple encoding columns",
    )
    parser.add_argument(
        "--filter-value",
        type=str,
        default="test",
        help="Value to filter by in the filter benchmark (default: 'test')",
    )
    parser.add_argument(
        "--contains-pattern",
        type=str,
        default="a",
        help="Substring pattern for the contains benchmark (default: 'a')",
    )
    parser.add_argument(
        "-n",
        "--iterations",
        type=int,
        default=10,
        help="Number of benchmark iterations (default: 10)",
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=3,
        help="Number of warmup iterations (default: 3)",
    )
    parser.add_argument(
        "-o",
        "--output-json",
        type=Path,
        default=None,
        help="Optional path to write JSON results",
    )

    args = parser.parse_args()

    if not args.input_file.exists():
        print(f"Error: Input file {args.input_file} does not exist", file=sys.stderr)
        return 1

    print(f"Benchmarking: {args.input_file}")
    print(f"Filter value: '{args.filter_value}'")
    print(f"Contains pattern: '{args.contains_pattern}'")
    print()

    results = run_benchmarks(
        args.input_file,
        args.filter_value,
        args.contains_pattern,
        args.iterations,
        args.warmup,
    )

    print_results_table(results)

    if args.output_json:
        # Remove raw times for cleaner JSON output
        json_results = {}
        for col, col_data in results.items():
            json_results[col] = {
                "compressed_size": col_data["compressed_size"],
            }
            for op, data in col_data.items():
                if op != "compressed_size":
                    json_results[col][op] = data["stats"]
        with open(args.output_json, "w") as f:
            json.dump(json_results, f, indent=2)
        print(f"\nJSON results written to: {args.output_json}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
