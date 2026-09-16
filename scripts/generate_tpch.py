#!/usr/bin/env python3
"""
Generate TPC-H datasets with specific partition configurations.

Configuration for partition counts based on scale factor.
These partition counts give us roughly 100,000,000 rows per file.

The total number of rows is determined by the scale factor and the table multipliers.
Table sizes (from Fig. 2 in https://www.tpc.org/TPC_Documents_Current_Versions/pdf/TPC-H_v3.0.1.pdf):
Table      Multiplier
part       200000
partsupp   800000
supplier   10000
customer   150000
lineitem   6000000
orders     1500000

With the remaining tables (nation, region) being constant.
The number of partitions equals:
  max(1, ceil(SF * multiplier / 100_000_000))
"""

import argparse
import json
import time

import math
import os
import shutil
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# Table multipliers (rows per scale factor)
TABLE_MULTIPLIERS = {
    "customer": 150000,
    "lineitem": 6000000,
    "nation": 0,  # constant size, always 1 partition
    "orders": 1500000,
    "part": 200000,
    "partsupp": 800000,
    "region": 0,  # constant size, always 1 partition
    "supplier": 10000,
}

# Target rows per partition file
TARGET_ROWS_PER_FILE = 100_000_000

# Per-table parquet row group byte defaults
# These give approximately 1,000,000 rows per row group (maximum).
PARQUET_ROW_GROUP_BYTES_DEFAULTS = {
    "customer": 165000000,
    "lineitem": 68700000,
    "nation": 5000,
    "orders": 99000000,
    "part": 69000000,
    "partsupp": 147000000,
    "region": 5000,
    "supplier": 154000000,
}

# Default: disable compression for certain columns to match cudf-polars defaults at sf3k
UNCOMPRESSED_COLUMN_OVERRIDES = (
    "c_mktsegment,c_nationkey,l_commitdate,l_discount,l_quantity,l_receiptdate,"
    "l_shipdate,l_shipinstruct,l_shipmode,l_tax,n_nationkey,n_regionkey,o_orderdate,"
    "o_orderpriority,o_shippriority,p_brand,p_container,p_mfgr,p_size,p_type,"
    "r_regionkey,s_nationkey"
)

DELTA_BINARY_PACKED = "DELTA_BINARY_PACKED"
DELTA_LENGTH_BYTE_ARRAY = "DELTA_LENGTH_BYTE_ARRAY"
PLAIN = "PLAIN"
RLE_DICTIONARY = "RLE_DICTIONARY"
DEFAULT_COLUMN_ENCODINGS = {
  "l_comment": DELTA_LENGTH_BYTE_ARRAY,
  "ps_comment": DELTA_LENGTH_BYTE_ARRAY,
  "l_extendedprice": DELTA_BINARY_PACKED,
  "l_partkey": DELTA_BINARY_PACKED,
  "o_comment": DELTA_LENGTH_BYTE_ARRAY,
  "l_orderkey": DELTA_BINARY_PACKED,
  "o_orderkey": DELTA_BINARY_PACKED,
  "o_totalprice": DELTA_BINARY_PACKED,
  "o_custkey": DELTA_BINARY_PACKED,
  "ps_supplycost": PLAIN,
  "c_comment": DELTA_LENGTH_BYTE_ARRAY,
  "ps_partkey": DELTA_BINARY_PACKED,
  "l_suppkey": DELTA_BINARY_PACKED,
  "p_name": DELTA_LENGTH_BYTE_ARRAY,
  "p_partkey": DELTA_BINARY_PACKED,
  "c_custkey": DELTA_BINARY_PACKED,
  "c_address": DELTA_LENGTH_BYTE_ARRAY,
  "c_acctbal": DELTA_BINARY_PACKED,
  "ps_availqty": PLAIN,
  "ps_suppkey": DELTA_BINARY_PACKED,
  "p_comment": DELTA_LENGTH_BYTE_ARRAY,
  "l_receiptdate": PLAIN,
  "l_shipdate": PLAIN,
  "l_commitdate": PLAIN,
  "c_name": DELTA_LENGTH_BYTE_ARRAY,
  "c_phone": DELTA_LENGTH_BYTE_ARRAY,
  "l_linestatus": PLAIN,
  "o_orderdate": PLAIN,
  "s_suppkey": DELTA_BINARY_PACKED,
  "l_returnflag": PLAIN,
  "o_clerk": PLAIN,
  "s_address": DELTA_LENGTH_BYTE_ARRAY,
  "s_acctbal": DELTA_BINARY_PACKED,
  "s_comment": DELTA_LENGTH_BYTE_ARRAY,
  "s_name": DELTA_LENGTH_BYTE_ARRAY,
  "s_phone": DELTA_LENGTH_BYTE_ARRAY,
  "l_quantity": PLAIN,
  "l_shipinstruct": PLAIN,
  "l_shipmode": PLAIN,
  "l_discount": PLAIN,
  "l_tax": PLAIN,
  "o_orderstatus": PLAIN,
  "o_orderpriority": PLAIN,
  "o_shippriority": PLAIN,
  "c_nationkey": PLAIN,
  "c_mktsegment": PLAIN,
  "p_size": PLAIN,
  "n_nationkey": DELTA_BINARY_PACKED,
  "n_comment": DELTA_LENGTH_BYTE_ARRAY,
  "p_container": PLAIN,
  "n_name": DELTA_LENGTH_BYTE_ARRAY,
  "r_regionkey": DELTA_BINARY_PACKED,
  "r_comment": DELTA_LENGTH_BYTE_ARRAY,
  "r_name": DELTA_LENGTH_BYTE_ARRAY,
  "n_regionkey": PLAIN,
  "p_mfgr": PLAIN,
  "s_nationkey": PLAIN,
  "p_brand": PLAIN,
  "p_type": PLAIN,
  "p_retailprice": PLAIN,
  "l_linenumber": PLAIN,
}

def column_belongs_to_table(column: str, table: str) -> bool:
    """Return True if a TPC-H column name belongs to the given table."""
    if table == "partsupp":
        return column.startswith("ps_")
    if table == "part":
        return column.startswith("p_") and not column.startswith("ps_")

    prefixes = {
        "customer": "c_",
        "lineitem": "l_",
        "nation": "n_",
        "orders": "o_",
        "region": "r_",
        "supplier": "s_",
    }
    return column.startswith(prefixes[table])


DEFAULT_DISABLE_DICTIONARY_ENCODING_COLUMNS = [
  "l_comment",
  "ps_comment",
  "l_extendedprice",
  "l_partkey",
  "o_comment",
  "l_orderkey",
  "o_orderkey",
  "o_totalprice",
  "c_comment",
  "ps_partkey",
  "l_suppkey",
  "p_name",
  "p_partkey",
  "c_custkey",
  "c_address",
  "c_acctbal",
  "p_comment",
  "c_name",
  "c_phone",
  "s_suppkey",
  "s_address",
  "s_acctbal",
  "s_comment",
  "s_name",
  "s_phone",
  "n_nationkey",
  "n_comment",
  "n_name",
  "r_regionkey",
  "r_comment",
  "r_name",
]


def calculate_partitions(scale: int, multiplier: int) -> int:
    """
    Calculate partition count for a table at a given scale.
    Uses ceiling division: ceil(a/b) = (a + b - 1) / b
    """
    if multiplier == 0:
        # Constant-size tables (nation, region) always get 1 partition
        return 1

    total_rows = scale * multiplier
    partitions = math.ceil(total_rows / TARGET_ROWS_PER_FILE)

    # Ensure at least 1 partition
    return max(1, partitions)


def generate_partition(
    table: str,
    num_parts: int,
    part: int,
    scale: int,
    format: str,
    output_base: Path,
    temp_root: Path,
    parquet_row_group_bytes_override: int | None,
    use_upstream_compression: bool,
    use_upstream_encoding: bool,
    parquet_version: str,
    decimal_column_type: str,
    date_column_type: str,
    nationkey_type: str,
    regionkey_type: str,
    use_upstream_disable_dictionary_encoding: bool,
    no_delta_length_byte_array: bool,
) -> tuple[str, int, float]:
    """
    Generate a single partition.

    Returns:
        Tuple of (table, part, elapsed_seconds)
    """
    start_time = time.time()

    # Create a temporary directory within the output directory
    temp_dir = temp_root / f"{table}-part-{part}-{os.getpid()}"
    temp_dir.mkdir(parents=True, exist_ok=True)

    # Determine parquet row group bytes: use override if set, otherwise per-table default
    if parquet_row_group_bytes_override is not None:
        row_group_bytes = parquet_row_group_bytes_override
    else:
        row_group_bytes = PARQUET_ROW_GROUP_BYTES_DEFAULTS[table]

    print(f"  Generating partition {part} of {num_parts} for {table}...")

    # Build the command with optional flags. Upstream uses format subcommands
    # (parquet/tbl/csv) rather than --format.
    cmd = [
        "tpchgen-cli",
        format,
        "-s",
        str(scale),
        "--tables",
        table,
        "--output-dir",
        str(temp_dir),
        "--parts",
        str(num_parts),
        "--part",
        str(part),
        "--num-threads",
        "1",
    ]

    if format == "parquet":
        cmd.extend(["--row-group-bytes", str(row_group_bytes)])

        # Add uncompressed column overrides unless using upstream compression.
        # Filter to columns in the current table; upstream ignores unknown columns
        # but shorter commands are easier to debug.
        if not use_upstream_compression and UNCOMPRESSED_COLUMN_OVERRIDES:
            uncompressed_columns = [
                col
                for col in UNCOMPRESSED_COLUMN_OVERRIDES.split(",")
                if column_belongs_to_table(col, table)
            ]
            if uncompressed_columns:
                cmd.append(
                    f"--uncompressed-column-overrides={','.join(uncompressed_columns)}"
                )

        # Add column encoding overrides unless using upstream encoding
        # Use DELTA_LENGTH_BYTE_ARRAY for string columns (better compression than default RLE_DICTIONARY)
        if not use_upstream_encoding:
            decimal_columns_with_delta = {
                "c_acctbal",
                "l_extendedprice",
                "o_totalprice",
                "s_acctbal",
            }

            for col, encoding in DEFAULT_COLUMN_ENCODINGS.items():
                if not column_belongs_to_table(col, table):
                    continue

                if (
                    decimal_column_type == "f64"
                    and col in decimal_columns_with_delta
                    and encoding == DELTA_BINARY_PACKED
                ):
                    encoding = PLAIN
                elif encoding == DELTA_LENGTH_BYTE_ARRAY and no_delta_length_byte_array:
                    encoding = PLAIN

                cmd.append(f"--column-encoding={col}={encoding}")

        # Add disable dictionary encoding columns if specified
        if not use_upstream_disable_dictionary_encoding:
            disable_dictionary_columns = [
                col
                for col in DEFAULT_DISABLE_DICTIONARY_ENCODING_COLUMNS
                if column_belongs_to_table(col, table)
            ]
            if disable_dictionary_columns:
                cmd.append(
                    f"--disable-dictionary-encoding={','.join(disable_dictionary_columns)}"
                )

        # Add column type flags
        cmd.extend(
            [
                "--decimal-column-type",
                decimal_column_type,
                "--date-column-type",
                date_column_type,
                "--nationkey-type",
                nationkey_type,
                "--regionkey-type",
                regionkey_type,
            ]
        )

        # Add parquet version
        cmd.extend(["--parquet-version", parquet_version])

    subprocess.run(cmd, check=True)

    # Move the generated file to the final location with the desired name
    table_dir = output_base / table
    table_dir.mkdir(parents=True, exist_ok=True)

    # The file will be named table/table.1.format in the temp directory
    src_file = temp_dir / table / f"{table}.{part}.{format}"
    dst_file = table_dir / f"part.{part - 1}.{format}"

    shutil.move(str(src_file), str(dst_file))
    shutil.rmtree(temp_dir)

    elapsed = time.time() - start_time
    print(
        f"  Finished partition {part - 1} of {num_parts} for {table} in {elapsed:.1f}s"
    )

    return (table, part, elapsed)


def build_job_list(partition_config: dict[str, int]) -> list[tuple[str, int, int]]:
    """
    Build a flat list of all (table, num_parts, part) jobs.

    Returns:
        List of (table, num_parts, part) tuples
    """
    jobs = []
    for table, num_parts in partition_config.items():
        for part in range(1, num_parts + 1):
            jobs.append((table, num_parts, part))
    return jobs


def main():
    parser = argparse.ArgumentParser(
        description="Generate TPC-H datasets with specific partition configurations",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Current defaults:
  Parquet row group bytes (per table):
    customer: {PARQUET_ROW_GROUP_BYTES_DEFAULTS["customer"]}
    lineitem: {PARQUET_ROW_GROUP_BYTES_DEFAULTS["lineitem"]}
    nation:   {PARQUET_ROW_GROUP_BYTES_DEFAULTS["nation"]}
    orders:   {PARQUET_ROW_GROUP_BYTES_DEFAULTS["orders"]}
    part:     {PARQUET_ROW_GROUP_BYTES_DEFAULTS["part"]}
    partsupp: {PARQUET_ROW_GROUP_BYTES_DEFAULTS["partsupp"]}
    region:   {PARQUET_ROW_GROUP_BYTES_DEFAULTS["region"]}
    supplier: {PARQUET_ROW_GROUP_BYTES_DEFAULTS["supplier"]}

  Column type defaults:
    Decimal columns: decimal128 (use --use-float-type for f64)
    Date columns: date32 (use --use-timestamp-type for timestamp_ms)
    Nationkey/Regionkey columns: i32 (use --use-large-ids for i64)

  Encoding defaults:
    String columns: DELTA_LENGTH_BYTE_ARRAY (use --use-upstream-encoding for PLAIN/RLE_DICTIONARY)

  Parquet defaults:
    Parquet version: v2 (use --use-upstream-parquet-version for v1)
""",
    )

    parser.add_argument(
        "-s",
        "--scale",
        type=int,
        default=1000,
        help="Scale factor (any positive integer; default: 1000)",
    )
    parser.add_argument(
        "-f",
        "--format",
        choices=["parquet", "tbl"],
        default="parquet",
        help="Output format: parquet or tbl (default: parquet)",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path("tpch-data"),
        help="Base output directory (default: tpch-data)",
    )
    parser.add_argument(
        "-j",
        "--jobs",
        type=int,
        default=None,
        help=f"Number of parallel jobs (default: number of CPU threads)",
    )
    parser.add_argument(
        "--row-group-bytes",
        "--parquet-row-group-bytes",
        type=int,
        default=None,
        dest="row_group_bytes",
        help="Override parquet row group size in bytes for all tables",
    )
    parser.add_argument(
        "--use-upstream-compression",
        action="store_true",
        help="Use upstream default compression (compress all columns)",
    )
    parser.add_argument(
        "--use-upstream-encoding",
        action="store_true",
        help="Use upstream default encoding (PLAIN/RLE_DICTIONARY for strings)",
    )
    parser.add_argument(
        "--use-upstream-parquet-version",
        action="store_true",
        help="Use upstream Parquet version (v1 instead of v2)",
    )
    parser.add_argument(
        "--use-float-type",
        action="store_true",
        help="Use f64 for decimal columns (instead of decimal128)",
    )
    parser.add_argument(
        "--use-timestamp-type",
        action="store_true",
        help="Use timestamp_ms for date columns (instead of date32)",
    )
    parser.add_argument(
        "--use-large-ids",
        action="store_true",
        help="Use i64 for nationkey/regionkey columns (instead of i32)",
    )
    parser.add_argument(
        "--use-upstream-disable-dictionary-encoding",
        action="store_true",
        help="Use upstream default disable dictionary encoding (disable dictionary encoding for all columns)",
    )
    parser.add_argument(
        "--no-delta-length-byte-array",
        action="store_true",
        help="Use PLAIN encoding (instead of DELTA_LENGTH_BYTE_ARRAY). Some engines don't support DELTA_LENGTH_BYTE_ARRAY.",
    )

    args = parser.parse_args()

    # Validate scale factor
    if args.scale < 1:
        print(f"Error: Scale factor must be a positive integer, got: {args.scale}")
        sys.exit(1)

    # Determine column types based on flags
    decimal_column_type = "f64" if args.use_float_type else "decimal128"
    date_column_type = "timestamp_ms" if args.use_timestamp_type else "date32"
    nationkey_type = "i64" if args.use_large_ids else "i32"
    regionkey_type = "i64" if args.use_large_ids else "i32"
    parquet_version = "v1" if args.use_upstream_parquet_version else "v2"

    # Calculate partition counts dynamically for each table
    partition_config = {}
    for table, multiplier in TABLE_MULTIPLIERS.items():
        partition_config[table] = calculate_partitions(args.scale, multiplier)

    # Display the calculated partition configuration
    print(f"Scale factor: {args.scale}")
    print("Partition configuration:")
    for table in [
        "customer",
        "lineitem",
        "nation",
        "orders",
        "part",
        "partsupp",
        "region",
        "supplier",
    ]:
        print(f"  {table}: {partition_config[table]} partition(s)")

    # Create the base output directory and temporary directory
    output_base = args.output
    output_base.mkdir(parents=True, exist_ok=True)
    temp_root = output_base / ".tmp"
    temp_root.mkdir(parents=True, exist_ok=True)

    # Build job list
    jobs = build_job_list(partition_config)
    total_jobs = len(jobs)
    print(
        f"Generating {total_jobs} total partitions across all tables using {args.jobs} threads..."
    )

    try:
        # Generate all partitions in parallel across all tables
        with ThreadPoolExecutor(max_workers=args.jobs) as executor:
            futures = []
            for table, num_parts, part in jobs:
                future = executor.submit(
                    generate_partition,
                    table=table,
                    num_parts=num_parts,
                    part=part,
                    scale=args.scale,
                    format=args.format,
                    output_base=output_base,
                    temp_root=temp_root,
                    parquet_row_group_bytes_override=args.row_group_bytes,
                    use_upstream_compression=args.use_upstream_compression,
                    use_upstream_encoding=args.use_upstream_encoding,
                    parquet_version=parquet_version,
                    decimal_column_type=decimal_column_type,
                    date_column_type=date_column_type,
                    nationkey_type=nationkey_type,
                    regionkey_type=regionkey_type,
                    use_upstream_disable_dictionary_encoding=args.use_upstream_disable_dictionary_encoding,
                    no_delta_length_byte_array=args.no_delta_length_byte_array,
                )
                futures.append(future)

            # Wait for all futures to complete and handle exceptions
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Error generating partition: {e}")
                    raise

    finally:
        # Cleanup temporary files
        print("Cleaning up temporary files...")
        if temp_root.exists():
            shutil.rmtree(temp_root)

    print("TPC-H data generation complete!")
    print(f"Data has been generated in: {output_base}")

    # Generate metadata.json
    script_dir = Path(__file__).parent
    inspect_script = script_dir / "inspect_tpch_parquet.py"

    print("Generating metadata.json...")

    options = {
        "scale_factor": args.scale,
        "format": args.format,
        "output_base_dir": str(output_base),
        "threads": args.jobs,
        "use_upstream_compression": args.use_upstream_compression,
        "use_upstream_encoding": args.use_upstream_encoding,
        "parquet_version": parquet_version,
        "nationkey_type": nationkey_type,
        "regionkey_type": regionkey_type,
        "decimal_column_type": decimal_column_type,
        "date_column_type": date_column_type,
        "parquet_row_group_bytes_customer": PARQUET_ROW_GROUP_BYTES_DEFAULTS[
            "customer"
        ],
        "parquet_row_group_bytes_lineitem": PARQUET_ROW_GROUP_BYTES_DEFAULTS[
            "lineitem"
        ],
        "parquet_row_group_bytes_nation": PARQUET_ROW_GROUP_BYTES_DEFAULTS["nation"],
        "parquet_row_group_bytes_orders": PARQUET_ROW_GROUP_BYTES_DEFAULTS["orders"],
        "parquet_row_group_bytes_part": PARQUET_ROW_GROUP_BYTES_DEFAULTS["part"],
        "parquet_row_group_bytes_partsupp": PARQUET_ROW_GROUP_BYTES_DEFAULTS[
            "partsupp"
        ],
        "parquet_row_group_bytes_region": PARQUET_ROW_GROUP_BYTES_DEFAULTS["region"],
        "parquet_row_group_bytes_supplier": PARQUET_ROW_GROUP_BYTES_DEFAULTS[
            "supplier"
        ],
    }

    subprocess.run(
        [
            sys.executable,
            str(inspect_script),
            str(output_base),
            "--output",
            "json",
            "--output-file",
            str(output_base / "metadata.json"),
            "--options",
            json.dumps(options),
        ],
        check=True,
    )

    print(f"Metadata written to: {output_base / 'metadata.json'}")


if __name__ == "__main__":
    main()
