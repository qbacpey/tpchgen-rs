#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "pyarrow",
# ]
# ///

import functools
import statistics
import argparse
import dataclasses
import csv
import datetime
import json
import sys
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal
import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq


# TODO: think about how to capture things like compression ratio.


def _convert_value(v: Any) -> Any:
    """Convert non-JSON-serializable values."""
    if isinstance(v, bytes):
        return v.decode("utf-8", errors="replace")
    if isinstance(v, (datetime.date, datetime.datetime)):
        return v.isoformat()
    if isinstance(v, Decimal):
        sign, digits, exponent = v.as_tuple()
        return {
            "type": "decimal",
            "value": str(v),
            "sign": sign,
            "digits": digits,
            "exponent": exponent,
        }
    return v


@dataclasses.dataclass
class ColumnStats:
    min: Any  # Type depends on column type
    max: Any  # Type depends on column type
    null_count: int
    num_values: int

    def serialize(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict."""
        result = dataclasses.asdict(self)
        result["min"] = _convert_value(result["min"])
        result["max"] = _convert_value(result["max"])
        return result


@dataclasses.dataclass
class ColumnInfo:
    """
    Metadata for a single column in a parquet table.

    Parameters
    ----------
    name
        The name of the column.
    physical_type
        The physical type of the column.
    is_stats_set
        Whether the stats are set for the column.
    stats
        The stats for the column.

        These statistics are aggregated over all the row groups
        and partitions in the table. The stats are aggregated
        as you'd expect:

        - minimum is the minimum value of the column over all row groups.
        - maximum is the maximum value of the column over all row groups.
        - null_count is the sum of the null counts over all row groups.
        - num_values is the sum of the number of values over all row groups.

    encodings
        The encodings used for the column.
    compression
        The compression used for the column.
    total_compressed_size
        The total size of the compressed column data in bytes.
    total_uncompressed_size
        The total size of the uncompressed column data in bytes.

    """

    name: str
    physical_type: str
    is_stats_set: bool
    stats: ColumnStats | None  # Aggregated over row groups
    encodings: list[str]
    compression: str
    total_compressed_size: int  # sum over all the rgs
    total_uncompressed_size: int  # sum over all the rgs

    def serialize(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict."""
        result = dataclasses.asdict(self)
        result["stats"] = self.stats.serialize() if self.stats else None
        return result


@dataclasses.dataclass
class TableInfo:
    """
    Summary statistics for a parquet table.

    Parameters
    ----------
    partition_count
        The number of partitions (files) in the table.
    row_group_count
        The total number of row groups in the table.
    row_count
        The total number of rows in the table.
    total_bytes
        The total size of the table in bytes.
    avg_rows_per_partition
        The average number of rows per partition.
    min_rows_per_partition
        The fewest rows in any partition.
    max_rows_per_partition
        The most rows in any partition.
    avg_bytes_per_partition
        The average size of a partition in bytes.
    min_bytes_per_partition
        The smallest partition in bytes.
    max_bytes_per_partition
        The largest partition in bytes.
    avg_rows_per_row_group
        The average number of rows per row group.
    min_rows_per_row_group
        The fewest rows in any row group.
    max_rows_per_row_group
        The most rows in any row group.
    avg_bytes_per_row_group
        The average size of a row group in bytes.
    min_bytes_per_row_group
        The smallest row group in bytes.
    max_bytes_per_row_group
        The largest row group in bytes.
    parquet_format_version
        The version of the parquet format used.
    created_by
        The software that created the parquet file.
    columns
        A dictionary mapping column names to ColumnInfo.
    """

    table_schema: pa.Schema
    # global table stats
    partition_count: int
    row_group_count: int
    row_count: int
    total_bytes: int
    parquet_format_version: Literal[1, 2]
    created_by: str

    # partitions summary stats
    avg_rows_per_partition: int
    min_rows_per_partition: int
    max_rows_per_partition: int

    avg_bytes_per_partition: int
    min_bytes_per_partition: int
    max_bytes_per_partition: int

    # row groups summary stats
    avg_rows_per_row_group: int
    min_rows_per_row_group: int
    max_rows_per_row_group: int

    avg_bytes_per_row_group: int
    min_bytes_per_row_group: int
    max_bytes_per_row_group: int

    columns: dict[str, ColumnInfo]

    @classmethod
    def build(cls, filesystem: pafs.FileSystem, path: str) -> "TableInfo":
        """Inspect a parquet table at ``path`` on the given ``filesystem`` (S3, local, etc.)."""
        ds = pq.ParquetDataset(path, filesystem=filesystem)

        rg_flat = [rg for fragment in ds.fragments for rg in fragment.row_groups]

        # Per-partition stats
        rows_per_partition = [
            sum(rg.num_rows for rg in fragment.row_groups) for fragment in ds.fragments
        ]
        bytes_per_partition = [
            sum(rg.total_byte_size for rg in fragment.row_groups)
            for fragment in ds.fragments
        ]

        # Per-row-group stats
        rows_per_rg = [rg.num_rows for rg in rg_flat]
        bytes_per_rg = [rg.total_byte_size for rg in rg_flat]

        # Paths from the dataset are relative to ``filesystem`` (no s3:// prefix).
        parquet_files = _parquet_paths_for_dataset(ds, path)
        if not parquet_files:
            raise ValueError(
                f"No parquet file paths resolved for dataset {path!r}"
            )

        first_pf = pq.ParquetFile(parquet_files[0], filesystem=filesystem)
        file_metadata = first_pf.metadata
        parquet_format_version: Literal[1, 2] = (
            2 if file_metadata.format_version == "2.6" else 1
        )
        created_by = file_metadata.created_by or ""

        # Collect column metadata aggregated across all files/row groups
        columns: dict[str, ColumnInfo] = {}
        column_aggregates: dict[str, dict[str, Any]] = {}

        # Process files in parallel
        with ThreadPoolExecutor() as executor:
            results = list(
                executor.map(
                    functools.partial(
                        process_parquet_file,
                        table_schema=ds.schema,
                        filesystem=filesystem,
                    ),
                    parquet_files,
                )
            )

        # Merge results from all files
        for file_aggregates in results:
            for col_name, file_agg in file_aggregates.items():
                if col_name not in column_aggregates:
                    column_aggregates[col_name] = file_agg
                else:
                    agg = column_aggregates[col_name]
                    agg["encodings"].update(file_agg["encodings"])
                    agg["total_compressed_size"] += file_agg["total_compressed_size"]
                    agg["total_uncompressed_size"] += file_agg[
                        "total_uncompressed_size"
                    ]
                    agg["min_values"].extend(file_agg["min_values"])
                    agg["max_values"].extend(file_agg["max_values"])
                    agg["null_count"] += file_agg["null_count"]
                    agg["num_values"] += file_agg["num_values"]

        # Build ColumnInfo objects from aggregates
        for col_name, agg in column_aggregates.items():
            # Compute aggregated stats
            col_stats: ColumnStats | None = None
            if agg["is_stats_set"] and agg["min_values"] and agg["max_values"]:
                col_stats = ColumnStats(
                    min=min(agg["min_values"]),
                    max=max(agg["max_values"]),
                    null_count=agg["null_count"],
                    num_values=agg["num_values"],
                )

            columns[col_name] = ColumnInfo(
                name=col_name,
                physical_type=agg["physical_type"],
                is_stats_set=agg["is_stats_set"],
                stats=col_stats,
                encodings=sorted(agg["encodings"]),
                compression=agg["compression"],
                total_compressed_size=agg["total_compressed_size"],
                total_uncompressed_size=agg["total_uncompressed_size"],
            )

        return cls(
            table_schema=ds.schema,
            partition_count=len(ds.fragments),
            row_group_count=len(rg_flat),
            row_count=sum(rows_per_rg),
            total_bytes=sum(bytes_per_rg),
            parquet_format_version=parquet_format_version,
            created_by=created_by,
            avg_rows_per_partition=int(statistics.mean(rows_per_partition))
            if rows_per_partition
            else 0,
            min_rows_per_partition=min(rows_per_partition, default=0),
            max_rows_per_partition=max(rows_per_partition, default=0),
            avg_bytes_per_partition=int(statistics.mean(bytes_per_partition))
            if bytes_per_partition
            else 0,
            min_bytes_per_partition=min(bytes_per_partition, default=0),
            max_bytes_per_partition=max(bytes_per_partition, default=0),
            avg_rows_per_row_group=int(statistics.mean(rows_per_rg))
            if rows_per_rg
            else 0,
            min_rows_per_row_group=min(rows_per_rg, default=0),
            max_rows_per_row_group=max(rows_per_rg, default=0),
            avg_bytes_per_row_group=int(statistics.mean(bytes_per_rg))
            if bytes_per_rg
            else 0,
            min_bytes_per_row_group=min(bytes_per_rg, default=0),
            max_bytes_per_row_group=max(bytes_per_rg, default=0),
            columns=columns,
        )

    def serialize(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict."""
        return {
            "partition_count": self.partition_count,
            "row_group_count": self.row_group_count,
            "row_count": self.row_count,
            "total_bytes": self.total_bytes,
            "parquet_format_version": self.parquet_format_version,
            "created_by": self.created_by,
            "table_schema": [
                {
                    "name": field.name,
                    "type": str(field.type),
                    "nullable": field.nullable,
                }
                for field in self.table_schema
            ],
            "avg_rows_per_partition": self.avg_rows_per_partition,
            "min_rows_per_partition": self.min_rows_per_partition,
            "max_rows_per_partition": self.max_rows_per_partition,
            "avg_bytes_per_partition": self.avg_bytes_per_partition,
            "min_bytes_per_partition": self.min_bytes_per_partition,
            "max_bytes_per_partition": self.max_bytes_per_partition,
            "avg_rows_per_row_group": self.avg_rows_per_row_group,
            "min_rows_per_row_group": self.min_rows_per_row_group,
            "max_rows_per_row_group": self.max_rows_per_row_group,
            "avg_bytes_per_row_group": self.avg_bytes_per_row_group,
            "min_bytes_per_row_group": self.min_bytes_per_row_group,
            "max_bytes_per_row_group": self.max_bytes_per_row_group,
            "columns": {name: col.serialize() for name, col in self.columns.items()},
        }


@dataclasses.dataclass
class Metadata:
    options: dict[str, Any]
    tables: dict[str, TableInfo]

    def serialize(self) -> dict[str, Any]:
        result = dataclasses.asdict(self)
        result["tables"] = {
            table: info.serialize() for table, info in self.tables.items()
        }
        return result


def _parquet_paths_for_dataset(ds: pq.ParquetDataset, dataset_path: str) -> list[str]:
    """Resolve parquet file paths within ``ds.filesystem`` (no URI prefix)."""
    files = ds.files
    if files:
        return list(files)
    out: list[str] = []
    for frag in ds.fragments:
        try:
            out.append(str(frag.path))
        except AttributeError:
            continue
    if out:
        return out
    fs = ds.filesystem
    # Single-file dataset: path may be the file itself
    try:
        info = fs.get_file_info(dataset_path)
    except OSError:
        return []
    if info.type == pafs.FileType.File:
        return [dataset_path]
    return []


def _is_s3_url(s: str) -> bool:
    return s.startswith("s3://")


def open_filesystem(location: str | Path) -> tuple[pafs.FileSystem, str]:
    """
    Open a PyArrow filesystem and root path for a local directory/file or ``s3://`` URI.

    The returned ``path`` is always relative to that filesystem (what ``ParquetDataset``
    and ``ParquetFile`` expect together with ``filesystem=``).
    """
    s = str(location)
    if _is_s3_url(s):
        return pafs.FileSystem.from_uri(s.rstrip("/"))
    p = Path(s).expanduser().resolve()
    return pafs.FileSystem.from_uri(p.as_uri())



def _table_path_exists(
    filesystem: pafs.FileSystem, root_path: str, table: str
) -> str | None:
    """Return a parquet dataset path (directory or single file) under ``root_path`` if present."""
    root_path = root_path.rstrip("/")
    candidates = [
        f"{root_path}/{table}",
        f"{root_path}/{table}.parquet",
    ]
    for candidate in candidates:
        try:
            ds = pq.ParquetDataset(candidate, filesystem=filesystem)
            if ds.fragments:
                return candidate
        except (OSError, pa.ArrowInvalid, ValueError):
            continue
    return None


def write_json_destination(destination: str, data: dict[str, Any]) -> None:
    """Write JSON to a local path or s3:// URL using PyArrow filesystem (not fsspec)."""
    with open(destination, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def process_parquet_file(
    pf_path: str,
    table_schema: pa.Schema,
    *,
    filesystem: pafs.FileSystem,
) -> dict[str, dict[str, Any]]:
    """Process a single parquet file and return its column aggregates."""
    file_aggregates: dict[str, dict[str, Any]] = {}
    pf = pq.ParquetFile(pf_path, filesystem=filesystem)
    metadata = pf.metadata

    for rg_idx in range(metadata.num_row_groups):
        rg = metadata.row_group(rg_idx)

        for col_idx in range(rg.num_columns):
            col = rg.column(col_idx)
            col_name = col.path_in_schema

            if col_name not in file_aggregates:
                file_aggregates[col_name] = {
                    "physical_type": str(col.physical_type),
                    "compression": str(col.compression),
                    "encodings": set(),
                    "total_compressed_size": 0,
                    "total_uncompressed_size": 0,
                    "is_stats_set": col.is_stats_set,
                    "min_values": [],
                    "max_values": [],
                    "null_count": 0,
                    "num_values": 0,
                }

            agg = file_aggregates[col_name]
            agg["encodings"].update(str(e) for e in col.encodings)
            agg["total_compressed_size"] += col.total_compressed_size
            agg["total_uncompressed_size"] += col.total_uncompressed_size

            # Aggregate stats if available
            if col.is_stats_set and col.statistics is not None:
                stats = col.statistics
                if stats.has_min_max:
                    # For Decimal types, pyarrow might throw a not implemented error here :/
                    if not pa.types.is_decimal128(table_schema.field(col_name).type):
                        agg["min_values"].append(stats.min)
                        agg["max_values"].append(stats.max)
                agg["null_count"] += stats.null_count
                agg["num_values"] += stats.num_values

    return file_aggregates


def inspect_table_text(
    table_name: str, info: TableInfo, show_schema: bool = True, show_sizes: bool = True
):
    """Print metadata for a parquet table in text format."""
    print(f"\n{table_name}")
    print("=" * 80)

    if show_sizes:
        print(f"  Partitions:  {info.partition_count}")
        print(f"  Row groups:  {info.row_group_count}")
        print(f"  Total rows:  {info.row_count:,}")
        print(f"  Total bytes: {info.total_bytes:,}")

        print("\n  Partition stats:")
        print(
            f"    Rows:  avg={info.avg_rows_per_partition:,}  min={info.min_rows_per_partition:,}  max={info.max_rows_per_partition:,}"
        )
        print(
            f"    Bytes: avg={info.avg_bytes_per_partition:,}  min={info.min_bytes_per_partition:,}  max={info.max_bytes_per_partition:,}"
        )

        print("\n  Row group stats:")
        print(
            f"    Rows:  avg={info.avg_rows_per_row_group:,}  min={info.min_rows_per_row_group:,}  max={info.max_rows_per_row_group:,}"
        )
        print(
            f"    Bytes: avg={info.avg_bytes_per_row_group:,}  min={info.min_bytes_per_row_group:,}  max={info.max_bytes_per_row_group:,}"
        )

    if show_schema:
        if show_sizes:
            print()
        print("  Schema:")
        print("  " + "-" * 40)
        for i, field in enumerate(info.table_schema):
            print(f"    {i}: {field.name} ({field.type}, nullable={field.nullable})")


def main():
    parser = argparse.ArgumentParser(description="Inspect TPC-H parquet files")
    parser.add_argument(
        "data_dir",
        type=str,
        help="Path or s3:// URL to TPC-H data directory",
    )
    parser.add_argument(
        "--schema/--no-schema",
        dest="show_schema",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Print schema information (default: True)",
    )
    parser.add_argument(
        "--sizes/--no-sizes",
        dest="show_sizes",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Print partition/row group size statistics (default: True)",
    )
    parser.add_argument(
        "-o",
        "--output",
        choices=["text", "csv", "json"],
        default="text",
        help="Output format (default: text)",
    )
    parser.add_argument(
        "--output-file",
        type=Path,
        default=None,
        help="Write output to file instead of stdout (only used with --output json)",
    )
    parser.add_argument(
        "--options",
        type=json.loads,
        default={},
        help="Options to include in the metadata (default: {})",
    )
    parser.add_argument(
        "-k",
        "--kind",
        choices=["tpch", "tpcds"],
        help="Dataset kind. Controls which tables are expected.",
        default="tpch",
    )
    args = parser.parse_args()

    data_dir = args.data_dir
    if not _is_s3_url(data_dir) and not Path(data_dir).exists():
        print(f"Error: Directory {data_dir} does not exist", file=sys.stderr)
        sys.exit(1)

    root_filesystem, root_path = open_filesystem(data_dir)

    if args.output == "text":
        print(f"Inspecting {data_dir}")

    if args.kind == "tpch":
        tables = [
            "customer",
            "lineitem",
            "nation",
            "orders",
            "part",
            "partsupp",
            "region",
            "supplier",
        ]
    else:
        tables = [
            "call_center",
            "catalog_page",
            "catalog_returns",
            "catalog_sales",
            "customer",
            "customer_address",
            "customer_demographics",
            "date_dim",
            "household_demographics",
            "income_band",
            "inventory",
            "item",
            "promotion",
            "reason",
            "ship_mode",
            "store",
            "store_returns",
            "store_sales",
            "time_dim",
            "warehouse",
            "web_page",
            "web_returns",
            "web_sales",
            "web_site",
        ]

    # Collect all table info (one filesystem for the whole run; paths are fs-relative)
    table_infos: list[tuple[str, TableInfo]] = []
    for table in tables:
        table_path = _table_path_exists(root_filesystem, root_path, table)
        if table_path is None:
            if args.output == "text":
                base = data_dir.rstrip("/")
                print(
                    f"Warning: {table} not found (checked {base}/{table} and "
                    f"{base}/{table}.parquet)"
                )
            continue

        info = TableInfo.build(root_filesystem, table_path)
        table_infos.append((table, info))

    if args.output == "text":
        for table, info in table_infos:
            inspect_table_text(
                table, info, show_schema=args.show_schema, show_sizes=args.show_sizes
            )
    elif args.output == "csv":
        writer = csv.writer(sys.stdout)
        if args.show_schema:
            writer.writerow(["table_name", "name", "type", "nullable"])
            for table, info in table_infos:
                for field in info.table_schema:
                    writer.writerow(
                        [table, field.name, str(field.type), field.nullable]
                    )
        else:
            writer.writerow(
                [
                    "table_name",
                    "partition_count",
                    "row_group_count",
                    "row_count",
                    "total_bytes",
                    "avg_rows_per_partition",
                    "min_rows_per_partition",
                    "max_rows_per_partition",
                    "avg_bytes_per_partition",
                    "min_bytes_per_partition",
                    "max_bytes_per_partition",
                    "avg_rows_per_row_group",
                    "min_rows_per_row_group",
                    "max_rows_per_row_group",
                    "avg_bytes_per_row_group",
                    "min_bytes_per_row_group",
                    "max_bytes_per_row_group",
                ]
            )
            for table, info in table_infos:
                writer.writerow(
                    [
                        table,
                        info.partition_count,
                        info.row_group_count,
                        info.row_count,
                        info.total_bytes,
                        info.avg_rows_per_partition,
                        info.min_rows_per_partition,
                        info.max_rows_per_partition,
                        info.avg_bytes_per_partition,
                        info.min_bytes_per_partition,
                        info.max_bytes_per_partition,
                        info.avg_rows_per_row_group,
                        info.min_rows_per_row_group,
                        info.max_rows_per_row_group,
                        info.avg_bytes_per_row_group,
                        info.min_bytes_per_row_group,
                        info.max_bytes_per_row_group,
                    ]
                )
    elif args.output == "json":
        metadata = Metadata(options=args.options, tables=dict(table_infos))
        # output_data = {table: info.serialize() for table, info in table_infos}
        output_data = metadata.serialize()
        if args.output_file:
            write_json_destination(args.output_file, output_data)
        else:
            json.dump(output_data, sys.stdout, indent=2)
            print()  # Add newline after JSON


if __name__ == "__main__":
    main()
