#!/usr/bin/env python3
"""
Compare TPC-H parquet output directories.

Focuses on row group counts and sizes to validate data generation consistency.
"""

import argparse
from pathlib import Path
from collections import defaultdict
import pyarrow.parquet as pq


def get_parquet_files(directory: Path) -> dict[str, list[Path]]:
    """Get all parquet files organized by table name."""
    files = defaultdict(list)
    for table_dir in sorted(directory.iterdir()):
        if table_dir.is_dir():
            table_name = table_dir.name
            for pf in sorted(table_dir.glob("*.parquet")):
                files[table_name].append(pf)
    return dict(files)


def get_parquet_metadata(path: Path) -> dict:
    """Extract metadata from a parquet file."""
    pf = pq.ParquetFile(path)
    metadata = pf.metadata
    
    row_groups = []
    for i in range(metadata.num_row_groups):
        rg = metadata.row_group(i)
        row_groups.append({
            "num_rows": rg.num_rows,
            "total_byte_size": rg.total_byte_size,
            "num_columns": rg.num_columns,
        })
    
    return {
        "num_rows": metadata.num_rows,
        "num_row_groups": metadata.num_row_groups,
        "num_columns": metadata.num_columns,
        "row_groups": row_groups,
        "schema": metadata.schema.to_arrow_schema(),
    }


def format_bytes(size: int) -> str:
    """Format bytes to human readable string."""
    for unit in ["B", "KB", "MB", "GB"]:
        if abs(size) < 1024:
            return f"{size:,.1f} {unit}"
        size /= 1024
    return f"{size:,.1f} TB"


def check_file_differences(meta1: dict, meta2: dict) -> dict:
    """Check for differences between two file metadata dicts.
    
    Returns a dict with difference info.
    """
    diffs = {
        "rows_differ": meta1["num_rows"] != meta2["num_rows"],
        "num_row_groups_differ": meta1["num_row_groups"] != meta2["num_row_groups"],
        "columns_differ": meta1["num_columns"] != meta2["num_columns"],
        "row_group_rows_differ": [],
        "row_group_bytes_differ": [],
    }
    
    # Check row group differences (only if same number of row groups)
    if not diffs["num_row_groups_differ"]:
        for rg_idx in range(len(meta1["row_groups"])):
            rg1 = meta1["row_groups"][rg_idx]
            rg2 = meta2["row_groups"][rg_idx]
            if rg1["num_rows"] != rg2["num_rows"]:
                diffs["row_group_rows_differ"].append(rg_idx)
            if rg1["total_byte_size"] != rg2["total_byte_size"]:
                diffs["row_group_bytes_differ"].append(rg_idx)
    
    diffs["has_any_difference"] = (
        diffs["rows_differ"]
        or diffs["num_row_groups_differ"]
        or diffs["columns_differ"]
        or diffs["row_group_rows_differ"]
        or diffs["row_group_bytes_differ"]
    )
    
    return diffs


def compare_directories(dir1: Path, dir2: Path, verbose: bool = False):
    """Compare two parquet directories."""
    files1 = get_parquet_files(dir1)
    files2 = get_parquet_files(dir2)
    
    all_tables = sorted(set(files1.keys()) | set(files2.keys()))
    
    print("=" * 100)
    print("Comparing parquet outputs")
    print(f"  Directory 1: {dir1}")
    print(f"  Directory 2: {dir2}")
    print(f"  Mode: {'verbose' if verbose else 'differences only'}")
    print("=" * 100)
    print()
    
    # Cache metadata to avoid reading files twice
    metadata_cache: dict[Path, dict] = {}
    
    def get_cached_metadata(path: Path) -> dict:
        if path not in metadata_cache:
            metadata_cache[path] = get_parquet_metadata(path)
        return metadata_cache[path]
    
    # Summary table
    print("SUMMARY BY TABLE")
    print("-" * 100)
    header = f"{'Table':<12} | {'Dir1 Files':>10} | {'Dir2 Files':>10} | {'Dir1 Rows':>14} | {'Dir2 Rows':>14} | {'Rows Match':>10}"
    print(header)
    print("-" * 100)
    
    for table in all_tables:
        f1 = files1.get(table, [])
        f2 = files2.get(table, [])
        
        total_rows1 = sum(get_cached_metadata(f)["num_rows"] for f in f1) if f1 else 0
        total_rows2 = sum(get_cached_metadata(f)["num_rows"] for f in f2) if f2 else 0
        
        match = "✓" if total_rows1 == total_rows2 else "✗"
        
        print(f"{table:<12} | {len(f1):>10} | {len(f2):>10} | {total_rows1:>14,} | {total_rows2:>14,} | {match:>10}")
    
    print()
    print()
    
    # Detailed row group comparison
    print("ROW GROUP DETAILS")
    print("=" * 100)
    
    any_differences = False
    
    for table in all_tables:
        f1 = files1.get(table, [])
        f2 = files2.get(table, [])
        
        table_has_differences = len(f1) != len(f2)
        table_output_lines = []
        
        # Check for missing files
        if not f1:
            table_has_differences = True
            table_output_lines.append("  [Dir1] No files found")
        if not f2:
            table_has_differences = True
            table_output_lines.append("  [Dir2] No files found")
        
        # Compare each file
        for i, (path1, path2) in enumerate(zip(f1, f2)):
            meta1 = get_cached_metadata(path1)
            meta2 = get_cached_metadata(path2)
            
            diffs = check_file_differences(meta1, meta2)
            
            if diffs["has_any_difference"]:
                table_has_differences = True
            
            # Only add file details if verbose or there are differences
            if verbose or diffs["has_any_difference"]:
                table_output_lines.append(f"\n  File {i}: {path1.name} vs {path2.name}")
                table_output_lines.append(f"    {'Metric':<25} {'Dir1':>20} {'Dir2':>20} {'Match':>10}")
                table_output_lines.append(f"    {'-'*75}")
                
                # Compare basic stats
                rows_match = "✓" if not diffs["rows_differ"] else "✗"
                rg_match = "✓" if not diffs["num_row_groups_differ"] else "✗"
                cols_match = "✓" if not diffs["columns_differ"] else "✗"
                
                table_output_lines.append(f"    {'Total Rows':<25} {meta1['num_rows']:>20,} {meta2['num_rows']:>20,} {rows_match:>10}")
                table_output_lines.append(f"    {'Num Row Groups':<25} {meta1['num_row_groups']:>20,} {meta2['num_row_groups']:>20,} {rg_match:>10}")
                table_output_lines.append(f"    {'Num Columns':<25} {meta1['num_columns']:>20,} {meta2['num_columns']:>20,} {cols_match:>10}")
                
                # Row group size details
                max_rgs = max(len(meta1["row_groups"]), len(meta2["row_groups"]))
                
                # Only show row group rows if verbose or there are differences
                if verbose or diffs["num_row_groups_differ"] or diffs["row_group_rows_differ"]:
                    table_output_lines.append("\n    Row Group Sizes (rows):")
                    for rg_idx in range(max_rgs):
                        rg1_rows = meta1["row_groups"][rg_idx]["num_rows"] if rg_idx < len(meta1["row_groups"]) else None
                        rg2_rows = meta2["row_groups"][rg_idx]["num_rows"] if rg_idx < len(meta2["row_groups"]) else None
                        
                        rg1_str = f"{rg1_rows:,}" if rg1_rows is not None else "N/A"
                        rg2_str = f"{rg2_rows:,}" if rg2_rows is not None else "N/A"
                        match = "✓" if rg1_rows == rg2_rows else "✗"
                        
                        # In non-verbose mode, only show differing row groups
                        if verbose or rg1_rows != rg2_rows:
                            table_output_lines.append(f"      RG {rg_idx:<3}: {rg1_str:>18} {rg2_str:>20} {match:>10}")
                
                # Only show row group bytes if verbose or there are differences
                if verbose or diffs["num_row_groups_differ"] or diffs["row_group_bytes_differ"]:
                    table_output_lines.append("\n    Row Group Sizes (bytes):")
                    for rg_idx in range(max_rgs):
                        rg1_bytes = meta1["row_groups"][rg_idx]["total_byte_size"] if rg_idx < len(meta1["row_groups"]) else None
                        rg2_bytes = meta2["row_groups"][rg_idx]["total_byte_size"] if rg_idx < len(meta2["row_groups"]) else None
                        
                        rg1_str = format_bytes(rg1_bytes) if rg1_bytes is not None else "N/A"
                        rg2_str = format_bytes(rg2_bytes) if rg2_bytes is not None else "N/A"
                        
                        diff = ""
                        if rg1_bytes is not None and rg2_bytes is not None:
                            pct_diff = ((rg2_bytes - rg1_bytes) / rg1_bytes * 100) if rg1_bytes > 0 else 0
                            diff = f"{pct_diff:+.1f}%"
                        
                        # In non-verbose mode, only show differing row groups
                        if verbose or rg1_bytes != rg2_bytes:
                            table_output_lines.append(f"      RG {rg_idx:<3}: {rg1_str:>18} {rg2_str:>20} {diff:>10}")
        
        # Handle case where one dir has more files
        if len(f1) != len(f2):
            table_output_lines.append(f"\n  ⚠ File count mismatch: Dir1 has {len(f1)} files, Dir2 has {len(f2)} files")
            if len(f1) > len(f2):
                for f in f1[len(f2):]:
                    table_output_lines.append(f"    Dir1 extra: {f.name}")
            else:
                for f in f2[len(f1):]:
                    table_output_lines.append(f"    Dir2 extra: {f.name}")
        
        # Print table output if verbose or has differences
        if verbose or table_has_differences:
            any_differences = any_differences or table_has_differences
            print(f"\n{table.upper()}")
            print("-" * 80)
            for line in table_output_lines:
                print(line)
        elif not verbose:
            # Just note that table matches
            pass
    
    if not verbose and not any_differences:
        print("\n  All tables match! Use --verbose to see full details.")
    
    print()
    print("=" * 100)
    print("Comparison complete!")


def main():
    parser = argparse.ArgumentParser(
        description="Compare TPC-H parquet output directories"
    )
    parser.add_argument("dir1", type=Path, help="First directory to compare")
    parser.add_argument("dir2", type=Path, help="Second directory to compare")
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Show all details (by default, only differences are shown)"
    )
    
    args = parser.parse_args()
    
    if not args.dir1.exists():
        print(f"Error: {args.dir1} does not exist")
        return 1
    if not args.dir2.exists():
        print(f"Error: {args.dir2} does not exist")
        return 1
    
    compare_directories(args.dir1, args.dir2, verbose=args.verbose)
    return 0


if __name__ == "__main__":
    exit(main())
