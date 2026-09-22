//! Generates small datasets and checks them against the generator

use parquet::basic::Compression;
use std::path::Path;
use tpcgen_cli::parquet::ParquetVersion;
use tpchgen_arrow::ColumnTypeConfig;
use tpchgen_sorted::verify::{self, Fingerprint};
use tpchgen_sorted::write::{self, LineItems, Options, Orders, Report, SortedTable, Table};

fn options(output_dir: &Path, files: usize, files_per_pass: usize) -> Options {
    Options {
        scale_factor: 0.05,
        output_dir: output_dir.to_path_buf(),
        file_count: files,
        threads: 3,
        partitions_per_pass: files_per_pass,
        row_group_bytes: 64 * 1024,
        batch_rows: 1024,
        compression: Compression::UNCOMPRESSED,
        column_encodings: None,
        uncompressed_column_overrides: Vec::new(),
        disable_dictionary_encoding_columns: Vec::new(),
        parquet_version: ParquetVersion::V1,
        column_types: ColumnTypeConfig::default(),
        plan_only: false,
    }
}

fn check(options: &Options, table: Table, report: &Report, expected: Fingerprint) {
    assert_eq!(report.rows, expected.rows);
    assert_eq!(report.partitions.len(), options.file_count);

    let table_dir = options.output_dir.join(table.name());
    verify::check(&table_dir, table, &report.partitions, expected).unwrap();
    assert!(table_dir.join("sort_metadata.json").exists());
}

#[test]
fn orders_are_sorted_by_date_then_key() {
    let output = tempfile::tempdir().unwrap();
    let options = options(output.path(), 4, 4);

    let report = write::generate::<Orders>(&options).unwrap();

    assert_eq!(report.passes, 1);
    check(
        &options,
        Table::Orders,
        &report,
        verify::expected_orders(options.scale_factor),
    );
}

#[test]
fn lineitems_are_sorted_by_ship_date_then_key() {
    let output = tempfile::tempdir().unwrap();
    let options = options(output.path(), 4, 4);

    let report = write::generate::<LineItems>(&options).unwrap();

    check(
        &options,
        Table::LineItem,
        &report,
        verify::expected_lineitem(options.scale_factor),
    );
}

/// Filling a few partitions per sweep of the key space bounds memory, and must
/// not change the output.
#[test]
fn multiple_passes_produce_the_same_dataset() {
    let single = tempfile::tempdir().unwrap();
    let multiple = tempfile::tempdir().unwrap();

    let one_pass = options(single.path(), 6, 6);
    let six_passes = options(multiple.path(), 6, 1);
    let single_report = write::generate::<LineItems>(&one_pass).unwrap();
    let multiple_report = write::generate::<LineItems>(&six_passes).unwrap();

    assert_eq!(single_report.passes, 1);
    assert_eq!(multiple_report.passes, 6);
    assert_eq!(single_report.partitions, multiple_report.partitions);

    let expected = verify::expected_lineitem(one_pass.scale_factor);
    check(&one_pass, Table::LineItem, &single_report, expected);
    check(&six_passes, Table::LineItem, &multiple_report, expected);

    for partition in &single_report.partitions {
        let name = format!("{}.{}.parquet", LineItems::NAME, partition.number);
        let left = std::fs::read(single.path().join(LineItems::NAME).join(&name)).unwrap();
        let right = std::fs::read(multiple.path().join(LineItems::NAME).join(&name)).unwrap();
        assert_eq!(left, right, "{name} differs between pass counts");
    }
}
