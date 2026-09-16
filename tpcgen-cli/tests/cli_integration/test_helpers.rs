use arrow::array::RecordBatchReader;
use arrow::datatypes::DataType;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{Compression, Encoding};
use parquet::file::metadata::ParquetMetaDataReader;
use std::fs::File;
use std::path::Path;

#[derive(Debug, PartialEq)]
pub(crate) struct RowGroups {
    pub(crate) table: &'static str,
    /// total bytes in each row group
    pub(crate) row_group_bytes: Vec<i64>,
}

/// For each table in tables, check that the parquet file in output_dir has
/// a file with the expected row group sizes.
pub(crate) fn expect_row_group_sizes(output_dir: &Path, expected_row_groups: Vec<RowGroups>) {
    let mut actual_row_groups = vec![];
    for table in &expected_row_groups {
        let output_path = output_dir.join(format!("{}.parquet", table.table));
        assert!(
            output_path.exists(),
            "Expected parquet file {:?} to exist",
            output_path
        );
        // read the metadata to get the row group size
        let file = File::open(&output_path).expect("Failed to open parquet file");
        let mut metadata_reader = ParquetMetaDataReader::new();
        metadata_reader.try_parse(&file).unwrap();
        let metadata = metadata_reader.finish().unwrap();
        let row_groups = metadata.row_groups();
        let actual_row_group_bytes: Vec<_> =
            row_groups.iter().map(|rg| rg.total_byte_size()).collect();
        actual_row_groups.push(RowGroups {
            table: table.table,
            row_group_bytes: actual_row_group_bytes,
        })
    }
    // compare the expected and actual row groups debug print actual on failure
    // for better output / easier comparison
    let expected_row_groups = format!("{expected_row_groups:#?}");
    let actual_row_groups = format!("{actual_row_groups:#?}");
    assert_eq!(actual_row_groups, expected_row_groups);
}

/// Asserts `column` does not use `forbidden` in any row group.
pub(crate) fn expect_column_encoding_absent(path: &Path, column: &str, forbidden: Encoding) {
    let file = File::open(path).expect("Failed to open parquet file");
    let mut metadata_reader = ParquetMetaDataReader::new();
    metadata_reader.try_parse(&file).unwrap();
    let metadata = metadata_reader.finish().unwrap();
    let mut found_in_any_row_group = false;
    for (row_group_idx, row_group) in metadata.row_groups().iter().enumerate() {
        for col in row_group.columns() {
            if col.column_path().string() == column {
                found_in_any_row_group = true;
                let encodings: Vec<Encoding> = col.encodings().collect();
                assert!(
                    !encodings.contains(&forbidden),
                    "expected {column} to not use {forbidden:?} in row group {row_group_idx}, encodings: {encodings:?}"
                );
            }
        }
    }
    assert!(
        found_in_any_row_group,
        "column {column} not found in {}",
        path.display()
    );
}

/// Asserts `column` uses `expected` as one of its encodings in *every* row
/// group of the file at `path` (not just the first row group that happens to
/// contain it), so a regression that only affects later row groups (e.g. a
/// dictionary-fallback threshold silently reverting to a different encoding
/// partway through the file) doesn't go unnoticed.
pub(crate) fn expect_column_encoding(path: &Path, column: &str, expected: Encoding) {
    let file = File::open(path).expect("Failed to open parquet file");
    let mut metadata_reader = ParquetMetaDataReader::new();
    metadata_reader.try_parse(&file).unwrap();
    let metadata = metadata_reader.finish().unwrap();
    let mut found_in_any_row_group = false;
    for (row_group_idx, row_group) in metadata.row_groups().iter().enumerate() {
        for col in row_group.columns() {
            if col.column_path().string() == column {
                found_in_any_row_group = true;
                let encodings: Vec<Encoding> = col.encodings().collect();
                assert!(
                    encodings.contains(&expected),
                    "expected {column} to use {expected:?} in row group {row_group_idx}, encodings: {encodings:?}"
                );
            }
        }
    }
    assert!(
        found_in_any_row_group,
        "column {column} not found in {}",
        path.display()
    );
}

/// Asserts `column` uses `expected` block compression in every row group.
pub(crate) fn expect_parquet_file_version(path: &Path, expected: i32) {
    let file = File::open(path).expect("Failed to open parquet file");
    let mut metadata_reader = ParquetMetaDataReader::new();
    metadata_reader.try_parse(&file).unwrap();
    let metadata = metadata_reader.finish().unwrap();
    assert_eq!(
        metadata.file_metadata().version(),
        expected,
        "unexpected parquet file version for {}",
        path.display()
    );
}

/// Asserts `column` has Arrow type `expected` in the Parquet file schema.
pub(crate) fn expect_column_arrow_type(path: &Path, column: &str, expected: &DataType) {
    let file = File::open(path).expect("Failed to open parquet file");
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("parquet reader")
        .build()
        .expect("build reader");
    let schema = reader.schema();
    let field = schema
        .field_with_name(column)
        .expect("column not found in schema");
    assert_eq!(
        field.data_type(),
        expected,
        "unexpected Arrow type for {column} in {}",
        path.display()
    );
}

pub(crate) fn expect_column_compression(path: &Path, column: &str, expected: Compression) {
    let file = File::open(path).expect("Failed to open parquet file");
    let mut metadata_reader = ParquetMetaDataReader::new();
    metadata_reader.try_parse(&file).unwrap();
    let metadata = metadata_reader.finish().unwrap();
    let mut found_in_any_row_group = false;
    for (row_group_idx, row_group) in metadata.row_groups().iter().enumerate() {
        for col in row_group.columns() {
            if col.column_path().string() == column {
                found_in_any_row_group = true;
                assert_eq!(
                    col.compression(),
                    expected,
                    "expected {column} to use {expected:?} in row group {row_group_idx}"
                );
            }
        }
    }
    assert!(
        found_in_any_row_group,
        "column {column} not found in {}",
        path.display()
    );
}
