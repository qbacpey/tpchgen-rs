//! Checks a generated dataset against the generator it came from
//!
//! Three things are checked by reading the written files back:
//!
//! * every file holds exactly the rows the plan promised, within the day range
//!   the plan assigned it, so partition pruning on the sort key is sound
//! * rows are non-decreasing in `(sort key, secondary key)` within each file
//! * the multiset of `(sort key, secondary key, line number)` over the whole
//!   dataset matches the one derived independently from the random streams, so
//!   no row was dropped, duplicated or assigned to the wrong day
//!
//! The last check uses the cheap key-only scan rather than a second full
//! generation, which makes it affordable at large scale factors.

use crate::histogram::day_index;
use crate::layout::Partition;
use arrow::array::{Array, Date32Array, Int32Array, Int64Array};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use std::fs::File;
use std::io;
use std::path::Path;
use tpchgen::dates::{GenerateUtils, TPCHDate};
use tpchgen::generators::{LineItemGenerator, OrderGenerator};

/// An order-independent fingerprint of a set of rows
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Fingerprint {
    pub rows: u64,
    pub checksum: u64,
}

impl Fingerprint {
    fn add(&mut self, day: i64, key: i64, line: i64) {
        self.rows += 1;
        self.checksum = self.checksum.wrapping_add(mix(day, key, line));
    }
}

/// Mixes a row's keys into a value whose sum does not depend on row order
fn mix(day: i64, key: i64, line: i64) -> u64 {
    let mut hash = (day as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    hash ^= (key as u64).wrapping_mul(0xC2B2_AE3D_27D4_EB4F);
    hash ^= (line as u64).wrapping_mul(0x1656_67B1_9E37_79F9);
    hash ^= hash >> 29;
    hash = hash.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    hash ^ (hash >> 32)
}

/// Fingerprints `orders` from its date stream alone
pub fn expected_orders(scale_factor: f64) -> Fingerprint {
    let row_count = OrderGenerator::calculate_row_count(scale_factor, 1, 1);
    let mut order_date_random = OrderGenerator::create_order_date_random();

    let mut fingerprint = Fingerprint::default();
    for index in 1..=row_count {
        let day = TPCHDate::new(order_date_random.next_value()).into_inner();
        order_date_random.row_finished();
        fingerprint.add(day as i64, OrderGenerator::make_order_key(index), 0);
    }
    fingerprint
}

/// Fingerprints `lineitem` from its date and line count streams alone
pub fn expected_lineitem(scale_factor: f64) -> Fingerprint {
    let order_count =
        GenerateUtils::calculate_row_count(OrderGenerator::SCALE_BASE, scale_factor, 1, 1);
    let mut order_date_random = OrderGenerator::create_order_date_random();
    let mut line_count_random = OrderGenerator::create_line_count_random();
    let mut ship_date_random = LineItemGenerator::create_ship_date_random();

    let mut fingerprint = Fingerprint::default();
    for index in 1..=order_count {
        let order_date = order_date_random.next_value();
        let line_count = line_count_random.next_value();
        let order_key = OrderGenerator::make_order_key(index);
        for line in 0..line_count {
            let day = TPCHDate::new(ship_date_random.next_value() + order_date).into_inner();
            fingerprint.add(day as i64, order_key, line as i64 + 1);
        }
        order_date_random.row_finished();
        line_count_random.row_finished();
        ship_date_random.row_finished();
    }
    fingerprint
}

/// The key columns to read back
///
/// `file_columns` are positions in the file's schema and must be ascending;
/// the others are positions in the projected batch, which keeps schema order.
struct Keys {
    file_columns: &'static [usize],
    sort_key: usize,
    secondary_key: usize,
    line_number: Option<usize>,
}

/// `o_orderkey`, `o_orderdate`
const ORDER_KEYS: Keys = Keys {
    file_columns: &[0, 4],
    secondary_key: 0,
    sort_key: 1,
    line_number: None,
};

/// `l_orderkey`, `l_linenumber`, `l_shipdate`
const LINEITEM_KEYS: Keys = Keys {
    file_columns: &[0, 3, 10],
    secondary_key: 0,
    line_number: Some(1),
    sort_key: 2,
};

/// Reads the dataset back and checks it against `partitions` and `expected`
pub fn check(
    table_dir: &Path,
    table: crate::Table,
    partitions: &[Partition],
    expected: Fingerprint,
) -> io::Result<()> {
    let keys = match table {
        crate::Table::Orders => ORDER_KEYS,
        crate::Table::LineItem => LINEITEM_KEYS,
    };

    let mut actual = Fingerprint::default();
    let mut previous_last_day = None;

    for partition in partitions {
        let path = table_dir.join(format!("{}.{}.parquet", table.name(), partition.number));
        let file = File::open(&path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(io::Error::other)?;

        let mask =
            ProjectionMask::roots(builder.parquet_schema(), keys.file_columns.iter().copied());
        let reader = builder
            .with_projection(mask)
            .build()
            .map_err(io::Error::other)?;

        let first_day = day_index(partition.first_day) as i32;
        let last_day = day_index(partition.last_day) as i32;
        if let Some(previous) = previous_last_day {
            if first_day <= previous {
                return Err(io::Error::other(format!(
                    "{}: day range starts at {first_day}, overlapping the previous partition",
                    path.display()
                )));
            }
        }
        previous_last_day = Some(last_day);

        let mut rows = 0u64;
        let mut previous_row = None;
        for batch in reader {
            let batch = batch.map_err(io::Error::other)?;
            let days = batch
                .column(keys.sort_key)
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| io::Error::other("sort key is not a Date32 column"))?;
            let secondary = batch
                .column(keys.secondary_key)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| io::Error::other("secondary key is not an Int64 column"))?;
            let line_numbers = match keys.line_number {
                Some(column) => Some(
                    batch
                        .column(column)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .ok_or_else(|| io::Error::other("line number is not an Int32 column"))?,
                ),
                None => None,
            };

            for row in 0..batch.num_rows() {
                let day = days.value(row) - TPCHDate::UNIX_EPOCH_OFFSET;
                let key = secondary.value(row);
                let line = line_numbers.map_or(0, |numbers| numbers.value(row) as i64);

                if day < first_day || day > last_day {
                    return Err(io::Error::other(format!(
                        "{}: day {day} is outside the partition range {first_day}..={last_day}",
                        path.display()
                    )));
                }
                let current = (day, key, line);
                if let Some(previous) = previous_row {
                    if current < previous {
                        return Err(io::Error::other(format!(
                            "{}: {current:?} follows {previous:?}, output is not sorted",
                            path.display()
                        )));
                    }
                }
                previous_row = Some(current);
                actual.add(day as i64, key, line);
                rows += 1;
            }
        }

        if rows != partition.rows {
            return Err(io::Error::other(format!(
                "{}: read {rows} rows, planned {}",
                path.display(),
                partition.rows
            )));
        }
    }

    if actual != expected {
        return Err(io::Error::other(format!(
            "dataset fingerprint {actual:?} does not match the generator's {expected:?}"
        )));
    }
    Ok(())
}
