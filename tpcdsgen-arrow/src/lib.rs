//! Generate TPC-DS data as Apache Arrow [`RecordBatch`](arrow::array::RecordBatch)es.
//!
//! This crate wraps the [`tpcdsgen`] row generators and produces typed Arrow
//! arrays directly — bypassing the intermediate string formatting step —
//! for significantly faster ingestion into Arrow-based engines.
//!
//! # Example
//! ```
//! use tpcdsgen::config::Session;
//! use tpcdsgen_arrow::ReasonArrow;
//!
//! let session = Session::default();
//! let mut gen = ReasonArrow::new(session).with_batch_size(100);
//! let batch = gen.next().unwrap().unwrap();
//! assert_eq!(batch.num_columns(), 3);
//! ```

pub mod conversions;
mod tables;

use std::collections::VecDeque;
use std::fmt;
use std::str::FromStr;

use tpcdsgen::config::Session;
use tpcdsgen::row::{GeneratedRow, RowGenerator};

pub use tables::{
    CallCenterArrow, CatalogPageArrow, CatalogReturnsArrow, CatalogSalesArrow,
    CustomerAddressArrow, CustomerArrow, CustomerDemographicsArrow, DateDimArrow,
    DbgenVersionArrow, HouseholdDemographicsArrow, IncomeBandArrow, InventoryArrow, ItemArrow,
    PromotionArrow, ReasonArrow, ShipModeArrow, StoreArrow, StoreReturnsArrow, StoreSalesArrow,
    TimeDimArrow, WarehouseArrow, WebPageArrow, WebReturnsArrow, WebSalesArrow, WebSiteArrow,
};

/// Default number of rows per [`RecordBatch`](arrow::array::RecordBatch).
pub const DEFAULT_BATCH_SIZE: usize = 8_000;

/// Type to use for decimal/monetary columns.
///
/// Controls the Arrow type for TPC-DS monetary columns such as prices and totals.
/// Generated values fit exactly in `f64`, but the default `Decimal128(38, 2)`
/// declared precision exceeds what `f64` represents exactly.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DecimalColumnType {
    #[default]
    Decimal128,
    F64,
}

impl fmt::Display for DecimalColumnType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DecimalColumnType::Decimal128 => write!(f, "decimal128"),
            DecimalColumnType::F64 => write!(f, "f64"),
        }
    }
}

impl FromStr for DecimalColumnType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "decimal128" => Ok(DecimalColumnType::Decimal128),
            "f64" => Ok(DecimalColumnType::F64),
            _ => Err(format!(
                "Invalid decimal column type: '{}'. Valid values are: decimal128, f64",
                s
            )),
        }
    }
}

/// Type to use for date columns.
///
/// Controls the Arrow type for TPC-DS `Date32` columns such as `d_date` and
/// slowly-changing-dimension validity dates.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DateColumnType {
    #[default]
    Date32,
    TimestampMs,
}

impl fmt::Display for DateColumnType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DateColumnType::Date32 => write!(f, "date32"),
            DateColumnType::TimestampMs => write!(f, "timestamp_ms"),
        }
    }
}

impl FromStr for DateColumnType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "date32" => Ok(DateColumnType::Date32),
            "timestamp_ms" => Ok(DateColumnType::TimestampMs),
            _ => Err(format!(
                "Invalid date column type: '{}'. Valid values are: date32, timestamp_ms",
                s
            )),
        }
    }
}

/// Configuration for column types in generated TPC-DS Arrow data.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ColumnTypeConfig {
    pub decimal_type: DecimalColumnType,
    pub date_type: DateColumnType,
}

/// Adapts a [`RowGenerator`] into a streaming [`Iterator`] of [`GeneratedRow`]s.
///
/// Handles both simple generators (one row per call, `should_end_row` always
/// true) and paired fact-table generators (multiple calls per source row,
/// `should_end_row` signals when to advance the row counter).
pub(crate) struct RowIter<G: RowGenerator> {
    generator: G,
    session: Session,
    current_row: i64,
    row_count: i64,
    pending: VecDeque<GeneratedRow>,
}

impl<G: RowGenerator> RowIter<G> {
    pub(crate) fn new(generator: G, session: Session, row_count: i64) -> Self {
        Self {
            generator,
            session,
            current_row: 1,
            row_count,
            pending: VecDeque::new(),
        }
    }

    pub(crate) fn skip_rows_until_starting_row_number(&mut self, starting_row_number: i64) {
        self.generator
            .skip_rows_until_starting_row_number(starting_row_number);
        self.current_row = starting_row_number;
        self.pending.clear();
    }

    /// Restrict generation to source rows
    /// `starting_row_number..=ending_row_number` (1-based, inclusive).
    ///
    /// The ending row number is clamped to the table's row count.
    pub(crate) fn set_source_row_range(
        &mut self,
        starting_row_number: i64,
        ending_row_number: i64,
    ) {
        self.skip_rows_until_starting_row_number(starting_row_number);
        self.row_count = self.row_count.min(ending_row_number);
    }
}

impl<G: RowGenerator> Iterator for RowIter<G> {
    type Item = GeneratedRow;

    fn next(&mut self) -> Option<GeneratedRow> {
        while self.pending.is_empty() {
            if self.current_row > self.row_count {
                return None;
            }
            let result = self
                .generator
                .generate_row_and_child_rows(self.current_row, &self.session, None, None)
                .expect("row gen");
            for row in result.get_rows() {
                self.pending.push_back(row.clone());
            }
            if result.should_end_row() {
                self.generator.consume_remaining_seeds_for_row();
                self.current_row += 1;
            }
        }
        self.pending.pop_front()
    }
}
