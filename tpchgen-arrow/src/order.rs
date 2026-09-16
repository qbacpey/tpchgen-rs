use crate::conversions::{
    decimal128_array_from_iter, string_view_array_from_display_iter, to_arrow_date32,
    to_arrow_timestamp_ms,
};
use crate::{ColumnTypeConfig, DEFAULT_BATCH_SIZE, DateColumnType, DecimalColumnType};
use arrow::array::{
    ArrayRef, Date32Array, Float64Array, Int32Array, Int64Array, RecordBatch, StringViewArray,
    TimestampMillisecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatchReader;
use std::sync::{Arc, LazyLock};
use tpchgen::generators::{OrderGenerator, OrderGeneratorIterator};

/// Generate [`Order`]s in [`RecordBatch`] format
///
/// [`Order`]: tpchgen::generators::Order
///
/// # Example
/// ```
/// # use tpchgen::generators::{OrderGenerator};
/// # use tpchgen_arrow::OrderArrow;
///
/// // Create a SF=1.0 generator and wrap it in an Arrow generator
/// let generator = OrderGenerator::new(1.0, 1, 1);
/// let mut arrow_generator = OrderArrow::new(generator)
///   .with_batch_size(10);
/// // Read the first 10 batches
/// let batch = arrow_generator.next().unwrap().unwrap();
/// // compare the output by pretty printing it
/// let formatted_batches = arrow::util::pretty::pretty_format_batches(&[batch])
///   .unwrap()
///   .to_string();
/// let lines = formatted_batches.lines().collect::<Vec<_>>();
/// assert_eq!(lines, vec![
///   "+------------+-----------+---------------+--------------+-------------+-----------------+-----------------+----------------+---------------------------------------------------------------------------+",
///   "| o_orderkey | o_custkey | o_orderstatus | o_totalprice | o_orderdate | o_orderpriority | o_clerk         | o_shippriority | o_comment                                                                 |",
///   "+------------+-----------+---------------+--------------+-------------+-----------------+-----------------+----------------+---------------------------------------------------------------------------+",
///   "| 1          | 36901     | O             | 173665.47    | 1996-01-02  | 5-LOW           | Clerk#000000951 | 0              | nstructions sleep furiously among                                         |",
///   "| 2          | 78002     | O             | 46929.18     | 1996-12-01  | 1-URGENT        | Clerk#000000880 | 0              |  foxes. pending accounts at the pending, silent asymptot                  |",
///   "| 3          | 123314    | F             | 193846.25    | 1993-10-14  | 5-LOW           | Clerk#000000955 | 0              | sly final accounts boost. carefully regular ideas cajole carefully. depos |",
///   "| 4          | 136777    | O             | 32151.78     | 1995-10-11  | 5-LOW           | Clerk#000000124 | 0              | sits. slyly regular warthogs cajole. regular, regular theodolites acro    |",
///   "| 5          | 44485     | F             | 144659.20    | 1994-07-30  | 5-LOW           | Clerk#000000925 | 0              | quickly. bold deposits sleep slyly. packages use slyly                    |",
///   "| 6          | 55624     | F             | 58749.59     | 1992-02-21  | 4-NOT SPECIFIED | Clerk#000000058 | 0              | ggle. special, final requests are against the furiously specia            |",
///   "| 7          | 39136     | O             | 252004.18    | 1996-01-10  | 2-HIGH          | Clerk#000000470 | 0              | ly special requests                                                       |",
///   "| 32         | 130057    | O             | 208660.75    | 1995-07-16  | 2-HIGH          | Clerk#000000616 | 0              | ise blithely bold, regular requests. quickly unusual dep                  |",
///   "| 33         | 66958     | F             | 163243.98    | 1993-10-27  | 3-MEDIUM        | Clerk#000000409 | 0              | uriously. furiously final request                                         |",
///   "| 34         | 61001     | O             | 58949.67     | 1998-07-21  | 3-MEDIUM        | Clerk#000000223 | 0              | ly final packages. fluffily final deposits wake blithely ideas. spe       |",
///   "+------------+-----------+---------------+--------------+-------------+-----------------+-----------------+----------------+---------------------------------------------------------------------------+"
/// ]);
/// ```
pub struct OrderArrow {
    inner: OrderGeneratorIterator<'static>,
    batch_size: usize,
    column_type_config: ColumnTypeConfig,
    /// Cached schema based on column_type_config
    schema: SchemaRef,
}

impl OrderArrow {
    /// Return the schema without initializing a data generator.
    pub fn schema_ref() -> SchemaRef {
        Arc::clone(&ORDER_SCHEMA)
    }

    pub fn new(generator: OrderGenerator<'static>) -> Self {
        Self {
            inner: generator.iter(),
            batch_size: DEFAULT_BATCH_SIZE,
            column_type_config: ColumnTypeConfig::default(),
            schema: Arc::clone(&ORDER_SCHEMA),
        }
    }

    /// Set the batch size
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set column type configuration to customize column types.
    pub fn with_column_type_config(mut self, config: ColumnTypeConfig) -> Self {
        self.schema = if config == ColumnTypeConfig::default() {
            Arc::clone(&ORDER_SCHEMA)
        } else {
            make_order_schema(&config)
        };
        self.column_type_config = config;
        self
    }
}

impl RecordBatchReader for OrderArrow {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Iterator for OrderArrow {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        // Get next rows to convert
        let rows: Vec<_> = self.inner.by_ref().take(self.batch_size).collect();
        if rows.is_empty() {
            return None;
        }

        let o_orderkey = Int64Array::from_iter_values(rows.iter().map(|r| r.o_orderkey));
        let o_custkey = Int64Array::from_iter_values(rows.iter().map(|r| r.o_custkey));
        let o_orderstatus =
            string_view_array_from_display_iter(rows.iter().map(|r| r.o_orderstatus));

        // Build o_totalprice based on config
        let o_totalprice: ArrayRef = match self.column_type_config.decimal_type {
            DecimalColumnType::F64 => Arc::new(Float64Array::from_iter_values(
                rows.iter().map(|r| r.o_totalprice.as_f64()),
            )),
            DecimalColumnType::Decimal128 => Arc::new(decimal128_array_from_iter(
                rows.iter().map(|r| r.o_totalprice),
            )),
        };

        // Build o_orderdate based on config
        let o_orderdate: ArrayRef = match self.column_type_config.date_type {
            DateColumnType::Date32 => Arc::new(Date32Array::from_iter_values(
                rows.iter().map(|r| to_arrow_date32(r.o_orderdate)),
            )),
            DateColumnType::TimestampMs => Arc::new(TimestampMillisecondArray::from_iter_values(
                rows.iter().map(|r| to_arrow_timestamp_ms(r.o_orderdate)),
            )),
        };

        let o_orderpriority =
            StringViewArray::from_iter_values(rows.iter().map(|r| r.o_orderpriority));
        let o_clerk = string_view_array_from_display_iter(rows.iter().map(|r| r.o_clerk));
        let o_shippriority = Int32Array::from_iter_values(rows.iter().map(|r| r.o_shippriority));
        let o_comment = StringViewArray::from_iter_values(rows.iter().map(|r| r.o_comment));

        Some(RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(o_orderkey),
                Arc::new(o_custkey),
                Arc::new(o_orderstatus),
                o_totalprice,
                o_orderdate,
                Arc::new(o_orderpriority),
                Arc::new(o_clerk),
                Arc::new(o_shippriority),
                Arc::new(o_comment),
            ],
        ))
    }
}

static ORDER_SCHEMA: LazyLock<SchemaRef> =
    LazyLock::new(|| make_order_schema(&ColumnTypeConfig::default()));

fn make_order_schema(config: &ColumnTypeConfig) -> SchemaRef {
    let totalprice_type = match config.decimal_type {
        DecimalColumnType::F64 => DataType::Float64,
        DecimalColumnType::Decimal128 => DataType::Decimal128(15, 2),
    };
    let date_type = match config.date_type {
        DateColumnType::Date32 => DataType::Date32,
        DateColumnType::TimestampMs => {
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None)
        }
    };

    Arc::new(Schema::new(vec![
        Field::new("o_orderkey", DataType::Int64, false),
        Field::new("o_custkey", DataType::Int64, false),
        Field::new("o_orderstatus", DataType::Utf8View, false),
        Field::new("o_totalprice", totalprice_type, false),
        Field::new("o_orderdate", date_type, false),
        Field::new("o_orderpriority", DataType::Utf8View, false),
        Field::new("o_clerk", DataType::Utf8View, false),
        Field::new("o_shippriority", DataType::Int32, false),
        Field::new("o_comment", DataType::Utf8View, false),
    ]))
}
