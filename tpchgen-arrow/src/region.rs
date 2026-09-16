use crate::{ColumnTypeConfig, DEFAULT_BATCH_SIZE, KeyColumnType};
use arrow::array::{ArrayRef, Int32Array, Int64Array, RecordBatch, StringViewArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatchReader;
use std::sync::{Arc, LazyLock};
use tpchgen::generators::{RegionGenerator, RegionGeneratorIterator};

/// Generate  [`Region`]s in [`RecordBatch`] format
///
/// [`Region`]: tpchgen::generators::Region
///
/// # Example
/// ```
/// # use tpchgen::generators::{RegionGenerator};
/// # use tpchgen_arrow::RegionArrow;
///
/// // Create a SF=1.0 generator and wrap it in an Arrow generator
/// let generator = RegionGenerator::new(1.0, 1, 1);
/// let mut arrow_generator = RegionArrow::new(generator)
///   .with_batch_size(10);
/// // Read the first 10 batches
/// let batch = arrow_generator.next().unwrap().unwrap();
/// // compare the output by pretty printing it
/// let formatted_batches = arrow::util::pretty::pretty_format_batches(&[batch])
///   .unwrap()
///   .to_string();
/// let lines = formatted_batches.lines().collect::<Vec<_>>();
/// assert_eq!(lines, vec![
///   "+-------------+-------------+---------------------------------------------------------------------------------------------------------------------+",
///   "| r_regionkey | r_name      | r_comment                                                                                                           |",
///   "+-------------+-------------+---------------------------------------------------------------------------------------------------------------------+",
///   "| 0           | AFRICA      | lar deposits. blithely final packages cajole. regular waters are final requests. regular accounts are according to  |",
///   "| 1           | AMERICA     | hs use ironic, even requests. s                                                                                     |",
///   "| 2           | ASIA        | ges. thinly even pinto beans ca                                                                                     |",
///   "| 3           | EUROPE      | ly final courts cajole furiously final excuse                                                                       |",
///   "| 4           | MIDDLE EAST | uickly special accounts cajole carefully blithely close requests. carefully final asymptotes haggle furiousl        |",
///   "+-------------+-------------+---------------------------------------------------------------------------------------------------------------------+"
/// ]);
/// ```
pub struct RegionArrow {
    inner: RegionGeneratorIterator<'static>,
    batch_size: usize,
    column_type_config: ColumnTypeConfig,
    /// Cached schema based on column_type_config
    schema: SchemaRef,
}

impl RegionArrow {
    /// Return the schema without initializing a data generator.
    pub fn schema_ref() -> SchemaRef {
        Arc::clone(&REGION_SCHEMA)
    }

    pub fn new(generator: RegionGenerator<'static>) -> Self {
        Self {
            inner: generator.iter(),
            batch_size: DEFAULT_BATCH_SIZE,
            column_type_config: ColumnTypeConfig::default(),
            schema: Arc::clone(&REGION_SCHEMA),
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
            Arc::clone(&REGION_SCHEMA)
        } else {
            make_region_schema(&config)
        };
        self.column_type_config = config;
        self
    }
}

impl RecordBatchReader for RegionArrow {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Iterator for RegionArrow {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        // Get next rows to convert
        let rows: Vec<_> = self.inner.by_ref().take(self.batch_size).collect();
        if rows.is_empty() {
            return None;
        }

        // Build r_regionkey based on config
        let r_regionkey: ArrayRef = match self.column_type_config.regionkey_type {
            KeyColumnType::I32 => Arc::new(Int32Array::from_iter_values(
                rows.iter().map(|r| r.r_regionkey as i32),
            )),
            KeyColumnType::I64 => Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|r| r.r_regionkey),
            )),
        };

        let r_name = StringViewArray::from_iter_values(rows.iter().map(|r| r.r_name));
        let r_comment = StringViewArray::from_iter_values(rows.iter().map(|r| r.r_comment));

        Some(RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![r_regionkey, Arc::new(r_name), Arc::new(r_comment)],
        ))
    }
}

static REGION_SCHEMA: LazyLock<SchemaRef> =
    LazyLock::new(|| make_region_schema(&ColumnTypeConfig::default()));

fn make_region_schema(config: &ColumnTypeConfig) -> SchemaRef {
    let regionkey_type = match config.regionkey_type {
        KeyColumnType::I32 => DataType::Int32,
        KeyColumnType::I64 => DataType::Int64,
    };

    Arc::new(Schema::new(vec![
        Field::new("r_regionkey", regionkey_type, false),
        Field::new("r_name", DataType::Utf8View, false),
        Field::new("r_comment", DataType::Utf8View, false),
    ]))
}
