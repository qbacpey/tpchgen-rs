use crate::{ColumnTypeConfig, DEFAULT_BATCH_SIZE, KeyColumnType};
use arrow::array::{ArrayRef, Int32Array, Int64Array, RecordBatch, StringViewArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatchReader;
use std::sync::{Arc, LazyLock};
use tpchgen::generators::{NationGenerator, NationGeneratorIterator};

/// Generate  [`Nation`]s in [`RecordBatch`] format
///
/// [`Nation`]: tpchgen::generators::Nation
///
/// # Example
/// ```
/// # use tpchgen::generators::{NationGenerator};
/// # use tpchgen_arrow::NationArrow;
///
/// // Create a SF=1.0 generator and wrap it in an Arrow generator
/// let generator = NationGenerator::new(1.0, 1, 1);
/// let mut arrow_generator = NationArrow::new(generator)
///   .with_batch_size(10);
/// // Read the first 10 batches
/// let batch = arrow_generator.next().unwrap().unwrap();
/// // compare the output by pretty printing it
/// let formatted_batches = arrow::util::pretty::pretty_format_batches(&[batch])
///   .unwrap()
///   .to_string();
/// let lines = formatted_batches.lines().collect::<Vec<_>>();
/// assert_eq!(lines, vec![
///   "+-------------+-----------+-------------+--------------------------------------------------------------------------------------------------------------------+",
///   "| n_nationkey | n_name    | n_regionkey | n_comment                                                                                                          |",
///   "+-------------+-----------+-------------+--------------------------------------------------------------------------------------------------------------------+",
///   "| 0           | ALGERIA   | 0           |  haggle. carefully final deposits detect slyly agai                                                                |",
///   "| 1           | ARGENTINA | 1           | al foxes promise slyly according to the regular accounts. bold requests alon                                       |",
///   "| 2           | BRAZIL    | 1           | y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special         |",
///   "| 3           | CANADA    | 1           | eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold              |",
///   "| 4           | EGYPT     | 4           | y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular d                |",
///   "| 5           | ETHIOPIA  | 0           | ven packages wake quickly. regu                                                                                    |",
///   "| 6           | FRANCE    | 3           | refully final requests. regular, ironi                                                                             |",
///   "| 7           | GERMANY   | 3           | l platelets. regular accounts x-ray: unusual, regular acco                                                         |",
///   "| 8           | INDIA     | 2           | ss excuses cajole slyly across the packages. deposits print aroun                                                  |",
///   "| 9           | INDONESIA | 2           |  slyly express asymptotes. regular deposits haggle slyly. carefully ironic hockey players sleep blithely. carefull |",
///   "+-------------+-----------+-------------+--------------------------------------------------------------------------------------------------------------------+"
/// ]);
/// ```
pub struct NationArrow {
    inner: NationGeneratorIterator<'static>,
    batch_size: usize,
    column_type_config: ColumnTypeConfig,
    /// Cached schema based on column_type_config
    schema: SchemaRef,
}

impl NationArrow {
    /// Return the schema without initializing a data generator.
    pub fn schema_ref() -> SchemaRef {
        Arc::clone(&NATION_SCHEMA)
    }

    pub fn new(generator: NationGenerator<'static>) -> Self {
        Self {
            inner: generator.iter(),
            batch_size: DEFAULT_BATCH_SIZE,
            column_type_config: ColumnTypeConfig::default(),
            schema: Arc::clone(&NATION_SCHEMA),
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
            Arc::clone(&NATION_SCHEMA)
        } else {
            make_nation_schema(&config)
        };
        self.column_type_config = config;
        self
    }
}

impl RecordBatchReader for NationArrow {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Iterator for NationArrow {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        // Get next rows to convert
        let rows: Vec<_> = self.inner.by_ref().take(self.batch_size).collect();
        if rows.is_empty() {
            return None;
        }

        // Build n_nationkey based on config
        let n_nationkey: ArrayRef = match self.column_type_config.nationkey_type {
            KeyColumnType::I32 => Arc::new(Int32Array::from_iter_values(
                rows.iter().map(|r| r.n_nationkey as i32),
            )),
            KeyColumnType::I64 => Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|r| r.n_nationkey),
            )),
        };

        let n_name = StringViewArray::from_iter_values(rows.iter().map(|r| r.n_name));

        // Build n_regionkey based on config
        let n_regionkey: ArrayRef = match self.column_type_config.regionkey_type {
            KeyColumnType::I32 => Arc::new(Int32Array::from_iter_values(
                rows.iter().map(|r| r.n_regionkey as i32),
            )),
            KeyColumnType::I64 => Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|r| r.n_regionkey),
            )),
        };

        let n_comment = StringViewArray::from_iter_values(rows.iter().map(|r| r.n_comment));

        Some(RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                n_nationkey,
                Arc::new(n_name),
                n_regionkey,
                Arc::new(n_comment),
            ],
        ))
    }
}

static NATION_SCHEMA: LazyLock<SchemaRef> =
    LazyLock::new(|| make_nation_schema(&ColumnTypeConfig::default()));

fn make_nation_schema(config: &ColumnTypeConfig) -> SchemaRef {
    let nationkey_type = match config.nationkey_type {
        KeyColumnType::I32 => DataType::Int32,
        KeyColumnType::I64 => DataType::Int64,
    };
    let regionkey_type = match config.regionkey_type {
        KeyColumnType::I32 => DataType::Int32,
        KeyColumnType::I64 => DataType::Int64,
    };

    Arc::new(Schema::new(vec![
        Field::new("n_nationkey", nationkey_type, false),
        Field::new("n_name", DataType::Utf8View, false),
        Field::new("n_regionkey", regionkey_type, false),
        Field::new("n_comment", DataType::Utf8View, false),
    ]))
}
