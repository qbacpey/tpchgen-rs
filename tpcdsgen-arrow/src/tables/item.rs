use crate::conversions::{
    date_array_from_opt_date32, date_arrow_type, decimal_array_from_opt_i128, decimal_arrow_type,
    is_null, julian_to_date32, opt, sk_opt, string_view_array_from_opt_iter,
};
use crate::{ColumnTypeConfig, RowIter, DEFAULT_BATCH_SIZE};
use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatchReader;
use std::sync::{Arc, LazyLock};
use tpcdsgen::config::{Session, Table};
use tpcdsgen::row::{GeneratedRow, ItemRowGenerator};

pub struct ItemArrow {
    inner: RowIter<ItemRowGenerator>,
    batch_size: usize,
    column_type_config: ColumnTypeConfig,
    schema: SchemaRef,
}

impl ItemArrow {
    /// Return the schema without initializing a data generator.
    pub fn schema_ref() -> SchemaRef {
        Arc::clone(&ITEM_SCHEMA)
    }

    pub fn new(session: Session) -> Self {
        let row_count = session.get_scaling().get_row_count(Table::Item);
        Self {
            inner: RowIter::new(ItemRowGenerator::new(), session, row_count),
            batch_size: DEFAULT_BATCH_SIZE,
            column_type_config: ColumnTypeConfig::default(),
            schema: Arc::clone(&ITEM_SCHEMA),
        }
    }
    pub fn skip_rows_until_starting_row_number(&mut self, starting_row_number: i64) {
        self.inner
            .skip_rows_until_starting_row_number(starting_row_number);
    }

    /// Generate only source rows `starting_row_number..=ending_row_number`
    /// (1-based, inclusive). The ending row number is clamped to the table's
    /// row count.
    pub fn with_source_row_range(
        mut self,
        starting_row_number: i64,
        ending_row_number: i64,
    ) -> Self {
        self.inner
            .set_source_row_range(starting_row_number, ending_row_number);
        self
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    pub fn with_column_type_config(mut self, config: ColumnTypeConfig) -> Self {
        self.schema = if config == ColumnTypeConfig::default() {
            Arc::clone(&ITEM_SCHEMA)
        } else {
            make_schema(&config)
        };
        self.column_type_config = config;
        self
    }
}

impl RecordBatchReader for ItemArrow {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Iterator for ItemArrow {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let rows: Vec<_> = self
            .inner
            .by_ref()
            .map(|g| match g {
                GeneratedRow::Item(r) => r,
                _ => unreachable!(),
            })
            .take(self.batch_size)
            .collect();
        if rows.is_empty() {
            return None;
        }

        let mut i_sk: Vec<Option<i64>> = Vec::with_capacity(rows.len());
        let mut i_id: Vec<Option<String>> = Vec::with_capacity(rows.len());
        let mut i_rec_start: Vec<Option<i32>> = Vec::with_capacity(rows.len());
        let mut i_rec_end: Vec<Option<i32>> = Vec::with_capacity(rows.len());
        let mut i_desc: Vec<Option<String>> = Vec::with_capacity(rows.len());
        let mut i_current_price: Vec<Option<i128>> = Vec::with_capacity(rows.len());
        let mut i_wholesale_cost: Vec<Option<i128>> = Vec::with_capacity(rows.len());
        let mut i_brand_id: Vec<Option<i64>> = Vec::with_capacity(rows.len());
        let mut i_brand: Vec<Option<String>> = Vec::with_capacity(rows.len());
        let mut i_class_id: Vec<Option<i64>> = Vec::with_capacity(rows.len());
        let mut i_class: Vec<Option<String>> = Vec::with_capacity(rows.len());
        let mut i_category_id: Vec<Option<i64>> = Vec::with_capacity(rows.len());
        let mut i_category: Vec<Option<String>> = Vec::with_capacity(rows.len());
        let mut i_manufact_id: Vec<Option<i64>> = Vec::with_capacity(rows.len());
        let mut i_manufact: Vec<Option<String>> = Vec::with_capacity(rows.len());
        let mut i_size: Vec<Option<String>> = Vec::with_capacity(rows.len());
        let mut i_formulation: Vec<Option<String>> = Vec::with_capacity(rows.len());
        let mut i_color: Vec<Option<String>> = Vec::with_capacity(rows.len());
        let mut i_units: Vec<Option<String>> = Vec::with_capacity(rows.len());
        let mut i_container: Vec<Option<String>> = Vec::with_capacity(rows.len());
        let mut i_manager_id: Vec<Option<i64>> = Vec::with_capacity(rows.len());
        let mut i_product_name: Vec<Option<String>> = Vec::with_capacity(rows.len());

        for r in &rows {
            let nbm = r.null_bit_map();
            i_sk.push(sk_opt(nbm, 0, r.get_i_item_sk()));
            i_id.push(opt(nbm, 1, r.get_i_item_id().to_owned()));
            i_rec_start.push(if is_null(nbm, 2) {
                None
            } else {
                julian_to_date32(r.get_i_rec_start_date_id())
            });
            i_rec_end.push(julian_to_date32(r.get_i_rec_end_date_id()));
            i_desc.push(opt(nbm, 4, r.get_i_item_desc().to_owned()));
            i_current_price.push(opt(
                nbm,
                5,
                crate::conversions::decimal_to_i128(r.get_i_current_price()),
            ));
            i_wholesale_cost.push(opt(
                nbm,
                6,
                crate::conversions::decimal_to_i128(r.get_i_wholesale_cost()),
            ));
            i_brand_id.push(opt(nbm, 7, r.get_i_brand_id()));
            i_brand.push(opt(nbm, 8, r.get_i_brand().to_owned()));
            i_class_id.push(opt(nbm, 9, r.get_i_class_id()));
            i_class.push(opt(nbm, 10, r.get_i_class().to_owned()));
            i_category_id.push(opt(nbm, 11, r.get_i_category_id()));
            i_category.push(opt(nbm, 12, r.get_i_category().to_owned()));
            i_manufact_id.push(opt(nbm, 13, r.get_i_manufact_id()));
            i_manufact.push(opt(nbm, 14, r.get_i_manufact().to_owned()));
            i_size.push(opt(nbm, 15, r.get_i_size().to_owned()));
            i_formulation.push(opt(nbm, 16, r.get_i_formulation().to_owned()));
            i_color.push(opt(nbm, 17, r.get_i_color().to_owned()));
            i_units.push(opt(nbm, 18, r.get_i_units().to_owned()));
            i_container.push(opt(nbm, 19, r.get_i_container().to_owned()));
            i_manager_id.push(opt(nbm, 20, r.get_i_manager_id()));
            i_product_name.push(opt(nbm, 21, r.get_i_product_name().to_owned()));
        }

        let decimal_type = self.column_type_config.decimal_type;
        let date_type = self.column_type_config.date_type;

        let batch = RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(Int64Array::from(i_sk)),
                Arc::new(string_view_array_from_opt_iter(
                    i_id.iter().map(|s| s.as_deref()),
                )),
                date_array_from_opt_date32(i_rec_start, date_type),
                date_array_from_opt_date32(i_rec_end, date_type),
                Arc::new(string_view_array_from_opt_iter(
                    i_desc.iter().map(|s| s.as_deref()),
                )),
                decimal_array_from_opt_i128(i_current_price, decimal_type),
                decimal_array_from_opt_i128(i_wholesale_cost, decimal_type),
                Arc::new(Int64Array::from(i_brand_id)),
                Arc::new(string_view_array_from_opt_iter(
                    i_brand.iter().map(|s| s.as_deref()),
                )),
                Arc::new(Int64Array::from(i_class_id)),
                Arc::new(string_view_array_from_opt_iter(
                    i_class.iter().map(|s| s.as_deref()),
                )),
                Arc::new(Int64Array::from(i_category_id)),
                Arc::new(string_view_array_from_opt_iter(
                    i_category.iter().map(|s| s.as_deref()),
                )),
                Arc::new(Int64Array::from(i_manufact_id)),
                Arc::new(string_view_array_from_opt_iter(
                    i_manufact.iter().map(|s| s.as_deref()),
                )),
                Arc::new(string_view_array_from_opt_iter(
                    i_size.iter().map(|s| s.as_deref()),
                )),
                Arc::new(string_view_array_from_opt_iter(
                    i_formulation.iter().map(|s| s.as_deref()),
                )),
                Arc::new(string_view_array_from_opt_iter(
                    i_color.iter().map(|s| s.as_deref()),
                )),
                Arc::new(string_view_array_from_opt_iter(
                    i_units.iter().map(|s| s.as_deref()),
                )),
                Arc::new(string_view_array_from_opt_iter(
                    i_container.iter().map(|s| s.as_deref()),
                )),
                Arc::new(Int64Array::from(i_manager_id)),
                Arc::new(string_view_array_from_opt_iter(
                    i_product_name.iter().map(|s| s.as_deref()),
                )),
            ],
        );
        Some(batch)
    }
}

static ITEM_SCHEMA: LazyLock<SchemaRef> =
    LazyLock::new(|| make_schema(&ColumnTypeConfig::default()));

fn make_schema(config: &ColumnTypeConfig) -> SchemaRef {
    let decimal_type = decimal_arrow_type(config.decimal_type);
    let date_type = date_arrow_type(config.date_type);

    Arc::new(Schema::new(vec![
        Field::new("i_item_sk", DataType::Int64, true),
        Field::new("i_item_id", DataType::Utf8View, true),
        Field::new("i_rec_start_date", date_type.clone(), true),
        Field::new("i_rec_end_date", date_type, true),
        Field::new("i_item_desc", DataType::Utf8View, true),
        Field::new("i_current_price", decimal_type.clone(), true),
        Field::new("i_wholesale_cost", decimal_type, true),
        Field::new("i_brand_id", DataType::Int64, true),
        Field::new("i_brand", DataType::Utf8View, true),
        Field::new("i_class_id", DataType::Int64, true),
        Field::new("i_class", DataType::Utf8View, true),
        Field::new("i_category_id", DataType::Int64, true),
        Field::new("i_category", DataType::Utf8View, true),
        Field::new("i_manufact_id", DataType::Int64, true),
        Field::new("i_manufact", DataType::Utf8View, true),
        Field::new("i_size", DataType::Utf8View, true),
        Field::new("i_formulation", DataType::Utf8View, true),
        Field::new("i_color", DataType::Utf8View, true),
        Field::new("i_units", DataType::Utf8View, true),
        Field::new("i_container", DataType::Utf8View, true),
        Field::new("i_manager_id", DataType::Int64, true),
        Field::new("i_product_name", DataType::Utf8View, true),
    ]))
}
