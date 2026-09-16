use crate::conversions::{
    decimal_array_from_opt_i128, decimal_arrow_type, decimal_to_i128, opt, sk_opt,
};
use crate::{ColumnTypeConfig, RowIter, DEFAULT_BATCH_SIZE};
use arrow::array::{Int32Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatchReader;
use std::sync::{Arc, LazyLock};
use tpcdsgen::config::{Session, Table};
use tpcdsgen::row::{GeneratedRow, StoreSalesRowGenerator};

pub struct StoreSalesArrow {
    inner: RowIter<StoreSalesRowGenerator>,
    batch_size: usize,
    column_type_config: ColumnTypeConfig,
    schema: SchemaRef,
}

impl StoreSalesArrow {
    /// Return the schema without initializing a data generator.
    pub fn schema_ref() -> SchemaRef {
        Arc::clone(&STORE_SALES_SCHEMA)
    }

    pub fn new(session: Session) -> Self {
        let row_count = session.get_scaling().get_row_count(Table::StoreSales);
        Self {
            inner: RowIter::new(StoreSalesRowGenerator::new(), session, row_count),
            batch_size: DEFAULT_BATCH_SIZE,
            column_type_config: ColumnTypeConfig::default(),
            schema: Arc::clone(&STORE_SALES_SCHEMA),
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
            Arc::clone(&STORE_SALES_SCHEMA)
        } else {
            make_schema(&config)
        };
        self.column_type_config = config;
        self
    }
}

impl RecordBatchReader for StoreSalesArrow {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Iterator for StoreSalesArrow {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let rows: Vec<_> = self
            .inner
            .by_ref()
            .filter_map(|g| {
                if let GeneratedRow::StoreSales(r) = g {
                    Some(r)
                } else {
                    None
                }
            })
            .take(self.batch_size)
            .collect();
        if rows.is_empty() {
            return None;
        }

        let mut ss_sold_date: Vec<Option<i64>> = Vec::with_capacity(rows.len());
        let mut ss_sold_time: Vec<Option<i64>> = Vec::with_capacity(rows.len());
        let mut ss_item: Vec<Option<i64>> = Vec::with_capacity(rows.len());
        let mut ss_customer: Vec<Option<i64>> = Vec::with_capacity(rows.len());
        let mut ss_cdemo: Vec<Option<i64>> = Vec::with_capacity(rows.len());
        let mut ss_hdemo: Vec<Option<i64>> = Vec::with_capacity(rows.len());
        let mut ss_addr: Vec<Option<i64>> = Vec::with_capacity(rows.len());
        let mut ss_store: Vec<Option<i64>> = Vec::with_capacity(rows.len());
        let mut ss_promo: Vec<Option<i64>> = Vec::with_capacity(rows.len());
        let mut ss_ticket: Vec<Option<i64>> = Vec::with_capacity(rows.len());
        let mut ss_quantity: Vec<Option<i32>> = Vec::with_capacity(rows.len());
        let mut ss_wholesale_cost: Vec<Option<i128>> = Vec::with_capacity(rows.len());
        let mut ss_list_price: Vec<Option<i128>> = Vec::with_capacity(rows.len());
        let mut ss_sales_price: Vec<Option<i128>> = Vec::with_capacity(rows.len());
        let mut ss_ext_discount_amt: Vec<Option<i128>> = Vec::with_capacity(rows.len());
        let mut ss_ext_sales_price: Vec<Option<i128>> = Vec::with_capacity(rows.len());
        let mut ss_ext_wholesale_cost: Vec<Option<i128>> = Vec::with_capacity(rows.len());
        let mut ss_ext_list_price: Vec<Option<i128>> = Vec::with_capacity(rows.len());
        let mut ss_ext_tax: Vec<Option<i128>> = Vec::with_capacity(rows.len());
        let mut ss_coupon_amt: Vec<Option<i128>> = Vec::with_capacity(rows.len());
        let mut ss_net_paid: Vec<Option<i128>> = Vec::with_capacity(rows.len());
        let mut ss_net_paid_inc_tax: Vec<Option<i128>> = Vec::with_capacity(rows.len());
        let mut ss_net_profit: Vec<Option<i128>> = Vec::with_capacity(rows.len());

        for r in &rows {
            let nbm = r.null_bit_map();
            let p = r.get_ss_pricing();
            ss_sold_date.push(sk_opt(nbm, 0, r.get_ss_sold_date_sk()));
            ss_sold_time.push(sk_opt(nbm, 1, r.get_ss_sold_time_sk()));
            ss_item.push(sk_opt(nbm, 2, r.get_ss_sold_item_sk()));
            ss_customer.push(sk_opt(nbm, 3, r.get_ss_sold_customer_sk()));
            ss_cdemo.push(sk_opt(nbm, 4, r.get_ss_sold_cdemo_sk()));
            ss_hdemo.push(sk_opt(nbm, 5, r.get_ss_sold_hdemo_sk()));
            ss_addr.push(sk_opt(nbm, 6, r.get_ss_sold_addr_sk()));
            ss_store.push(sk_opt(nbm, 7, r.get_ss_sold_store_sk()));
            ss_promo.push(sk_opt(nbm, 8, r.get_ss_sold_promo_sk()));
            ss_ticket.push(sk_opt(nbm, 9, r.get_ss_ticket_number()));
            ss_quantity.push(opt(nbm, 10, p.get_quantity()));
            ss_wholesale_cost.push(opt(nbm, 11, decimal_to_i128(p.get_wholesale_cost())));
            ss_list_price.push(opt(nbm, 12, decimal_to_i128(p.get_list_price())));
            ss_sales_price.push(opt(nbm, 13, decimal_to_i128(p.get_sales_price())));
            // Java bug: coupon_amount appears at position 14 instead of ext_discount_amount
            ss_ext_discount_amt.push(opt(nbm, 14, decimal_to_i128(p.get_coupon_amount())));
            ss_ext_sales_price.push(opt(nbm, 15, decimal_to_i128(p.get_ext_sales_price())));
            ss_ext_wholesale_cost.push(opt(nbm, 16, decimal_to_i128(p.get_ext_wholesale_cost())));
            ss_ext_list_price.push(opt(nbm, 17, decimal_to_i128(p.get_ext_list_price())));
            ss_ext_tax.push(opt(nbm, 18, decimal_to_i128(p.get_ext_tax())));
            // Java bug: coupon_amount appears again at position 19
            ss_coupon_amt.push(opt(nbm, 14, decimal_to_i128(p.get_coupon_amount())));
            ss_net_paid.push(opt(nbm, 19, decimal_to_i128(p.get_net_paid())));
            ss_net_paid_inc_tax.push(opt(
                nbm,
                20,
                decimal_to_i128(p.get_net_paid_including_tax()),
            ));
            ss_net_profit.push(opt(nbm, 21, decimal_to_i128(p.get_net_profit())));
        }

        let decimal_type = self.column_type_config.decimal_type;
        let dec = |v: Vec<Option<i128>>| decimal_array_from_opt_i128(v, decimal_type);
        let batch = RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(Int64Array::from(ss_sold_date)),
                Arc::new(Int64Array::from(ss_sold_time)),
                Arc::new(Int64Array::from(ss_item)),
                Arc::new(Int64Array::from(ss_customer)),
                Arc::new(Int64Array::from(ss_cdemo)),
                Arc::new(Int64Array::from(ss_hdemo)),
                Arc::new(Int64Array::from(ss_addr)),
                Arc::new(Int64Array::from(ss_store)),
                Arc::new(Int64Array::from(ss_promo)),
                Arc::new(Int64Array::from(ss_ticket)),
                Arc::new(Int32Array::from(ss_quantity)),
                dec(ss_wholesale_cost),
                dec(ss_list_price),
                dec(ss_sales_price),
                dec(ss_ext_discount_amt),
                dec(ss_ext_sales_price),
                dec(ss_ext_wholesale_cost),
                dec(ss_ext_list_price),
                dec(ss_ext_tax),
                dec(ss_coupon_amt),
                dec(ss_net_paid),
                dec(ss_net_paid_inc_tax),
                dec(ss_net_profit),
            ],
        );
        Some(batch)
    }
}

static STORE_SALES_SCHEMA: LazyLock<SchemaRef> =
    LazyLock::new(|| make_schema(&ColumnTypeConfig::default()));

fn make_schema(config: &ColumnTypeConfig) -> SchemaRef {
    let decimal_type = decimal_arrow_type(config.decimal_type);

    Arc::new(Schema::new(vec![
        Field::new("ss_sold_date_sk", DataType::Int64, true),
        Field::new("ss_sold_time_sk", DataType::Int64, true),
        Field::new("ss_item_sk", DataType::Int64, true),
        Field::new("ss_customer_sk", DataType::Int64, true),
        Field::new("ss_cdemo_sk", DataType::Int64, true),
        Field::new("ss_hdemo_sk", DataType::Int64, true),
        Field::new("ss_addr_sk", DataType::Int64, true),
        Field::new("ss_store_sk", DataType::Int64, true),
        Field::new("ss_promo_sk", DataType::Int64, true),
        Field::new("ss_ticket_number", DataType::Int64, true),
        Field::new("ss_quantity", DataType::Int32, true),
        Field::new("ss_wholesale_cost", decimal_type.clone(), true),
        Field::new("ss_list_price", decimal_type.clone(), true),
        Field::new("ss_sales_price", decimal_type.clone(), true),
        Field::new("ss_ext_discount_amt", decimal_type.clone(), true),
        Field::new("ss_ext_sales_price", decimal_type.clone(), true),
        Field::new("ss_ext_wholesale_cost", decimal_type.clone(), true),
        Field::new("ss_ext_list_price", decimal_type.clone(), true),
        Field::new("ss_ext_tax", decimal_type.clone(), true),
        Field::new("ss_coupon_amt", decimal_type.clone(), true),
        Field::new("ss_net_paid", decimal_type.clone(), true),
        Field::new("ss_net_paid_inc_tax", decimal_type.clone(), true),
        Field::new("ss_net_profit", decimal_type, true),
    ]))
}
