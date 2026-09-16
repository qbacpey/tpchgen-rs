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

pub struct StoreReturnsArrow {
    inner: RowIter<StoreSalesRowGenerator>,
    batch_size: usize,
    column_type_config: ColumnTypeConfig,
    schema: SchemaRef,
}

impl StoreReturnsArrow {
    /// Return the schema without initializing a data generator.
    pub fn schema_ref() -> SchemaRef {
        Arc::clone(&STORE_RETURNS_SCHEMA)
    }

    pub fn new(session: Session) -> Self {
        let row_count = session.get_scaling().get_row_count(Table::StoreSales);
        Self {
            inner: RowIter::new(StoreSalesRowGenerator::new(), session, row_count),
            batch_size: DEFAULT_BATCH_SIZE,
            column_type_config: ColumnTypeConfig::default(),
            schema: Arc::clone(&STORE_RETURNS_SCHEMA),
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
            Arc::clone(&STORE_RETURNS_SCHEMA)
        } else {
            make_schema(&config)
        };
        self.column_type_config = config;
        self
    }
}

impl RecordBatchReader for StoreReturnsArrow {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Iterator for StoreReturnsArrow {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let rows: Vec<_> = self
            .inner
            .by_ref()
            .filter_map(|g| {
                if let GeneratedRow::StoreReturns(r) = g {
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

        let mut sr_returned_date: Vec<Option<i64>> = Vec::with_capacity(rows.len());
        let mut sr_returned_time: Vec<Option<i64>> = Vec::with_capacity(rows.len());
        let mut sr_item: Vec<Option<i64>> = Vec::with_capacity(rows.len());
        let mut sr_customer: Vec<Option<i64>> = Vec::with_capacity(rows.len());
        let mut sr_cdemo: Vec<Option<i64>> = Vec::with_capacity(rows.len());
        let mut sr_hdemo: Vec<Option<i64>> = Vec::with_capacity(rows.len());
        let mut sr_addr: Vec<Option<i64>> = Vec::with_capacity(rows.len());
        let mut sr_store: Vec<Option<i64>> = Vec::with_capacity(rows.len());
        let mut sr_reason: Vec<Option<i64>> = Vec::with_capacity(rows.len());
        let mut sr_ticket: Vec<Option<i64>> = Vec::with_capacity(rows.len());
        let mut sr_quantity: Vec<Option<i32>> = Vec::with_capacity(rows.len());
        let mut sr_return_amt: Vec<Option<i128>> = Vec::with_capacity(rows.len());
        let mut sr_return_tax: Vec<Option<i128>> = Vec::with_capacity(rows.len());
        let mut sr_return_amt_inc_tax: Vec<Option<i128>> = Vec::with_capacity(rows.len());
        let mut sr_fee: Vec<Option<i128>> = Vec::with_capacity(rows.len());
        let mut sr_return_ship_cost: Vec<Option<i128>> = Vec::with_capacity(rows.len());
        let mut sr_refunded_cash: Vec<Option<i128>> = Vec::with_capacity(rows.len());
        let mut sr_reversed_charge: Vec<Option<i128>> = Vec::with_capacity(rows.len());
        let mut sr_store_credit: Vec<Option<i128>> = Vec::with_capacity(rows.len());
        let mut sr_net_loss: Vec<Option<i128>> = Vec::with_capacity(rows.len());

        for r in &rows {
            let nbm = r.null_bit_map();
            let p = r.get_sr_pricing();
            sr_returned_date.push(sk_opt(nbm, 0, r.get_sr_returned_date_sk()));
            sr_returned_time.push(sk_opt(nbm, 1, r.get_sr_returned_time_sk()));
            sr_item.push(sk_opt(nbm, 2, r.get_sr_item_sk()));
            sr_customer.push(sk_opt(nbm, 3, r.get_sr_customer_sk()));
            sr_cdemo.push(sk_opt(nbm, 4, r.get_sr_cdemo_sk()));
            sr_hdemo.push(sk_opt(nbm, 5, r.get_sr_hdemo_sk()));
            sr_addr.push(sk_opt(nbm, 6, r.get_sr_addr_sk()));
            sr_store.push(sk_opt(nbm, 7, r.get_sr_store_sk()));
            sr_reason.push(sk_opt(nbm, 8, r.get_sr_reason_sk()));
            sr_ticket.push(sk_opt(nbm, 9, r.get_sr_ticket_number()));
            sr_quantity.push(opt(nbm, 10, p.get_quantity()));
            sr_return_amt.push(opt(nbm, 11, decimal_to_i128(p.get_net_paid())));
            sr_return_tax.push(opt(nbm, 12, decimal_to_i128(p.get_ext_tax())));
            sr_return_amt_inc_tax.push(opt(
                nbm,
                13,
                decimal_to_i128(p.get_net_paid_including_tax()),
            ));
            sr_fee.push(opt(nbm, 14, decimal_to_i128(p.get_fee())));
            sr_return_ship_cost.push(opt(nbm, 15, decimal_to_i128(p.get_ext_ship_cost())));
            sr_refunded_cash.push(opt(nbm, 16, decimal_to_i128(p.get_refunded_cash())));
            sr_reversed_charge.push(opt(nbm, 17, decimal_to_i128(p.get_reversed_charge())));
            sr_store_credit.push(opt(nbm, 18, decimal_to_i128(p.get_store_credit())));
            sr_net_loss.push(opt(nbm, 19, decimal_to_i128(p.get_net_loss())));
        }

        let decimal_type = self.column_type_config.decimal_type;
        let dec = |v: Vec<Option<i128>>| decimal_array_from_opt_i128(v, decimal_type);
        let batch = RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(Int64Array::from(sr_returned_date)),
                Arc::new(Int64Array::from(sr_returned_time)),
                Arc::new(Int64Array::from(sr_item)),
                Arc::new(Int64Array::from(sr_customer)),
                Arc::new(Int64Array::from(sr_cdemo)),
                Arc::new(Int64Array::from(sr_hdemo)),
                Arc::new(Int64Array::from(sr_addr)),
                Arc::new(Int64Array::from(sr_store)),
                Arc::new(Int64Array::from(sr_reason)),
                Arc::new(Int64Array::from(sr_ticket)),
                Arc::new(Int32Array::from(sr_quantity)),
                dec(sr_return_amt),
                dec(sr_return_tax),
                dec(sr_return_amt_inc_tax),
                dec(sr_fee),
                dec(sr_return_ship_cost),
                dec(sr_refunded_cash),
                dec(sr_reversed_charge),
                dec(sr_store_credit),
                dec(sr_net_loss),
            ],
        );
        Some(batch)
    }
}

static STORE_RETURNS_SCHEMA: LazyLock<SchemaRef> =
    LazyLock::new(|| make_schema(&ColumnTypeConfig::default()));

fn make_schema(config: &ColumnTypeConfig) -> SchemaRef {
    let decimal_type = decimal_arrow_type(config.decimal_type);

    Arc::new(Schema::new(vec![
        Field::new("sr_returned_date_sk", DataType::Int64, true),
        Field::new("sr_return_time_sk", DataType::Int64, true),
        Field::new("sr_item_sk", DataType::Int64, true),
        Field::new("sr_customer_sk", DataType::Int64, true),
        Field::new("sr_cdemo_sk", DataType::Int64, true),
        Field::new("sr_hdemo_sk", DataType::Int64, true),
        Field::new("sr_addr_sk", DataType::Int64, true),
        Field::new("sr_store_sk", DataType::Int64, true),
        Field::new("sr_reason_sk", DataType::Int64, true),
        Field::new("sr_ticket_number", DataType::Int64, true),
        Field::new("sr_return_quantity", DataType::Int32, true),
        Field::new("sr_return_amt", decimal_type.clone(), true),
        Field::new("sr_return_tax", decimal_type.clone(), true),
        Field::new("sr_return_amt_inc_tax", decimal_type.clone(), true),
        Field::new("sr_fee", decimal_type.clone(), true),
        Field::new("sr_return_ship_cost", decimal_type.clone(), true),
        Field::new("sr_refunded_cash", decimal_type.clone(), true),
        Field::new("sr_reversed_charge", decimal_type.clone(), true),
        Field::new("sr_store_credit", decimal_type.clone(), true),
        Field::new("sr_net_loss", decimal_type, true),
    ]))
}
