//! Progress registration shared by the TPC-DS row-generator outputs.
//!
//! The DAT and CSV outputs both drive the row generators directly and pair
//! sales tables with their returns table, so they register progress the same
//! way. Keeping that in one place stops the two from drifting.

use crate::progress::{ProgressHandle, ProgressTracker};
use std::ops::RangeInclusive;
use std::sync::Arc;
use tpcdsgen::config::{Session, Table};

/// Progress handles for one requested table.
///
/// Sales tables are generated together with their returns table, so they
/// register two handles; the returns tables themselves register none.
#[derive(Debug)]
pub(super) enum TableProgress {
    None,
    Single(ProgressHandle),
    Paired {
        sales: ProgressHandle,
        returns: ProgressHandle,
    },
}

/// Number of rows in an inclusive row range. An empty range, e.g. from a
/// chunk that got no rows under the small-table rule, has `end() < start()`
/// and counts as zero.
fn range_len(range: &RangeInclusive<u64>) -> u64 {
    (range.end() + 1).saturating_sub(*range.start())
}

/// Register progress for one requested table.
///
/// Totals reflect this session's chunk: a table split across `--parts` only
/// registers the rows this chunk will actually generate, so progress bars
/// stay accurate when generating one part at a time.
pub(super) fn register_table(
    table: Table,
    session: &Session,
    progress: Arc<dyn ProgressTracker>,
) -> TableProgress {
    let register =
        |table: Table, row_count: u64| progress.clone().register(table.get_name(), row_count);

    // Total rows this chunk will generate for `sales_table`'s source rows.
    let chunk_row_count =
        |sales_table: Table| range_len(&session.get_source_row_range(sales_table));

    // The returns table's own row count is only ever an approximate upper
    // bound for the progress bar (actual returns are data-driven per source
    // row), so scale that full-table estimate down by the same fraction of
    // source rows this chunk covers.
    let chunk_returns_row_count = |sales_table: Table, returns_table: Table| {
        let full_source = session.get_scaling().get_row_count(sales_table);
        if full_source == 0 {
            return 0;
        }
        let full_returns = session.get_scaling().get_row_count(returns_table);
        let chunk_source = chunk_row_count(sales_table);
        // The product exceeds u64 at large scale factors, so scale in u128.
        let scaled = u128::from(full_returns) * u128::from(chunk_source) / u128::from(full_source);
        u64::try_from(scaled).unwrap_or(u64::MAX)
    };

    match table {
        Table::StoreSales => TableProgress::Paired {
            sales: register(Table::StoreSales, chunk_row_count(Table::StoreSales)),
            returns: register(
                Table::StoreReturns,
                chunk_returns_row_count(Table::StoreSales, Table::StoreReturns),
            ),
        },
        Table::CatalogSales => TableProgress::Paired {
            sales: register(Table::CatalogSales, chunk_row_count(Table::CatalogSales)),
            returns: register(
                Table::CatalogReturns,
                chunk_returns_row_count(Table::CatalogSales, Table::CatalogReturns),
            ),
        },
        Table::WebSales => TableProgress::Paired {
            sales: register(Table::WebSales, chunk_row_count(Table::WebSales)),
            returns: register(
                Table::WebReturns,
                chunk_returns_row_count(Table::WebSales, Table::WebReturns),
            ),
        },
        Table::StoreReturns | Table::CatalogReturns | Table::WebReturns => TableProgress::None,
        _ => TableProgress::Single(register(table, chunk_row_count(table))),
    }
}
