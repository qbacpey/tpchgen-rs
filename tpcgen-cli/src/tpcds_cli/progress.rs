//! Progress registration shared by the TPC-DS row-generator outputs.
//!
//! The DAT and CSV outputs both drive the row generators directly and pair
//! sales tables with their returns table, so they register progress the same
//! way. Keeping that in one place stops the two from drifting.

use crate::progress::{ProgressHandle, ProgressTracker};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tpcdsgen::config::{Session, Table};

/// Progress handles for one requested table.
///
/// Sales tables are generated together with their returns table, so they
/// register two handles; the returns tables themselves register none.
#[derive(Debug, Clone)]
pub(super) enum TableProgress {
    None,
    Single(ProgressHandle),
    Paired {
        sales: ProgressHandle,
        returns: ProgressHandle,
    },
}

/// Register progress for one requested table, sized to its full row count.
///
/// A returns table's row count is only ever an approximate upper bound (actual
/// returns are data-driven per source row).
///
/// The total is independent of `--parts`: callers generating a table across
/// several parts register it once, then split the result across parts with
/// [`share_across_parts`] so `--parts` gets one bar per table, not one per
/// part.
pub(super) fn register_table(
    table: Table,
    session: &Session,
    progress: Arc<dyn ProgressTracker>,
) -> TableProgress {
    let register = |table: Table, row_count: i64| {
        // Row counts are always non negative, so this conversion never fails.
        // Clamp rather than panic if that ever changes: a wrong progress total
        // should not abort generation.
        debug_assert!(
            row_count >= 0,
            "negative row count for {}: {row_count}",
            table.get_name()
        );
        let row_count = u64::try_from(row_count).unwrap_or(0);
        progress.clone().register(table.get_name(), row_count)
    };
    let full_row_count = |table: Table| session.get_scaling().get_row_count(table);

    match table {
        Table::StoreSales => TableProgress::Paired {
            sales: register(Table::StoreSales, full_row_count(Table::StoreSales)),
            returns: register(Table::StoreReturns, full_row_count(Table::StoreReturns)),
        },
        Table::CatalogSales => TableProgress::Paired {
            sales: register(Table::CatalogSales, full_row_count(Table::CatalogSales)),
            returns: register(Table::CatalogReturns, full_row_count(Table::CatalogReturns)),
        },
        Table::WebSales => TableProgress::Paired {
            sales: register(Table::WebSales, full_row_count(Table::WebSales)),
            returns: register(Table::WebReturns, full_row_count(Table::WebReturns)),
        },
        Table::StoreReturns | Table::CatalogReturns | Table::WebReturns => TableProgress::None,
        _ => TableProgress::Single(register(table, full_row_count(table))),
    }
}

/// Split one registered handle into `num_parts` clones that all report to the
/// same bar.
///
/// Each part finishes independently and calls [`ProgressHandle::complete`]
/// on its own; gate that call so the bar only reaches its "done" style once
/// every part has completed, not after the first one.
pub(super) fn share_handle_across_parts(
    handle: ProgressHandle,
    num_parts: usize,
) -> Vec<ProgressHandle> {
    let remaining = Arc::new(AtomicUsize::new(num_parts.max(1)));
    (0..num_parts.max(1))
        .map(|_| {
            let remaining = remaining.clone();
            let increment_handle = handle.clone();
            let complete_handle = handle.clone();
            ProgressHandle::new_with_complete(
                move |units| increment_handle.increment(units),
                move || {
                    if remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
                        complete_handle.complete();
                    }
                },
            )
        })
        .collect()
}

/// [`share_handle_across_parts`] applied to every handle a [`TableProgress`]
/// holds, so a paired sales/returns table shares each half independently.
pub(super) fn share_across_parts(progress: TableProgress, num_parts: usize) -> Vec<TableProgress> {
    match progress {
        TableProgress::None => (0..num_parts.max(1)).map(|_| TableProgress::None).collect(),
        TableProgress::Single(handle) => share_handle_across_parts(handle, num_parts)
            .into_iter()
            .map(TableProgress::Single)
            .collect(),
        TableProgress::Paired { sales, returns } => share_handle_across_parts(sales, num_parts)
            .into_iter()
            .zip(share_handle_across_parts(returns, num_parts))
            .map(|(sales, returns)| TableProgress::Paired { sales, returns })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU64;
    use std::sync::Mutex;
    use tpcdsgen::config::SessionBuilder;

    #[derive(Debug, Default)]
    struct RecordingProgress {
        registered: Mutex<Vec<(String, u64)>>,
    }

    impl ProgressTracker for RecordingProgress {
        fn register(self: Arc<Self>, item: &str, total_units: u64) -> ProgressHandle {
            self.registered
                .lock()
                .unwrap()
                .push((item.to_owned(), total_units));
            ProgressHandle::new(|_| {})
        }
    }

    /// A handle plus counters for what it observed, for asserting on
    /// [`share_handle_across_parts`]'s forwarding behavior.
    fn recording_handle() -> (ProgressHandle, Arc<AtomicU64>, Arc<AtomicUsize>) {
        let increments = Arc::new(AtomicU64::new(0));
        let completions = Arc::new(AtomicUsize::new(0));
        let handle = {
            let increments = increments.clone();
            let completions = completions.clone();
            ProgressHandle::new_with_complete(
                move |units| {
                    increments.fetch_add(units, Ordering::Relaxed);
                },
                move || {
                    completions.fetch_add(1, Ordering::Relaxed);
                },
            )
        };
        (handle, increments, completions)
    }

    #[test]
    fn register_table_total_ignores_chunking() {
        // A table's registered total must be the same whether the session
        // represents the whole table or one `--parts` chunk of it: parts
        // share one bar sized to the full table, not a per-chunk total.
        let tracker = Arc::new(RecordingProgress::default());
        let whole = SessionBuilder::new()
            .with_scale_factor(1.0)
            .build()
            .unwrap();
        let one_of_four = SessionBuilder::new()
            .with_scale_factor(1.0)
            .with_chunk_number(2)
            .with_total_chunks(4)
            .with_partitioned(true)
            .build()
            .unwrap();

        register_table(Table::Reason, &whole, tracker.clone());
        register_table(Table::Reason, &one_of_four, tracker.clone());

        let registered = tracker.registered.lock().unwrap();
        assert_eq!(registered[0], registered[1]);
    }

    #[test]
    fn share_handle_across_parts_forwards_every_increment() {
        let (handle, increments, _completions) = recording_handle();
        let parts = share_handle_across_parts(handle, 4);
        assert_eq!(parts.len(), 4);

        for part in &parts {
            part.increment(1);
        }
        assert_eq!(increments.load(Ordering::Relaxed), 4);
    }

    #[test]
    fn share_handle_across_parts_completes_only_after_every_part_completes() {
        let (handle, _increments, completions) = recording_handle();
        let parts = share_handle_across_parts(handle, 3);

        parts[0].complete();
        parts[1].complete();
        assert_eq!(
            completions.load(Ordering::Relaxed),
            0,
            "must not finish the bar before the last part reports completion"
        );

        parts[2].complete();
        assert_eq!(completions.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn share_across_parts_gates_sales_and_returns_independently() {
        let (sales_handle, _sales_inc, sales_completions) = recording_handle();
        let (returns_handle, _returns_inc, returns_completions) = recording_handle();
        let progress = TableProgress::Paired {
            sales: sales_handle,
            returns: returns_handle,
        };

        let mut parts = share_across_parts(progress, 2).into_iter();
        let (
            TableProgress::Paired {
                sales: sales1,
                returns: returns1,
            },
            TableProgress::Paired {
                sales: sales2,
                returns: returns2,
            },
        ) = (parts.next().unwrap(), parts.next().unwrap())
        else {
            panic!("expected two paired parts");
        };

        sales1.complete();
        returns1.complete();
        assert_eq!(sales_completions.load(Ordering::Relaxed), 0);
        assert_eq!(returns_completions.load(Ordering::Relaxed), 0);

        sales2.complete();
        assert_eq!(sales_completions.load(Ordering::Relaxed), 1);
        assert_eq!(
            returns_completions.load(Ordering::Relaxed),
            0,
            "sales and returns gates are independent"
        );

        returns2.complete();
        assert_eq!(returns_completions.load(Ordering::Relaxed), 1);
    }
}
