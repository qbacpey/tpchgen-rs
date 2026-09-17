//! Key-only scans that count rows per day without generating them
//!
//! The sort keys of the date-clustered tables are cheap to derive:
//!
//! * `o_orderdate` is one draw from a stream that no other column reads
//! * `l_shipdate` is `o_orderdate` plus one draw, once per line of the order,
//!   and the line count is one more draw
//!
//! Replaying just those streams costs a handful of multiplications per row,
//! against the strings, decimals and simulated line items a full row needs, so
//! the exact row count of every day in the dataset can be measured in a
//! fraction of the time it takes to generate it.

use std::thread;
use tpchgen::dates::{GenerateUtils, MIN_GENERATE_DATE, TOTAL_DATE_RANGE};
use tpchgen::generators::{LineItemGenerator, OrderGenerator};

/// Number of distinct days a TPC-H date column can hold
pub const DAY_COUNT: usize = TOTAL_DATE_RANGE as usize;

/// Converts a generated date to an index into a day histogram
#[inline]
pub fn day_index(generated_date: i32) -> usize {
    (generated_date - MIN_GENERATE_DATE) as usize
}

/// Converts a day histogram index back to a generated date
#[inline]
pub fn generated_date(day_index: usize) -> i32 {
    day_index as i32 + MIN_GENERATE_DATE
}

/// Rows per day, measured per key-space chunk
///
/// `chunks[c][d]` is the number of rows chunk `c` contributes to day `d`, which
/// is both the exact capacity a bucket needs during generation and, summed over
/// chunks, the exact size of each output partition.
#[derive(Debug, Clone)]
pub struct DayHistogram {
    chunks: Vec<Vec<u64>>,
    totals: Vec<u64>,
}

impl DayHistogram {
    fn new(chunks: Vec<Vec<u64>>) -> Self {
        let mut totals = vec![0u64; DAY_COUNT];
        for chunk in &chunks {
            for (total, count) in totals.iter_mut().zip(chunk) {
                *total += count;
            }
        }
        Self { chunks, totals }
    }

    /// Rows per day, summed over every chunk
    pub fn totals(&self) -> &[u64] {
        &self.totals
    }

    /// Rows chunk `chunk` contributes to each day
    pub fn chunk(&self, chunk: usize) -> &[u64] {
        &self.chunks[chunk]
    }

    /// Number of key-space chunks measured
    pub fn chunk_count(&self) -> usize {
        self.chunks.len()
    }

    /// Total rows in the table
    pub fn rows(&self) -> u64 {
        self.totals.iter().sum()
    }

    /// Rows chunk `chunk` contributes to `first_day..=last_day`
    pub fn chunk_rows_in(&self, chunk: usize, first_day: i32, last_day: i32) -> u64 {
        self.chunks[chunk][day_index(first_day)..=day_index(last_day)]
            .iter()
            .sum()
    }
}

/// Counts `o_orderdate` per day over one chunk of the order key space
fn order_day_counts(scale_factor: f64, part: i32, part_count: i32) -> Vec<u64> {
    let start_index = GenerateUtils::calculate_start_index(
        OrderGenerator::SCALE_BASE,
        scale_factor,
        part,
        part_count,
    );
    let row_count = OrderGenerator::calculate_row_count(scale_factor, part, part_count);

    let mut order_date_random = OrderGenerator::create_order_date_random();
    order_date_random.advance_rows(start_index);

    let mut counts = vec![0u64; DAY_COUNT];
    for _ in 0..row_count {
        let order_date = order_date_random.next_value();
        counts[day_index(order_date)] += 1;
        order_date_random.row_finished();
    }
    counts
}

/// Counts `l_shipdate` per day over one chunk of the order key space
///
/// Line items are partitioned by order, exactly as [`LineItemGenerator`] is, so
/// a chunk holds every line of the orders it covers.
fn ship_day_counts(scale_factor: f64, part: i32, part_count: i32) -> Vec<u64> {
    let start_index = GenerateUtils::calculate_start_index(
        OrderGenerator::SCALE_BASE,
        scale_factor,
        part,
        part_count,
    );
    let order_count = OrderGenerator::calculate_row_count(scale_factor, part, part_count);

    let mut order_date_random = OrderGenerator::create_order_date_random();
    let mut line_count_random = OrderGenerator::create_line_count_random();
    let mut ship_date_random = LineItemGenerator::create_ship_date_random();
    order_date_random.advance_rows(start_index);
    line_count_random.advance_rows(start_index);
    ship_date_random.advance_rows(start_index);

    let mut counts = vec![0u64; DAY_COUNT];
    for _ in 0..order_count {
        let order_date = order_date_random.next_value();
        let line_count = line_count_random.next_value();
        for _ in 0..line_count {
            let ship_date = ship_date_random.next_value() + order_date;
            counts[day_index(ship_date)] += 1;
        }
        order_date_random.row_finished();
        line_count_random.row_finished();
        ship_date_random.row_finished();
    }
    counts
}

/// Measures rows per day for a table, splitting the key space into `chunks`
/// pieces counted in parallel
pub fn measure(
    counts: fn(f64, i32, i32) -> Vec<u64>,
    scale_factor: f64,
    chunks: usize,
) -> DayHistogram {
    let chunk_count = chunks.max(1) as i32;
    let measured = thread::scope(|scope| {
        let handles: Vec<_> = (1..=chunk_count)
            .map(|part| scope.spawn(move || counts(scale_factor, part, chunk_count)))
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().expect("histogram thread panicked"))
            .collect()
    });
    DayHistogram::new(measured)
}

/// The per-chunk `o_orderdate` histogram of the `orders` table
pub fn order_histogram(scale_factor: f64, chunks: usize) -> DayHistogram {
    measure(order_day_counts, scale_factor, chunks)
}

/// The per-chunk `l_shipdate` histogram of the `lineitem` table
pub fn lineitem_histogram(scale_factor: f64, chunks: usize) -> DayHistogram {
    measure(ship_day_counts, scale_factor, chunks)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tpchgen::generators::{LineItemGenerator, OrderGenerator};

    #[test]
    fn order_histogram_matches_generated_rows() {
        let histogram = order_histogram(0.1, 3);
        let mut expected = vec![0u64; DAY_COUNT];
        for order in OrderGenerator::new(0.1, 1, 1).iter() {
            expected[order.o_orderdate.into_inner() as usize] += 1;
        }

        assert_eq!(histogram.totals(), expected.as_slice());
        assert_eq!(histogram.rows(), expected.iter().sum::<u64>());
    }

    #[test]
    fn lineitem_histogram_matches_generated_rows() {
        let histogram = lineitem_histogram(0.1, 3);
        let mut expected = vec![0u64; DAY_COUNT];
        for line in LineItemGenerator::new(0.1, 1, 1).iter() {
            expected[line.l_shipdate.into_inner() as usize] += 1;
        }

        assert_eq!(histogram.totals(), expected.as_slice());
    }

    #[test]
    fn ship_dates_are_not_uniform_over_the_domain() {
        // l_shipdate is o_orderdate plus 1 to 121 days, so the first and last
        // days of the domain hold a fraction of the rows an interior day does.
        // Splitting the domain into equal day counts therefore does not split
        // the table into equal row counts.
        let totals = lineitem_histogram(0.1, 2).totals().to_vec();
        let populated: Vec<u64> = totals.iter().copied().filter(|count| *count > 0).collect();
        let interior = populated[populated.len() / 2];

        assert!(populated[0] * 10 < interior);
        assert!(populated[populated.len() - 1] * 10 < interior);
    }
}
