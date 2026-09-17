//! Turns a day histogram into an output partition layout
//!
//! Every partition covers a contiguous, disjoint range of days, so a reader can
//! skip whole files from the min/max statistics of the sort key. Because the
//! histogram is exact, the boundaries are placed to equalise *rows* rather than
//! days, which matters for `l_shipdate`: it is `o_orderdate` plus 1 to 121
//! days, so the ends of its domain are far sparser than the middle and an
//! equal-day split produces partitions that differ several fold in size.

use crate::histogram::{generated_date, DayHistogram};
use std::fmt::Write as _;

/// One output file: a day range and the exact number of rows it will hold
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Partition {
    /// 1-based file number, used in the file name
    pub number: usize,
    /// First day of the range, inclusive, in generated date units
    pub first_day: i32,
    /// Last day of the range, inclusive, in generated date units
    pub last_day: i32,
    /// Exact number of rows in the range
    pub rows: u64,
}

impl Partition {
    /// Number of days the range spans
    pub fn days(&self) -> usize {
        (self.last_day - self.first_day + 1) as usize
    }
}

/// Splits the populated part of the day domain into `file_count` contiguous
/// ranges holding as close to equal row counts as day granularity allows
///
/// Returns an error if the domain has fewer populated days than files, because
/// a day cannot be split across partitions without breaking the guarantee that
/// partition ranges do not overlap.
pub fn plan(histogram: &DayHistogram, file_count: usize) -> Result<Vec<Partition>, String> {
    let totals = histogram.totals();
    let total_rows: u64 = totals.iter().sum();
    if total_rows == 0 {
        return Err("table has no rows".to_string());
    }
    if file_count == 0 {
        return Err("file count must be at least 1".to_string());
    }

    let first_populated = totals.iter().position(|count| *count > 0).unwrap();
    let last_populated = totals.iter().rposition(|count| *count > 0).unwrap();
    let populated_days = totals[first_populated..=last_populated]
        .iter()
        .filter(|count| **count > 0)
        .count();
    if populated_days < file_count {
        return Err(format!(
            "cannot build {file_count} non-empty partitions from {populated_days} populated days"
        ));
    }

    let mut partitions = Vec::with_capacity(file_count);
    let mut day = first_populated;
    let mut rows_so_far = 0u64;

    for number in 1..=file_count {
        // Close this partition once the running total reaches its share of the
        // table, so rounding never starves the partitions that follow.
        let target = (total_rows as u128 * number as u128 / file_count as u128) as u64;
        let first_day = day;
        let mut rows = 0u64;

        // Always take at least one day, and leave at least one day for each
        // partition still to come.
        let last_available_day = last_populated - (file_count - number);
        while day <= last_available_day {
            rows += totals[day];
            day += 1;
            if rows_so_far + rows >= target && rows > 0 {
                break;
            }
        }
        if number == file_count {
            while day <= last_populated {
                rows += totals[day];
                day += 1;
            }
        }
        rows_so_far += rows;

        partitions.push(Partition {
            number,
            first_day: generated_date(first_day),
            last_day: generated_date(day - 1),
            rows,
        });
    }

    debug_assert_eq!(partitions.iter().map(|p| p.rows).sum::<u64>(), total_rows);
    Ok(partitions)
}

/// Groups partitions into generation passes
///
/// One pass makes a single filtered sweep of the key space and fills every
/// partition in the group, so the number of passes trades memory for the cost
/// of re-deriving sort keys: a pass buffers the rows of its whole group, and
/// the sweeps that skip them cost only their key draws.
pub fn passes(partitions: &[Partition], partitions_per_pass: usize) -> Vec<&[Partition]> {
    partitions.chunks(partitions_per_pass.max(1)).collect()
}

/// Renders the layout as JSON, to be written next to the data as a manifest
pub fn manifest_json(
    table: &str,
    scale_factor: f64,
    sort_key: &str,
    secondary_key: &str,
    partitions: &[Partition],
    to_date_string: impl Fn(i32) -> String,
) -> String {
    let mut json = String::new();
    writeln!(json, "{{").unwrap();
    writeln!(json, "  \"table\": \"{table}\",").unwrap();
    writeln!(json, "  \"scale_factor\": {scale_factor},").unwrap();
    writeln!(json, "  \"partition_key\": \"{sort_key}\",").unwrap();
    writeln!(
        json,
        "  \"sort_columns\": [\"{sort_key}\", \"{secondary_key}\"],"
    )
    .unwrap();
    writeln!(
        json,
        "  \"rows\": {},",
        partitions.iter().map(|p| p.rows).sum::<u64>()
    )
    .unwrap();
    writeln!(json, "  \"partitions\": [").unwrap();
    for (index, partition) in partitions.iter().enumerate() {
        let comma = if index + 1 == partitions.len() {
            ""
        } else {
            ","
        };
        writeln!(
            json,
            "    {{\"file\": \"{table}.{}.parquet\", \"first\": \"{}\", \"last\": \"{}\", \"days\": {}, \"rows\": {}}}{comma}",
            partition.number,
            to_date_string(partition.first_day),
            to_date_string(partition.last_day),
            partition.days(),
            partition.rows,
        )
        .unwrap();
    }
    writeln!(json, "  ]").unwrap();
    writeln!(json, "}}").unwrap();
    json
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::histogram::{day_index, lineitem_histogram, order_histogram};

    fn spans_are_contiguous_and_exact(partitions: &[Partition], histogram: &DayHistogram) {
        let totals = histogram.totals();
        for window in partitions.windows(2) {
            assert_eq!(window[0].last_day + 1, window[1].first_day);
        }
        for partition in partitions {
            let rows: u64 = totals[day_index(partition.first_day)..=day_index(partition.last_day)]
                .iter()
                .sum();
            assert_eq!(rows, partition.rows);
            assert!(partition.rows > 0);
        }
        assert_eq!(
            partitions.iter().map(|p| p.rows).sum::<u64>(),
            histogram.rows()
        );
    }

    #[test]
    fn order_partitions_are_balanced() {
        let histogram = order_histogram(0.1, 2);
        let partitions = plan(&histogram, 8).unwrap();

        assert_eq!(partitions.len(), 8);
        spans_are_contiguous_and_exact(&partitions, &histogram);

        let ideal = histogram.rows() as f64 / 8.0;
        for partition in &partitions {
            let error = (partition.rows as f64 - ideal).abs() / ideal;
            assert!(error < 0.01, "{partition:?} is {error} off {ideal}");
        }
    }

    #[test]
    fn lineitem_partitions_are_balanced_despite_sparse_ends() {
        let histogram = lineitem_histogram(0.1, 2);
        let partitions = plan(&histogram, 16).unwrap();

        assert_eq!(partitions.len(), 16);
        spans_are_contiguous_and_exact(&partitions, &histogram);

        let ideal = histogram.rows() as f64 / 16.0;
        for partition in &partitions {
            let error = (partition.rows as f64 - ideal).abs() / ideal;
            assert!(error < 0.01, "{partition:?} is {error} off {ideal}");
        }

        // The sparse ends of the ship date domain need more days to reach the
        // same row count as the middle.
        assert!(partitions[0].days() > partitions[8].days());
        assert!(partitions[15].days() > partitions[8].days());
    }

    #[test]
    fn too_many_partitions_is_an_error() {
        let histogram = order_histogram(0.01, 1);
        assert!(plan(&histogram, 100_000).is_err());
    }
}
