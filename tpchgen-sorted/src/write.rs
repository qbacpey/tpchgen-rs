//! Fills the planned partitions and writes them as Parquet

use crate::histogram::{self, day_index, DayHistogram};
use crate::layout::{self, Partition};
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};
use tpchgen::dates::TPCHDate;
use tpchgen::generators::{
    LineItem, LineItemGenerator, LineItemShipDateRangeIterator, Order, OrderDateRangeIterator,
    OrderGenerator,
};
use tpchgen_arrow::{lineitem_batch, order_batch, LineItemArrow, OrderArrow};

/// A table that can be generated clustered on a date column
pub trait SortedTable {
    /// The row type the generator produces
    type Row: Send + Sync + Clone;
    /// The iterator that produces the rows of one key-space chunk that fall in
    /// a date range
    type Rows: Iterator<Item = Self::Row>;

    /// Table name, used for the output directory and file names
    const NAME: &'static str;
    /// The date column the output is partitioned and primarily sorted on
    const SORT_KEY: &'static str;
    /// The column the output is sorted on within a date
    const SECONDARY_KEY: &'static str;
    /// Average Parquet bytes per row, used to size row groups. Matches the
    /// estimates `tpcgen-cli` uses for its own row group sizing.
    const PARQUET_BYTES_PER_ROW: u64;

    /// Counts rows per day without generating them
    fn histogram(scale_factor: f64, chunks: usize) -> DayHistogram;

    /// Generates the rows of chunk `part` of `part_count` whose sort key falls
    /// in `first_day..=last_day`
    fn rows(
        scale_factor: f64,
        part: i32,
        part_count: i32,
        first_day: i32,
        last_day: i32,
    ) -> Self::Rows;

    /// The day histogram index of a row's sort key
    fn day(row: &Self::Row) -> usize;

    /// The Arrow schema of the output
    fn schema() -> SchemaRef;

    /// Converts rows to a [`RecordBatch`]
    fn batch(schema: &SchemaRef, rows: &[Self::Row]) -> Result<RecordBatch, ArrowError>;
}

/// The `orders` table, clustered on `o_orderdate`
pub struct Orders;

impl SortedTable for Orders {
    type Row = Order<'static>;
    type Rows = OrderDateRangeIterator<'static>;

    const NAME: &'static str = "orders";
    const SORT_KEY: &'static str = "o_orderdate";
    const SECONDARY_KEY: &'static str = "o_orderkey";
    const PARQUET_BYTES_PER_ROW: u64 = 75;

    fn histogram(scale_factor: f64, chunks: usize) -> DayHistogram {
        histogram::order_histogram(scale_factor, chunks)
    }

    fn rows(
        scale_factor: f64,
        part: i32,
        part_count: i32,
        first_day: i32,
        last_day: i32,
    ) -> Self::Rows {
        OrderGenerator::new(scale_factor, part, part_count)
            .iter()
            .with_order_date_range(first_day, last_day)
    }

    fn day(row: &Self::Row) -> usize {
        row.o_orderdate.into_inner() as usize
    }

    fn schema() -> SchemaRef {
        OrderArrow::schema_ref()
    }

    fn batch(schema: &SchemaRef, rows: &[Self::Row]) -> Result<RecordBatch, ArrowError> {
        order_batch(schema, &Default::default(), rows)
    }
}

/// The `lineitem` table, clustered on `l_shipdate`
pub struct LineItems;

impl SortedTable for LineItems {
    type Row = LineItem<'static>;
    type Rows = LineItemShipDateRangeIterator<'static>;

    const NAME: &'static str = "lineitem";
    const SORT_KEY: &'static str = "l_shipdate";
    const SECONDARY_KEY: &'static str = "l_orderkey";
    const PARQUET_BYTES_PER_ROW: u64 = 64;

    fn histogram(scale_factor: f64, chunks: usize) -> DayHistogram {
        histogram::lineitem_histogram(scale_factor, chunks)
    }

    fn rows(
        scale_factor: f64,
        part: i32,
        part_count: i32,
        first_day: i32,
        last_day: i32,
    ) -> Self::Rows {
        LineItemGenerator::new(scale_factor, part, part_count)
            .iter()
            .with_ship_date_range(first_day, last_day)
    }

    fn day(row: &Self::Row) -> usize {
        row.l_shipdate.into_inner() as usize
    }

    fn schema() -> SchemaRef {
        LineItemArrow::schema_ref()
    }

    fn batch(schema: &SchemaRef, rows: &[Self::Row]) -> Result<RecordBatch, ArrowError> {
        lineitem_batch(schema, &Default::default(), rows)
    }
}

/// Which table to generate
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Table {
    Orders,
    LineItem,
}

impl Table {
    pub fn name(self) -> &'static str {
        match self {
            Table::Orders => Orders::NAME,
            Table::LineItem => LineItems::NAME,
        }
    }
}

/// How to build the dataset
#[derive(Debug, Clone)]
pub struct Options {
    pub scale_factor: f64,
    pub output_dir: PathBuf,
    /// Number of output files, each covering a disjoint range of days
    pub file_count: usize,
    /// Number of key-space chunks generated in parallel
    pub threads: usize,
    /// Number of output files filled by a single sweep of the key space
    pub partitions_per_pass: usize,
    /// Target uncompressed bytes per row group
    pub row_group_bytes: u64,
    /// Rows converted to Arrow at a time
    pub batch_rows: usize,
    pub compression: Compression,
    /// Print the plan without writing any data
    pub plan_only: bool,
}

/// What a run produced
#[derive(Debug, Clone)]
pub struct Report {
    pub partitions: Vec<Partition>,
    pub rows: u64,
    pub bytes: u64,
    pub measure_time: Duration,
    pub fill_time: Duration,
    pub passes: usize,
}

impl Report {
    pub fn total_time(&self) -> Duration {
        self.measure_time + self.fill_time
    }
}

/// Generates a date-clustered, sorted copy of a table
pub fn generate<T: SortedTable>(options: &Options) -> io::Result<Report> {
    let chunks = options.threads.max(1);

    let measure_start = Instant::now();
    let histogram = T::histogram(options.scale_factor, chunks);
    let measure_time = measure_start.elapsed();

    let partitions = layout::plan(&histogram, options.file_count)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
    let table_dir = options.output_dir.join(T::NAME);
    let passes = layout::passes(&partitions, options.partitions_per_pass);
    let pass_count = passes.len();

    if options.plan_only {
        return Ok(Report {
            rows: partitions.iter().map(|p| p.rows).sum(),
            partitions,
            bytes: 0,
            measure_time,
            fill_time: Duration::ZERO,
            passes: pass_count,
        });
    }

    std::fs::create_dir_all(&table_dir)?;
    let schema = T::schema();

    let fill_start = Instant::now();
    let mut bytes = 0u64;
    for group in passes {
        let first_day = group[0].first_day;
        let last_day = group[group.len() - 1].last_day;

        // One sweep of the key space per chunk, bucketing by day. Chunks cover
        // ascending key ranges, so concatenating a day's buckets in chunk order
        // yields the rows of that day in key order.
        let buckets: Vec<Vec<Vec<T::Row>>> = thread::scope(|scope| {
            let handles: Vec<_> = (1..=chunks)
                .map(|chunk| {
                    let histogram = &histogram;
                    scope.spawn(move || {
                        bucket_chunk::<T>(
                            options.scale_factor,
                            chunk as i32,
                            chunks as i32,
                            first_day,
                            last_day,
                            histogram,
                        )
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|handle| handle.join().expect("generation thread panicked"))
                .collect()
        });

        let written = thread::scope(|scope| {
            let handles: Vec<_> = group
                .iter()
                .map(|partition| {
                    let buckets = &buckets;
                    let schema = &schema;
                    let table_dir = table_dir.as_path();
                    scope.spawn(move || {
                        write_partition::<T>(
                            partition, buckets, first_day, schema, table_dir, options,
                        )
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|handle| handle.join().expect("writer thread panicked"))
                .collect::<io::Result<Vec<u64>>>()
        })?;
        bytes += written.iter().sum::<u64>();
    }
    let fill_time = fill_start.elapsed();

    let manifest = layout::manifest_json(
        T::NAME,
        options.scale_factor,
        T::SORT_KEY,
        T::SECONDARY_KEY,
        &partitions,
        |day| TPCHDate::new(day).to_string(),
    );
    std::fs::write(table_dir.join("sort_metadata.json"), manifest)?;

    Ok(Report {
        rows: partitions.iter().map(|p| p.rows).sum(),
        partitions,
        bytes,
        measure_time,
        fill_time,
        passes: pass_count,
    })
}

/// Generates one chunk of the key space, bucketing rows by day
///
/// Bucket capacities come from the histogram, so no bucket ever reallocates.
fn bucket_chunk<T: SortedTable>(
    scale_factor: f64,
    chunk: i32,
    chunk_count: i32,
    first_day: i32,
    last_day: i32,
    histogram: &DayHistogram,
) -> Vec<Vec<T::Row>> {
    let first = day_index(first_day);
    let counts = histogram.chunk(chunk as usize - 1);
    let mut buckets: Vec<Vec<T::Row>> = (first..=day_index(last_day))
        .map(|day| Vec::with_capacity(counts[day] as usize))
        .collect();

    for row in T::rows(scale_factor, chunk, chunk_count, first_day, last_day) {
        buckets[T::day(&row) - first].push(row);
    }
    buckets
}

/// Writes one partition, reading its days out of the per-chunk buckets in
/// order
fn write_partition<T: SortedTable>(
    partition: &Partition,
    buckets: &[Vec<Vec<T::Row>>],
    pass_first_day: i32,
    schema: &SchemaRef,
    table_dir: &Path,
    options: &Options,
) -> io::Result<u64> {
    // Even row groups: the exact row count is known, so the last row group does
    // not have to be a small remainder.
    let target_rows = (options.row_group_bytes / T::PARQUET_BYTES_PER_ROW).max(1);
    let row_groups = partition.rows.div_ceil(target_rows).max(1);
    let row_group_rows = partition.rows.div_ceil(row_groups);

    let properties = WriterProperties::builder()
        .set_compression(options.compression)
        .set_max_row_group_row_count(Some(row_group_rows as usize))
        .build();

    let path = table_dir.join(format!("{}.{}.parquet", T::NAME, partition.number));
    let file = File::create(&path)?;
    let mut writer =
        ArrowWriter::try_new(file, schema.clone(), Some(properties)).map_err(io::Error::other)?;

    let offset = day_index(pass_first_day);
    let mut pending: Vec<T::Row> = Vec::with_capacity(options.batch_rows);
    let mut rows = 0u64;
    for day in day_index(partition.first_day)..=day_index(partition.last_day) {
        for chunk in buckets {
            let segment = &chunk[day - offset];
            for slice in segment.chunks(options.batch_rows) {
                if pending.len() + slice.len() > options.batch_rows && !pending.is_empty() {
                    rows += flush::<T>(&mut writer, schema, &mut pending)?;
                }
                pending.extend_from_slice(slice);
                if pending.len() >= options.batch_rows {
                    rows += flush::<T>(&mut writer, schema, &mut pending)?;
                }
            }
        }
    }
    rows += flush::<T>(&mut writer, schema, &mut pending)?;

    writer.close().map_err(io::Error::other)?;
    if rows != partition.rows {
        return Err(io::Error::other(format!(
            "{}: wrote {rows} rows, planned {}",
            path.display(),
            partition.rows
        )));
    }

    Ok(std::fs::metadata(&path)?.len())
}

fn flush<T: SortedTable>(
    writer: &mut ArrowWriter<File>,
    schema: &SchemaRef,
    pending: &mut Vec<T::Row>,
) -> io::Result<u64> {
    if pending.is_empty() {
        return Ok(0);
    }
    let batch = T::batch(schema, pending).map_err(io::Error::other)?;
    writer.write(&batch).map_err(io::Error::other)?;
    let rows = pending.len() as u64;
    pending.clear();
    Ok(rows)
}
