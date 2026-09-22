//! Fills the planned partitions and writes them as Parquet
//!
//! Output goes through [`generate_parquet`], the same parallel column-chunk
//! encoder `tpcgen-cli` uses, so a sorted dataset differs from the unsorted
//! baseline only in row order, and each file's row groups are encoded across
//! all worker threads.

use crate::histogram::{self, day_index, DayHistogram};
use crate::layout::{self, Partition};
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatchReader;
use parquet::basic::{Compression, Encoding};
use parquet::file::metadata::SortingColumn;
use std::fs::File;
use std::io::{self, BufWriter};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tpcgen_cli::parquet::{generate_parquet, ParquetVersion};
use tpcgen_cli::progress::ProgressHandle;
use tpchgen::dates::TPCHDate;
use tpchgen::generators::{
    LineItem, LineItemGenerator, LineItemShipDateRangeIterator, Order, OrderDateRangeIterator,
    OrderGenerator,
};
use tpchgen_arrow::{lineitem_batch, order_batch, ColumnTypeConfig, LineItemArrow, OrderArrow};

/// A table that can be generated clustered on a date column
///
/// The `'static` bound lets row groups own their rows: `generate_parquet`
/// encodes each row group on its own task.
pub trait SortedTable: 'static {
    /// The row type the generator produces
    type Row: Send + Sync + Clone + 'static;
    /// The iterator that produces the rows of one key-space chunk that fall in
    /// a date range
    type Rows: Iterator<Item = Self::Row>;

    /// Table name, used for the output directory and file names
    const NAME: &'static str;
    /// The date column the output is partitioned and primarily sorted on
    const SORT_KEY: &'static str;
    /// The column the output is sorted on within a date
    const SECONDARY_KEY: &'static str;
    /// Parquet column index of `SORT_KEY`
    const SORT_KEY_COLUMN: i32;
    /// Parquet column index of `SECONDARY_KEY`
    const SECONDARY_KEY_COLUMN: i32;
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

    /// The Arrow schema of the output for a column type configuration
    fn schema_for(config: &ColumnTypeConfig) -> SchemaRef;

    /// Converts rows to a [`RecordBatch`]
    fn batch(
        schema: &SchemaRef,
        config: &ColumnTypeConfig,
        rows: &[Self::Row],
    ) -> Result<RecordBatch, ArrowError>;

    /// The sort order declared in the Parquet file metadata: ascending on the
    /// date, then the key. The columns are not nullable, so `nulls_first` is
    /// irrelevant.
    fn sorting_columns() -> [SortingColumn; 2] {
        [
            SortingColumn {
                column_idx: Self::SORT_KEY_COLUMN,
                descending: false,
                nulls_first: false,
            },
            SortingColumn {
                column_idx: Self::SECONDARY_KEY_COLUMN,
                descending: false,
                nulls_first: false,
            },
        ]
    }
}

/// The `orders` table, clustered on `o_orderdate`
pub struct Orders;

impl SortedTable for Orders {
    type Row = Order<'static>;
    type Rows = OrderDateRangeIterator<'static>;

    const NAME: &'static str = "orders";
    const SORT_KEY: &'static str = "o_orderdate";
    const SECONDARY_KEY: &'static str = "o_orderkey";
    const SORT_KEY_COLUMN: i32 = 4;
    const SECONDARY_KEY_COLUMN: i32 = 0;
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

    fn schema_for(config: &ColumnTypeConfig) -> SchemaRef {
        OrderArrow::schema_for(config)
    }

    fn batch(
        schema: &SchemaRef,
        config: &ColumnTypeConfig,
        rows: &[Self::Row],
    ) -> Result<RecordBatch, ArrowError> {
        order_batch(schema, config, rows)
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
    const SORT_KEY_COLUMN: i32 = 10;
    const SECONDARY_KEY_COLUMN: i32 = 0;
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

    fn schema_for(config: &ColumnTypeConfig) -> SchemaRef {
        LineItemArrow::schema_for(config)
    }

    fn batch(
        schema: &SchemaRef,
        config: &ColumnTypeConfig,
        rows: &[Self::Row],
    ) -> Result<RecordBatch, ArrowError> {
        lineitem_batch(schema, config, rows)
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
    /// Per-column Parquet encodings (overrides writer defaults)
    pub column_encodings: Option<Vec<(String, Encoding)>>,
    /// Columns that should use UNCOMPRESSED block compression
    pub uncompressed_column_overrides: Vec<String>,
    /// Columns that should not use dictionary encoding
    pub disable_dictionary_encoding_columns: Vec<String>,
    /// Parquet format version to write
    pub parquet_version: ParquetVersion,
    /// Arrow types for the decimal and date columns
    pub column_types: ColumnTypeConfig,
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
    let schema = T::schema_for(&options.column_types);
    let sorting_columns = T::sorting_columns();
    let column_encodings = options
        .column_encodings
        .as_deref()
        .map(|encodings| column_encodings_for_schema(&schema, encodings));
    let column_encodings = column_encodings.as_deref();

    // generate_parquet is async. One runtime drives every partition's write;
    // each write in turn encodes its row groups across all worker threads.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(chunks)
        .enable_all()
        .build()?;

    let fill_start = Instant::now();
    let mut bytes = 0u64;
    for group in passes {
        let first_day = group[0].first_day;
        let last_day = group[group.len() - 1].last_day;

        // One sweep of the key space per chunk, bucketing by day. Chunks cover
        // ascending key ranges, so concatenating a day's buckets in chunk order
        // yields the rows of that day in key order.
        let mut buckets: Vec<Vec<Vec<T::Row>>> = thread::scope(|scope| {
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

        // Partitions own disjoint day ranges, so each moves its rows out of
        // the shared buckets without touching another partition's.
        let mut prepared = Vec::with_capacity(group.len());
        for partition in group {
            prepared.push(prepare_partition::<T>(
                partition,
                &mut buckets,
                first_day,
                &schema,
                &table_dir,
                options,
            )?);
        }
        // Then write them concurrently: every generate_parquet call shares the
        // runtime's worker pool, so the encode tasks of all partitions in the
        // pass interleave and keep every core busy even when one file has
        // fewer row groups than threads.
        bytes += runtime.block_on(write_prepared::<T>(
            prepared,
            chunks,
            options,
            column_encodings,
            &sorting_columns,
        ))?;
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

/// Keeps only the encodings whose column exists in `schema`
///
/// `tpcgen-cli` applies the same filter per table; this crate writes one
/// table per run, so the schema is the whole filter.
fn column_encodings_for_schema(
    schema: &SchemaRef,
    encodings: &[(String, Encoding)],
) -> Vec<(String, Encoding)> {
    encodings
        .iter()
        .filter(|(column, _)| schema.fields().iter().any(|f| f.name() == column))
        .cloned()
        .collect()
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

/// A partition ready to write: its path and its owned row-group readers
type Prepared<T> = (PathBuf, Vec<RowGroup<T>>);

/// Moves one partition's rows out of the per-chunk buckets and regroups them
/// into owned, row-group-sized readers, each becoming one Parquet row group
fn prepare_partition<T: SortedTable>(
    partition: &Partition,
    buckets: &mut [Vec<Vec<T::Row>>],
    pass_first_day: i32,
    schema: &SchemaRef,
    table_dir: &Path,
    options: &Options,
) -> io::Result<Prepared<T>> {
    // Even row groups: the exact row count is known, so the last row group does
    // not have to be a small remainder.
    let target_rows = (options.row_group_bytes / T::PARQUET_BYTES_PER_ROW).max(1);
    let row_groups = partition.rows.div_ceil(target_rows).max(1);
    let row_group_rows = partition.rows.div_ceil(row_groups) as usize;

    let path = table_dir.join(format!("{}.{}.parquet", T::NAME, partition.number));
    let groups = take_row_groups::<T>(partition, buckets, pass_first_day, row_group_rows);
    let rows: u64 = groups
        .iter()
        .map(|group| {
            group
                .iter()
                .map(|segment| segment.len() as u64)
                .sum::<u64>()
        })
        .sum();
    if rows != partition.rows {
        return Err(io::Error::other(format!(
            "{}: collected {rows} rows, planned {}",
            path.display(),
            partition.rows
        )));
    }

    let readers = groups
        .into_iter()
        .map(|segments| RowGroup {
            schema: schema.clone(),
            config: options.column_types,
            segments,
            segment: 0,
            offset: 0,
            pending: Vec::with_capacity(options.batch_rows),
        })
        .collect();
    Ok((path, readers))
}

/// Writes every prepared partition of a pass concurrently
///
/// Each file gets its own [`generate_parquet`] call; the calls share the
/// runtime's worker pool, so row-group encoding stays parallel across the
/// whole pass rather than ramping up and draining once per file.
async fn write_prepared<T: SortedTable>(
    prepared: Vec<Prepared<T>>,
    threads: usize,
    options: &Options,
    column_encodings: Option<&[(String, Encoding)]>,
    sorting_columns: &[SortingColumn],
) -> io::Result<u64> {
    let writes = prepared.into_iter().map(|(path, readers)| async move {
        let file = File::create(&path)?;
        let writer = BufWriter::with_capacity(32 * 1024 * 1024, file);
        generate_parquet(
            writer,
            readers.into_iter(),
            threads,
            options.compression,
            column_encodings,
            &options.uncompressed_column_overrides,
            &options.disable_dictionary_encoding_columns,
            options.parquet_version,
            Some(sorting_columns),
            ProgressHandle::new(|_| {}),
        )
        .await?;
        Ok(std::fs::metadata(&path)?.len()) as io::Result<u64>
    });
    let sizes: Vec<u64> = futures::future::try_join_all(writes).await?;
    Ok(sizes.into_iter().sum())
}

/// Moves the rows of one partition out of the per-chunk buckets, grouped into
/// row-group-sized lists of day segments
///
/// Every day belongs to exactly one partition, so the takes never overlap.
/// Segments move whole, so no row is copied here: a group closes once it
/// holds at least `row_group_rows` rows, overshooting by less than one
/// segment.
fn take_row_groups<T: SortedTable>(
    partition: &Partition,
    buckets: &mut [Vec<Vec<T::Row>>],
    pass_first_day: i32,
    row_group_rows: usize,
) -> Vec<Vec<Vec<T::Row>>> {
    let offset = day_index(pass_first_day);
    let mut groups = Vec::new();
    let mut current: Vec<Vec<T::Row>> = Vec::new();
    let mut current_rows = 0usize;
    for day in day_index(partition.first_day)..=day_index(partition.last_day) {
        for chunk in buckets.iter_mut() {
            let segment = std::mem::take(&mut chunk[day - offset]);
            current_rows += segment.len();
            current.push(segment);
            if current_rows >= row_group_rows {
                groups.push(std::mem::take(&mut current));
                current_rows = 0;
            }
        }
    }
    if current_rows > 0 {
        groups.push(current);
    }
    groups
}

/// A [`RecordBatchReader`] owning the rows of one row group
///
/// [`generate_parquet`] encodes each reader as its own row group on its own
/// task, so the rows must be owned rather than borrowed from the fill pass's
/// buckets. They stay in their per-day segments until `next` batches them:
/// the copy into a contiguous batch buffer then runs on the encode task, in
/// parallel with the other row groups, instead of serially in the fill pass.
struct RowGroup<T: SortedTable> {
    schema: SchemaRef,
    config: ColumnTypeConfig,
    /// Rows as per-chunk, per-day segments, in (day, chunk) order
    segments: Vec<Vec<T::Row>>,
    /// Current segment and the offset into it
    segment: usize,
    offset: usize,
    /// Scratch space batches are assembled in
    pending: Vec<T::Row>,
}

impl<T: SortedTable> Iterator for RowGroup<T> {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let batch_rows = self.pending.capacity();
        self.pending.clear();
        while self.pending.len() < batch_rows && self.segment < self.segments.len() {
            let segment = &self.segments[self.segment];
            let take = (batch_rows - self.pending.len()).min(segment.len() - self.offset);
            self.pending
                .extend_from_slice(&segment[self.offset..self.offset + take]);
            self.offset += take;
            if self.offset == segment.len() {
                self.segment += 1;
                self.offset = 0;
            }
        }
        if self.pending.is_empty() {
            return None;
        }
        Some(T::batch(&self.schema, &self.config, &self.pending))
    }
}

impl<T: SortedTable> RecordBatchReader for RowGroup<T> {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
