//! Generates date-clustered, sorted TPC-H tables

use clap::{Parser, ValueEnum};
use parquet::basic::{Compression, Encoding};
use std::io;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Instant;
use tpcgen_cli::parquet::ParquetVersion;
use tpchgen::dates::TPCHDate;
use tpchgen_arrow::{ColumnTypeConfig, DateColumnType, DecimalColumnType};
use tpchgen_sorted::verify;
use tpchgen_sorted::write::{self, LineItems, Options, Orders, Report, Table};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum TableArg {
    /// `orders`, partitioned and sorted on `o_orderdate`, then `o_orderkey`
    Orders,
    /// `lineitem`, partitioned and sorted on `l_shipdate`, then `l_orderkey`
    Lineitem,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum CompressionArg {
    Snappy,
    Zstd,
    Uncompressed,
}

#[derive(Debug, Parser)]
#[command(
    name = "tpchgen-sorted",
    about = "Generate TPC-H tables already partitioned by date and sorted, without sorting"
)]
struct Args {
    /// Scale factor
    #[arg(short = 's', long, default_value_t = 1.0)]
    scale_factor: f64,

    /// Table to generate
    #[arg(short = 't', long, value_enum)]
    table: TableArg,

    /// Output directory; the table is written to a subdirectory of it
    #[arg(short = 'o', long, default_value = ".")]
    output_dir: PathBuf,

    /// Number of output files, each covering a disjoint range of days
    #[arg(short = 'f', long, default_value_t = 8)]
    files: usize,

    /// Number of key-space chunks generated in parallel
    #[arg(long)]
    threads: Option<usize>,

    /// Output files filled by a single sweep of the key space
    ///
    /// Higher values buffer more rows but re-derive sort keys fewer times. Peak
    /// memory is roughly this many output files' worth of rows.
    #[arg(long, default_value_t = 8)]
    files_per_pass: usize,

    /// Target uncompressed bytes per row group
    #[arg(long, default_value_t = 7 * 1024 * 1024)]
    row_group_bytes: u64,

    /// Rows converted to Arrow at a time
    #[arg(long, default_value_t = 65536)]
    batch_rows: usize,

    #[arg(long, value_enum, default_value_t = CompressionArg::Snappy)]
    compression: CompressionArg,

    /// Per-column Parquet encodings, overriding the writer defaults.
    ///
    /// Format: COLUMN=ENCODING pairs, comma separated. Encodings naming a
    /// column the table does not have are ignored. PLAIN_DICTIONARY and
    /// RLE_DICTIONARY are rejected by the writer: dictionary encoding is the
    /// writer default and cannot be requested through this flag.
    ///
    /// Example: `--column-encoding l_comment=PLAIN,l_orderkey=DELTA_BINARY_PACKED`
    #[arg(long, value_delimiter = ',', value_parser = parse_column_encoding_pair)]
    column_encoding: Option<Vec<(String, Encoding)>>,

    /// Disable dictionary encoding for specific columns.
    ///
    /// Format: comma separated list of column names.
    ///
    /// Example: `--disable-dictionary-encoding=l_comment,l_shipinstruct`
    #[arg(long = "disable-dictionary-encoding", num_args = 0.., value_delimiter = ',')]
    disable_dictionary_encoding_columns: Vec<String>,

    /// Parquet format version to write.
    ///
    /// Version 1 (default) has broader compatibility. Version 2 uses Data Page
    /// V2 format with improved encodings.
    ///
    /// Valid values: v1 (default), v2
    #[arg(long, default_value = "v1", value_parser = clap::value_parser!(ParquetVersion))]
    parquet_version: ParquetVersion,

    /// Type to use for decimal/monetary columns.
    ///
    /// Valid values: decimal128 (default), f64
    #[arg(long, default_value = "decimal128", value_parser = clap::value_parser!(DecimalColumnType))]
    decimal_column_type: DecimalColumnType,

    /// Type to use for date columns.
    ///
    /// Valid values: date32 (default), timestamp_ms
    #[arg(long, default_value = "date32", value_parser = clap::value_parser!(DateColumnType))]
    date_column_type: DateColumnType,

    /// Print the partition plan and exit
    #[arg(long)]
    plan_only: bool,

    /// Read the output back and check it against the generator
    #[arg(long)]
    verify: bool,
}

/// Parses one `COLUMN=ENCODING` pair of `--column-encoding`
fn parse_column_encoding_pair(s: &str) -> Result<(String, Encoding), String> {
    let Some((name, encoding)) = s.split_once('=') else {
        return Err(format!("expected COLUMN=ENCODING, got: '{s}'"));
    };
    let name = name.trim();
    let encoding = encoding.trim();
    if name.is_empty() || encoding.is_empty() {
        return Err(format!("expected COLUMN=ENCODING, got: '{s}'"));
    }
    let encoding = Encoding::from_str(encoding).map_err(|e| e.to_string())?;
    Ok((name.to_string(), encoding))
}

fn main() -> io::Result<()> {
    let args = Args::parse();
    let threads = args
        .threads
        .or_else(|| std::thread::available_parallelism().ok().map(|n| n.get()))
        .unwrap_or(1);

    let options = Options {
        scale_factor: args.scale_factor,
        output_dir: args.output_dir.clone(),
        file_count: args.files,
        threads,
        partitions_per_pass: args.files_per_pass,
        row_group_bytes: args.row_group_bytes,
        batch_rows: args.batch_rows,
        compression: match args.compression {
            CompressionArg::Snappy => Compression::SNAPPY,
            CompressionArg::Zstd => Compression::ZSTD(Default::default()),
            CompressionArg::Uncompressed => Compression::UNCOMPRESSED,
        },
        column_encodings: args.column_encoding,
        uncompressed_column_overrides: Vec::new(),
        disable_dictionary_encoding_columns: args.disable_dictionary_encoding_columns,
        parquet_version: args.parquet_version,
        column_types: ColumnTypeConfig {
            decimal_type: args.decimal_column_type,
            date_type: args.date_column_type,
            ..Default::default()
        },
        plan_only: args.plan_only,
    };

    let (table, report) = match args.table {
        TableArg::Orders => (Table::Orders, write::generate::<Orders>(&options)?),
        TableArg::Lineitem => (Table::LineItem, write::generate::<LineItems>(&options)?),
    };

    print_report(&options, table, &report);

    if args.verify && !args.plan_only {
        let start = Instant::now();
        let expected = match table {
            Table::Orders => verify::expected_orders(args.scale_factor),
            Table::LineItem => verify::expected_lineitem(args.scale_factor),
        };
        let table_dir = args.output_dir.join(table.name());
        verify::check(&table_dir, table, &report.partitions, expected)?;
        println!(
            "verified {} rows sorted, partitioned and complete in {:.2?}",
            expected.rows,
            start.elapsed()
        );
    }

    Ok(())
}

fn print_report(options: &Options, table: Table, report: &Report) {
    let day = |value: i32| TPCHDate::new(value).to_string();
    println!(
        "{} SF={} into {} files over {} passes with {} threads",
        table.name(),
        options.scale_factor,
        report.partitions.len(),
        report.passes,
        options.threads,
    );
    for partition in &report.partitions {
        println!(
            "  {:>4}  {} .. {}  {:>4} days  {:>12} rows",
            partition.number,
            day(partition.first_day),
            day(partition.last_day),
            partition.days(),
            partition.rows,
        );
    }

    let measure = report.measure_time.as_secs_f64();
    println!(
        "measured {} rows per day in {:.2?} ({:.1}M rows/s)",
        report.rows,
        report.measure_time,
        report.rows as f64 / measure / 1e6,
    );
    if report.fill_time.is_zero() {
        return;
    }
    let fill = report.fill_time.as_secs_f64();
    println!(
        "generated and wrote {:.2} GiB in {:.2?} ({:.1}M rows/s)",
        report.bytes as f64 / (1024.0 * 1024.0 * 1024.0),
        report.fill_time,
        report.rows as f64 / fill / 1e6,
    );
    println!(
        "total {:.2?}, key measurement was {:.1}% of it",
        report.total_time(),
        100.0 * measure / (measure + fill),
    );
}
