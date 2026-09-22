# POC: date-clustered TPC-H generated sorted, without sorting

Branch `poc/sorted-tpch-gen`, based on `tom/upstream-port`. Worktree lives at
`/home/qic/tpchgen-rs-sorted`; the git directory is in `/home/qic/tpchgen-rs-fork`.

`cargo` is not on `PATH` by default here: `export PATH="$HOME/.cargo/bin:$PATH"`.

## What this is for

An alternative to tpchgen-rs PR #385, which produces a sorted TPC-H dataset by
rewriting an existing one with pylibcudf on a GPU. That rewrite reads the source
once per output partition, because an unsorted table scatters every date across
every row group and statistics cannot prune those reads. This branch generates
the clustered dataset directly instead.

## Why no sort is needed

A row's cluster key is known before the row is built, and the key domain is
tiny:

- `o_orderdate` is one draw from a stream no other column reads
- `l_shipdate` is that date plus one draw per line
- the date domain is 2557 days (`dates::TOTAL_DATE_RANGE`)
- `o_orderkey` increases monotonically with the generator's row index

So ordering by `(date, key)` is a stable distribution of the natural generation
order into at most 2557 buckets: a counting sort, no comparisons, with the
secondary key satisfied for free because rows arrive in key order.

A run has three phases: measure rows per day by replaying only the date streams,
cut the day domain into contiguous equal-row partitions, then sweep the key
space filling one group of partitions at a time. A sweep rejects rows outside
its range after reading only their date draw, so the rows it does not want cost
a multiplication rather than a full row.

## Code map

- `tpchgen/src/generators.rs` — `OrderGeneratorIterator::with_order_date_range`
  and `LineItemGeneratorIterator::with_ship_date_range`, plus `finish_row`,
  `finish_order` and `skip_line_item`. These make rejection cheap.
- `tpchgen-arrow/src/{order,lineitem}.rs` — `order_batch` / `lineitem_batch`
  convert a row slice to a `RecordBatch`, for callers that buffer rows.
- `tpchgen-sorted/src/histogram.rs` — key-only scans and per-chunk day counts.
- `tpchgen-sorted/src/layout.rs` — day histogram to equal-row partitions.
- `tpchgen-sorted/src/write.rs` — bucketed fill and Parquet output through
  `tpcgen-cli`'s `generate_parquet` (same writer and flags as the unsorted
  baseline; row groups of a pass encode concurrently on one tokio runtime).
- `tpchgen-sorted/src/verify.rs` — reads output back, checks sortedness and
  compares a fingerprint derived independently from the random streams.
- `tpchgen-sorted/examples/walkthrough.rs` — prints every step for 15 rows.
- `tpchgen-sorted/scripts/post_process_cost.py` — measures what the #385 path
  costs on a dataset already on disk.

## Invariants

- Selective generation must stay bit-identical to filtering the full generator.
  Four tests in `tpchgen/src/generators.rs` cover this, including across parts.
- Partitions must be contiguous, disjoint, and hold equal row counts. Equal
  *day* counts are wrong: `l_shipdate` is `o_orderdate` plus 1 to 121 days, so
  the ends of its domain are far sparser than the middle.
- Date keys must never reach a comparison sort.
- Any pass count must produce byte-identical files, as
  `multiple_passes_produce_the_same_dataset` asserts.

## Measured, SF10 `lineitem`, 24 cores, Snappy, 48 files on both sides

Both sides now write through `tpcgen-cli`'s `generate_parquet` with the same
flags, so the datasets differ only in row order.

| | |
|---|---|
| sorted, 48 files, 6 sweeps (one sixth the memory) | 5.37 s |
| sorted, 48 files, 1 sweep | 5.71 s |
| unsorted `tpcgen-cli`, 48 files | 4.77 s |
| #385 path, floor: generate + 48 reads | ≥ 168 s |

One sweep is now *slower* than six: the writers already saturate all cores, so
the extra sweep costs less than the page-cache pressure of holding the whole
table. `orders` at 48 files: sorted 1.84 s, unsorted 1.48 s — at 15M rows the
key-only measure pass is a fixed cost the fill phase cannot amortise. Key-only
scans run at 1.2–1.5 G rows/s. Row group `l_shipdate` span drops from 2526
days (528 of 528 row groups unprunable) to 4 days median. Every row group
declares `sorting_columns` on `(date, key)`.

## Remaining work, in order

1. Emit a complete dataset: the other six tables, either via a wrapper around
 `tpcgen-cli` or by folding this into `tpcgen-cli` as a flag.

Not started, and not needed for the POC: resume support, cluster keys other than
the dates, TPC-DS.

## Running it

```bash
export PATH="$HOME/.cargo/bin:$PATH"
cargo run --release -p tpchgen-sorted -- -s 10 -t lineitem -o /tmp/out -f 48 --verify
cargo run -p tpchgen-sorted --example walkthrough
cargo test -p tpchgen -p tpchgen-arrow -p tpchgen-sorted
```

`--files-per-pass` trades memory against sweeps: peak memory is roughly that
many partitions' worth of rows, and each extra sweep costs about a 27th of a
generation pass. `--plan-only` prints the partition layout without writing.
