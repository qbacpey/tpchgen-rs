//! Generates TPC-H `orders` and `lineitem` already partitioned by date and
//! sorted, without ever sorting
//!
//! # Why no sort is needed
//!
//! The rows of these tables are produced in `o_orderkey` order, and
//! `o_orderkey` increases monotonically with the generator's row index. The
//! date that the output is clustered on is also fixed by the row index:
//! `o_orderdate` is one draw from a random stream no other column touches, and
//! `l_shipdate` is that date plus one more draw per line. Finally, the date
//! domain is only [`TOTAL_DATE_RANGE`] days wide.
//!
//! Put together, ordering by `(o_orderdate, o_orderkey)` is a *stable
//! distribution of the natural generation order into at most 2557 buckets* -
//! a counting sort. It costs one pass, no comparisons, and the secondary key
//! is satisfied for free because rows arrive in key order. The same holds for
//! `(l_shipdate, l_orderkey, l_linenumber)`.
//!
//! # How a dataset is built
//!
//! 1. **Measure.** Replay only the date streams to count rows per day exactly.
//!    This costs a few multiplications per row, so it is a small fraction of
//!    the cost of generating the table ([`histogram`]).
//! 2. **Plan.** Cut the day domain into contiguous, disjoint ranges holding
//!    equal *row* counts, one per output file, and size row groups from the
//!    exact counts ([`layout`]).
//! 3. **Fill.** For each group of output files, sweep the key space with
//!    [`OrderGeneratorIterator::with_order_date_range`], which materialises
//!    only the rows whose date falls in the group and skips the rest after
//!    reading their date draw. Rows land in per-day buckets, which are then
//!    concatenated in day order to produce sorted output ([`write`]).
//!
//! Step 3 buffers one group of files, so peak memory is set by
//! `partitions_per_pass`, and the sweeps that skip a group cost only their key
//! draws. That is the knob that trades memory against re-deriving keys; no
//! spilling, no external merge, and no second pass over written data.
//!
//! [`TOTAL_DATE_RANGE`]: tpchgen::dates::TOTAL_DATE_RANGE
//! [`OrderGeneratorIterator::with_order_date_range`]:
//!     tpchgen::generators::OrderGeneratorIterator::with_order_date_range

pub mod histogram;
pub mod layout;
pub mod verify;
pub mod write;

pub use layout::Partition;
pub use write::{Options, Report, SortedTable, Table};
