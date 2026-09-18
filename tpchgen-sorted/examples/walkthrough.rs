//! Walks through building a sorted `orders` dataset from a handful of rows
//!
//! Run with `cargo run -p tpchgen-sorted --example walkthrough`.
//!
//! Two separate mechanisms show up here, and they answer different needs:
//!
//! * *skipping* a row (advance every stream by one row) makes rejecting a row
//!   cheap, which is what lets a sweep fill part of the output
//! * *jumping* to a row (advance a stream by many rows at once) makes it
//!   possible to start generating in the middle, which is what lets several
//!   workers cover disjoint pieces of the key space
//!
//! Neither one sorts anything. The sort comes from the key being knowable
//! before the row is built, plus a key domain small enough to index an array.

use tpchgen::dates::{GenerateUtils, TPCHDate, MIN_GENERATE_DATE, TOTAL_DATE_RANGE};
use tpchgen::generators::OrderGenerator;

/// Small enough to print every row
const SCALE_FACTOR: f64 = 0.00001;
/// Large enough that several rows share a day
const BUSY_SCALE_FACTOR: f64 = 0.01;

fn date(day: i32) -> String {
    TPCHDate::new(day + MIN_GENERATE_DATE).to_string()
}

/// Buckets the o_orderkeys of one chunk of the key space by day, reading only
/// the o_orderdate stream
fn bucket_by_day(scale_factor: f64, part: i32, part_count: i32) -> Vec<Vec<i64>> {
    let start = GenerateUtils::calculate_start_index(
        OrderGenerator::SCALE_BASE,
        scale_factor,
        part,
        part_count,
    );
    let rows = OrderGenerator::calculate_row_count(scale_factor, part, part_count);

    let mut order_date_random = OrderGenerator::create_order_date_random();
    order_date_random.advance_rows(start);

    let mut buckets: Vec<Vec<i64>> = vec![Vec::new(); TOTAL_DATE_RANGE as usize];
    for index in start + 1..=start + rows {
        let day = order_date_random.next_value() - MIN_GENERATE_DATE;
        order_date_random.row_finished();
        buckets[day as usize].push(OrderGenerator::make_order_key(index));
    }
    buckets
}

/// Reads buckets in day order, which is what makes the output sorted
fn read_in_day_order(buckets: &[Vec<Vec<i64>>]) -> Vec<(i32, i64)> {
    let mut rows = Vec::new();
    for day in 0..TOTAL_DATE_RANGE as usize {
        for chunk in buckets {
            for order_key in &chunk[day] {
                rows.push((day as i32, *order_key));
            }
        }
    }
    rows
}

/// Multiplications a jump of `rows` rows costs, for a stream drawing `draws`
/// values per row: `advance_seed` squares its multiplier once per bit.
fn jump_cost(rows: i64, draws: i64) -> u32 {
    (rows * draws).max(1).ilog2() + 1
}

fn main() {
    let order_count = OrderGenerator::calculate_row_count(SCALE_FACTOR, 1, 1);
    println!("SF={SCALE_FACTOR} is {order_count} orders, and the date domain is {TOTAL_DATE_RANGE} days\n");

    // ----------------------------------------------------------------------
    println!("1. The key-only scan. One draw from the o_orderdate stream per");
    println!("   row, no row built. This is all it takes to learn the key.\n");

    let mut order_date_random = OrderGenerator::create_order_date_random();
    let mut keys = Vec::new();
    for index in 1..=order_count {
        let day = order_date_random.next_value() - MIN_GENERATE_DATE;
        order_date_random.row_finished();
        keys.push((index, OrderGenerator::make_order_key(index), day));
    }

    println!("   row  o_orderkey  o_orderdate   day");
    for (index, order_key, day) in &keys {
        println!("   {index:>3}  {order_key:>10}  {}  {day:>5}", date(*day));
    }

    // The full generator agrees, but pays for nine more columns per row,
    // including a comment string and a simulation of the order's line items.
    let generated: Vec<_> = OrderGenerator::new(SCALE_FACTOR, 1, 1)
        .iter()
        .map(|order| (order.o_orderkey, order.o_orderdate.into_inner()))
        .collect();
    let scanned: Vec<_> = keys.iter().map(|(_, key, day)| (*key, *day)).collect();
    assert_eq!(generated, scanned);
    println!("\n   the full generator produces exactly these dates\n");

    // ----------------------------------------------------------------------
    println!("2. Bucketing. Each row is appended to buckets[its day]. The day");
    println!("   is an array index, so nothing is compared to anything.\n");

    let mut buckets: Vec<Vec<i64>> = vec![Vec::new(); TOTAL_DATE_RANGE as usize];
    for (_, order_key, day) in &keys {
        buckets[*day as usize].push(*order_key);
    }
    for (day, bucket) in buckets.iter().enumerate() {
        if !bucket.is_empty() {
            println!("   buckets[{day:>4}]  {}  {bucket:?}", date(day as i32));
        }
    }

    // ----------------------------------------------------------------------
    println!("\n3. Reading the buckets in day order gives sorted output. Rows");
    println!("   arrived in o_orderkey order, so each bucket is already sorted");
    println!("   on the secondary key: it came free.\n");

    let mut sorted = Vec::new();
    for (day, bucket) in buckets.iter().enumerate() {
        for order_key in bucket {
            sorted.push((day as i32, *order_key));
        }
    }
    for (day, order_key) in &sorted {
        println!("   {}  o_orderkey {order_key}", date(*day));
    }
    assert!(sorted.windows(2).all(|pair| pair[0] < pair[1]));
    println!("\n   sorted by (o_orderdate, o_orderkey), zero comparisons used\n");

    // ----------------------------------------------------------------------
    println!("4. Why jumping matters: it is how workers split the key space.");
    println!("   A jump of n rows costs about log2(n) multiplications, not n");
    println!("   rows of work, so a worker can start in the middle.\n");

    let workers = 3;
    let busy_rows = OrderGenerator::calculate_row_count(BUSY_SCALE_FACTOR, 1, 1);
    println!(
        "   switching to SF={BUSY_SCALE_FACTOR} ({busy_rows} orders) so days have several rows\n"
    );

    let mut parallel = Vec::new();
    for part in 1..=workers {
        let start = GenerateUtils::calculate_start_index(
            OrderGenerator::SCALE_BASE,
            BUSY_SCALE_FACTOR,
            part,
            workers,
        );
        let rows = OrderGenerator::calculate_row_count(BUSY_SCALE_FACTOR, part, workers);
        println!(
            "   worker {part}: jumps {start} rows in {} multiplications, then owns rows {}..={}",
            jump_cost(start, 1),
            start + 1,
            start + rows
        );
        parallel.push(bucket_by_day(BUSY_SCALE_FACTOR, part, workers));
    }

    println!("\n   a day's rows now live in up to {workers} buckets. Workers own");
    println!("   ascending, contiguous row ranges, so concatenating them in");
    println!("   worker order restores o_orderkey order with no merge:\n");

    let mut shown = 0;
    for day in 0..TOTAL_DATE_RANGE as usize {
        let pieces: Vec<&Vec<i64>> = parallel
            .iter()
            .map(|worker| &worker[day])
            .filter(|bucket| !bucket.is_empty())
            .collect();
        if pieces.len() > 1 && shown < 3 {
            let joined: Vec<i64> = pieces
                .iter()
                .flat_map(|piece| piece.iter().copied())
                .collect();
            println!("   {}  {pieces:?}", date(day as i32));
            println!("               concatenated: {joined:?}");
            assert!(joined.windows(2).all(|pair| pair[0] < pair[1]));
            shown += 1;
        }
    }

    let parallel_rows = read_in_day_order(&parallel);
    let single_rows = read_in_day_order(&[bucket_by_day(BUSY_SCALE_FACTOR, 1, 1)]);
    assert_eq!(parallel_rows, single_rows);
    assert!(parallel_rows.windows(2).all(|pair| pair[0] < pair[1]));
    println!(
        "\n   all {} rows match the single-worker output, and are sorted\n",
        parallel_rows.len()
    );

    // ----------------------------------------------------------------------
    println!("5. Why skipping matters: it is how one sweep fills one partition");
    println!("   without building the rows that belong to other partitions.\n");

    for span in [2557, 256, 64] {
        let low = MIN_GENERATE_DATE + 1000;
        let high = low + span - 1;
        let built = OrderGenerator::new(BUSY_SCALE_FACTOR, 1, 1)
            .iter()
            .with_order_date_range(low, high)
            .count();
        println!(
            "   a {span:>4}-day partition: built {built:>5} of {busy_rows} rows, \
             skipped {:>5} after one draw each",
            busy_rows as usize - built
        );
    }
    println!("\n   a skipped row costs a draw and a row advance, not a customer");
    println!("   key, a clerk name, a comment and a line item simulation");

    // ----------------------------------------------------------------------
    println!("\n6. A rejected row is not remembered and not looked up again.");
    println!("   Every sweep walks the whole index space; a row rejected by one");
    println!("   sweep is built by whichever sweep owns its day. Partitions");
    println!("   cover the domain and do not overlap, so each row is built once.\n");

    let sweeps = [(0, 999), (1000, TOTAL_DATE_RANGE - 1)];
    let mut all_built: Vec<(i32, i64)> = Vec::new();
    for (partition, (first_day, last_day)) in sweeps.iter().enumerate() {
        let built: Vec<(i32, i64)> = OrderGenerator::new(SCALE_FACTOR, 1, 1)
            .iter()
            .with_order_date_range(MIN_GENERATE_DATE + first_day, MIN_GENERATE_DATE + last_day)
            .map(|order| (order.o_orderdate.into_inner(), order.o_orderkey))
            .collect();

        println!(
            "   sweep {} owns {} .. {}: walked all {order_count} rows, built {}, rejected {}",
            partition + 1,
            date(*first_day),
            date(*last_day),
            built.len(),
            order_count as usize - built.len(),
        );

        // A sweep restricts, it does not order: rows come out in index order,
        // which is why the bucket array from step 2 is still needed.
        println!("      as the sweep emits them, in row index order:");
        for (day, order_key) in &built {
            println!("         {}  o_orderkey {order_key}", date(*day));
        }
        assert!(!built.windows(2).all(|pair| pair[0] < pair[1]));

        let mut partition_buckets: Vec<Vec<i64>> = vec![Vec::new(); TOTAL_DATE_RANGE as usize];
        for (day, order_key) in &built {
            partition_buckets[*day as usize].push(*order_key);
        }
        println!("      after bucketing, read back in day order:");
        for (day, order_key) in read_in_day_order(&[partition_buckets]) {
            println!("         {}  o_orderkey {order_key}", date(day));
            all_built.push((day, order_key));
        }
    }

    // No row was generated twice, none was lost, and because the sweeps ran in
    // ascending day order their outputs concatenate into a sorted dataset.
    assert_eq!(all_built.len(), order_count as usize);
    assert!(all_built.windows(2).all(|pair| pair[0] < pair[1]));
    assert_eq!(all_built, sorted);
    println!("\n   {order_count} rows built in total, each exactly once, and the two");
    println!("   partitions read back to back are the sorted dataset from step 3");
}
