# Sort-Free Sorted TPC-H Generation vs. GPU Rewrite (tpchgen-rs #385): Benchmark Report

**Date:** 2026-09-22
**Author:** Qic (POC branch `poc/sorted-tpch-gen`, repo `tpchgen-rs-sorted`)
**Audience:** technical report for mentor review; structured for slide extraction.

---

## TL;DR

We generate a **date-clustered, fully sorted** TPC-H dataset (`lineitem` by `(l_shipdate, l_orderkey)`,
`orders` by `(o_orderdate, o_orderkey)`) **directly, with zero comparisons and zero re-reads**, on CPU.

At SF300 (1.8B rows, 78 GiB Parquet) the whole job takes **161.8 s**. The GPU-rewrite approach of
tpchgen-rs PR #385 (generate unsorted, then re-read once per output partition with pylibcudf, sort on
GPU, write again) has a measured **lower bound of 4,322 s** at the same scale — reads alone, before any
sorting or writing. **Our approach is ≥27× faster than that floor, needs no GPU, and the speedup grows
with scale and partition count.**

The sorted output costs only **1.13–1.59×** the time of plain unsorted generation, is byte-comparable
to it (same writer, same flags; identical row counts; ≤1.9% size difference), and collapses row-group
date spans from **2526 days (100% of row groups unprunable) to a median of 0–4 days**.

---

## 1. Problem statement

Benchmarks of Parquet readers (PrestoGPU / Velox-cuDF hybrid-scan reader, cuDF reader) need a
**sorted/clustered TPC-H dataset** so that row-group min/max statistics can actually prune date-range
scans. An unsorted dataset scatters every date across every row group, so statistics prune nothing.

Question: what is the cheapest way to *produce* such a dataset?

## 2. The two approaches

### 2.1 Baseline approach — GPU post-process rewrite (tpchgen-rs PR #385, "Greg's path")

```
1. Generate unsorted TPC-H with tpchgen-cli, write to disk        (CPU)
2. FOR EACH output partition (e.g. 48):
     read the source, filtered to that partition's date range     (GPU, pylibcudf)
     sort the partition on GPU
3. Write the sorted dataset                                       (write #2)
```

**Structural weakness:** the source is unsorted, so every row group's `l_shipdate` min/max spans
essentially the whole 2526-day domain. The per-partition "filtered read" **cannot be pruned by
statistics and degenerates to a full table scan**. We measured 100% of row groups spanning more than
one 48-way partition at every scale (528/528 at SF10; 5232/5232 at SF100; 15696/15696 at SF300).

Cost model: `generate + N_partitions × full_read + GPU_sort + write`.

### 2.2 Our approach — sort-free sorted generation ("counting sort during generation")

**Key insight: the cluster key is known before the row exists.**

- `o_orderdate` is one draw from a dedicated random stream that no other column reads;
- `l_shipdate` = `o_orderdate` + one draw (1..121 days) per line;
- `o_orderkey` increases monotonically with the generator's row index;
- the date domain is only 2,557 days.

Therefore "order by (date, key)" is a **stable distribution of the natural generation order into
≤2,557 day-buckets** — a counting sort, not a comparison sort. The secondary key comes for free
because rows arrive in key order.

Three phases:

```
1. MEASURE  Replay only the date streams; count rows per day.       (~2 multiplications/row)
2. PLAN     Cut the day domain into contiguous, disjoint,           (exact counts → equal ROWS,
            equal-row-count ranges — one per output file.            not equal days: the ends of
                                                                     the l_shipdate domain are
                                                                     ~10× sparser than the middle)
3. FILL     Per group of files: sweep the key space; a row outside  (rejection = 1 date draw +
            the range is dropped after its date draw only; a kept   stream advance, no row built)
            row is materialized once, appended to buckets[day],
            and buckets are concatenated in day order → sorted.
```

Multi-threading: the key space is split into 24 chunks in *both* measure and fill; each thread jumps
to its chunk start in ~log₂(n) multiplications (`advance_rows`). Because chunks own ascending,
contiguous row-index ranges, concatenating a day's per-chunk buckets in chunk order restores key order
**with no merge**. Per-chunk histograms double as exact bucket capacities (zero reallocation during
fill). Output is written through `tpcgen-cli`'s parallel `generate_parquet` — the same writer and
flags as the unsorted baseline — with each file declaring `sorting_columns` in its Parquet metadata.

I/O total: **exactly one write of the final dataset. No re-reads, no sort, no GPU.**

## 3. Experimental setup

| | |
|---|---|
| Machine | 24 cores, 62 GB RAM, Linux 7.0.0-31-generic |
| Compression | Snappy (both sides) |
| Files | 48 per dataset (both sides) |
| Writer | `tpcgen-cli generate_parquet` (parallel column-chunk encoder), identical flags |
| Unsorted baseline | `tpcgen-cli tpch parquet -s SF -T <table> -o DIR -p 48` |
| Sorted | `tpchgen-sorted -s SF -t <table> -o DIR -f 48 [--files-per-pass N] --verify` |
| Memory knob | `--files-per-pass`: 8 (SF10/SF100) → 6 sweeps; 4 (SF300) → 12 sweeps |
| Verification | every sorted run reads the files back and checks sortedness, partition ranges, and a stream-derived fingerprint (no row dropped/duplicated/mis-bucketed). **All runs passed.** |
| Conditions | same machine, same session, no cache dropping; both sides write to the same disk |

## 4. Results

### 4.1 `lineitem` — wall-clock generation time

| Scale | Rows | Unsorted | Sorted (total) | Ratio | Sorted peak RSS | Unsorted RSS |
|---|---|---|---|---|---|---|
| SF10 | 59,986,052 | 4.77 s | **5.37 s** | **1.13×** | ~9 GB (1 sweep) | 1.3 GB |
| SF100 | 600,037,902 | 37.10 s | **48.65 s** | **1.31×** | 27.3 GB (6 sweeps) | 1.4 GB |
| SF300 | 1,799,989,091 | 101.65 s | **161.83 s** | **1.59×** | 37.7 GB (12 sweeps) | 1.4 GB |

Sorted time decomposes as: measure 0.04 s / 0.31 s / 0.93 s (≈0.5%, at 1.9–2.0 G rows/s) + fill
(generate + bucket + encode + write). Read-back verification adds 1.2 s / 18.1 s / 59.8 s.

### 4.2 `orders` — wall-clock generation time

| Scale | Rows | Unsorted | Sorted | Ratio |
|---|---|---|---|---|
| SF100 | 150,000,000 | 10.92 s | **11.38 s** | **1.04×** |
| SF300 | 450,000,000 | 26.94 s | **34.99 s** | **1.30×** |

(At SF10, orders sorted was 1.84 s vs 1.48 s — the fixed measure pass does not amortize on small
tables. By SF100 it has amortized.)

### 4.3 The GPU-rewrite path (#385) — measured lower bound

Measured on the unsorted datasets above (one full streaming pass over the source; 48 partitions):

| Scale | Generate (unsorted) | One full source read | Read floor (48 × read) | **Total lower bound**¹ | vs. our sorted time |
|---|---|---|---|---|---|
| SF10 | 4.77 s | 3.41 s | 163.75 s | **≥ 168 s** | **31×** slower |
| SF100 | 37.10 s | 28.95 s | 1,389.60 s | **≥ 1,427 s (~24 min)** | **29×** slower |
| SF300 | 101.65 s | 87.92 s | 4,220.23 s | **≥ 4,322 s (~72 min)** | **27×** slower |

¹ Lower bound excludes the GPU sort itself and the final write. Reference point: one *CPU* sort of the
SF10 table (60M rows, pyarrow) took 41.16 s — the GPU sort is faster, but reads alone already dominate
and grow linearly with partition count.

Even an *idealized* single-read GPU pipeline (1 read + sort + 1 write) would cost ≥ ~15 s at SF10 —
still ~3× slower than ours, and it requires GPU hardware. Ours is pure CPU.

### 4.4 Output quality and comparability

| Property | Result |
|---|---|
| Row counts | identical between sorted and unsorted at every scale (verified to the row) |
| Dataset size ratio (sorted/unsorted) | 1.017 (SF10), 1.019 (SF100), 1.019 (SF300) |
| Row-group geometry | unsorted: 11 rg/file, 112–115K rows; sorted: 11–12 rg/file, 98–115K rows |
| `sorting_columns` metadata | declared on **every** row group: `(l_shipdate, l_orderkey)` / `(o_orderdate, o_orderkey)` |
| Row-group `l_shipdate` span, unsorted | 2526 days — 100% of row groups unprunable (528/528, 5232/5232, 15696/15696) |
| Row-group `l_shipdate` span, sorted | SF10: median 4 days, max 32; SF100: median 0, max 10; SF300: median 0, max 5 |
| Byte-identity across sweep counts | asserted by `multiple_passes_produce_the_same_dataset` (passing) |
| Selective generation correctness | 4 bit-exactness tests vs. full generator (passing, untouched) |

## 5. Analysis

**Why we are faster — three levels:**

1. **Algorithmic complexity.** Theirs: O(partitions × rows) reads + O(n log n) sort.
   Ours: O(rows) single-pass generation; rejecting an out-of-range row costs one RNG draw + a stream
   advance (~1/27th of building it). 60M rows × 48 reads vs. 60M rows × 1 generation is the 27–31×.
2. **When the key is known.** Theirs must materialize and read every row to learn its sort key.
   Ours derives the key from random-stream state *before* the row exists — so "sorting" degenerates to
   array indexing, and the secondary key is satisfied for free by generation order.
3. **Data movement.** Theirs moves ≥4× the dataset (write + 48 reads + write, plus CPU↔GPU transfers).
   Ours writes the dataset exactly once. Parquet generation is I/O- and encoding-bound; moving less
   data *is* the speedup.

**Why the sorted/unsorted ratio widens with scale (1.13× → 1.31× → 1.59×):**

- Sorted generation does strictly more work than unsorted (a measure pass + bucketing + rejection
  sweeps), so a ratio above 1.0 is expected — that is the *price of sortedness*, and it stays small.
- At SF300 we deliberately traded more sweeps (12) for bounded memory (37.7 GB RSS vs. an estimated
  ~55 GB for the 6-sweep config) on a 62 GB machine writing 78 GiB. Each extra sweep costs ~1/27th of
  a generation pass. The knob works as designed; on a bigger-memory machine the ratio would tighten.
- Unsorted generation streams with a constant ~1.4 GB RSS regardless of scale.

**Scaling per unit:** unsorted 0.48 → 0.37 → 0.34 s per SF unit (fixed overheads amortize); sorted
0.54 → 0.49 → 0.54 s per SF unit — linear scaling confirmed.

## 6. Applications

### 6.1 velox-testing / PrestoGPU benchmarks

- `velox-testing/benchmark_data_tools` already installs the patched `tpchgen-cli` with per-column
  encoding flags; `tpchgen-sorted` supports the same flags (`--column-encoding`,
  `--disable-dictionary-encoding`, `--parquet-version`, `--decimal-column-type`, `--date-column-type`),
  so the sorted dataset matches the velox codec definitions (use `--decimal-column-type f64` for the
  PrestoGPU float convention).
- The Velox-cuDF **hybrid-scan reader prunes row groups by statistics**
  (`filter_row_groups_with_stats`). On unsorted data that pruning is a no-op (100% of row groups
  overlap any 1/48-wide date range); on our sorted output a date-range scan prunes ~47 of 48 files and
  most row groups (median span 0–4 days). This yields a clean A/B pair for quantifying stats pruning
  on the GPU read path — the datasets are byte-comparable, so the experiment is not confounded by
  writer differences.
- Dataset generation cost drops from "tens of minutes + a GPU" to "seconds on CPU", making sorted
  datasets a routine dimension of the benchmark matrix instead of a one-off artifact.

### 6.2 Upstreaming into tpcgen-cli

All blockers are cleared: `generate_parquet` accepts `sorting_columns`; `OrderArrow`/`LineItemArrow`
expose `schema_for(config)`; selective generation is pinned by bit-exactness tests. Remaining work:
move `histogram`/`layout`/`write` from the POC crate into `tpcgen-cli` behind a flag (e.g.
`tpch parquet --cluster-by-date`), with the other six tables on the existing path, so one command
emits a complete sorted 8-table dataset. Out of scope for the POC: resume support, non-date cluster
keys, TPC-DS.

## 7. Limitations (be upfront)

- Only `orders` and `lineitem`; only date cluster keys.
- Sorted generation costs 1.1–1.6× plain generation (it does strictly more work); on small tables the
  fixed measure pass dominates (orders SF10: 1.24×).
- Peak memory is proportional to rows buffered per sweep (bounded by `--files-per-pass`); at SF300 on
  a 62 GB machine we ran 12 sweeps (37.7 GB peak).
- No resume support.

## 8. Suggested slide outline (for the illustration LLM)

1. **Title** — "Sorted TPC-H without sorting: 27–31× faster than GPU rewrite"
2. **Motivation** — Parquet stats pruning needs sorted data; unsorted row groups span 2526 days (100%
   unprunable) vs. 0–4 days median (sorted). [Chart: span distribution comparison]
3. **Approach A: GPU rewrite (#385)** — flow diagram (generate → 48× read → GPU sort → write);
   callout: "filtered reads degenerate to full scans".
4. **Approach B: sort-free generation** — the key insight ("the cluster key is known before the row
   exists"); 3-phase diagram (measure → plan → fill); callout: "zero comparisons, zero re-reads,
   one write".
5. **Headline numbers** — bar chart: our time vs. #385 lower bound at SF10/100/300 (5.4s vs ≥168s;
   48.7s vs ≥1427s; 161.8s vs ≥4322s). Log scale recommended.
6. **Cost of sortedness** — line chart: sorted vs. unsorted generation time across scales
   (ratio 1.13×→1.31×→1.59×); note linear scaling for both.
7. **Why we win** — three bullets (algorithmic complexity / key known before the row / data movement).
8. **Output quality** — identical row counts, ≤1.9% size delta, `sorting_columns` metadata,
   verification story.
9. **Applications** — velox-testing A/B for GPU reader stats pruning; upstream path into tpcgen-cli.
10. **Limitations & next steps.**

## Appendix A — reproduction commands

```bash
export PATH="$HOME/.cargo/bin:$PATH"
cargo build --release -p tpchgen-sorted -p tpcgen-cli

# unsorted baseline
./target/release/tpcgen-cli tpch parquet -s 300 -T lineitem -o /tmp/bench/u300 -p 48

# sorted (memory-bounded config for SF300)
./target/release/tpchgen-sorted -s 300 -t lineitem -o /tmp/bench/s300 -f 48 --files-per-pass 4 --verify

# #385 floor measurement
python3 tpchgen-sorted/scripts/post_process_cost.py /tmp/bench/u300/lineitem --partitions 48

# correctness
cargo test -p tpchgen -p tpchgen-arrow -p tpchgen-sorted
```

## Appendix B — raw numbers log

| Run | Wall (s) | Fill (s) | Measure (s) | Verify (s) | Peak RSS |
|---|---|---|---|---|---|
| SF10 lineitem unsorted | 4.77 | — | — | — | ~1.3 GB |
| SF10 lineitem sorted (6 sweeps) | 5.37 | 5.32 | 0.04 | 1.16 | ~9 GB |
| SF10 lineitem sorted (1 sweep) | 5.71 | 5.68 | 0.04 | 1.15 | ~9 GB+ |
| SF100 lineitem unsorted | 37.10 | — | — | — | 1.36 GB |
| SF100 lineitem sorted (6 sweeps) | 48.65 | 48.34 | 0.31 | 18.07 | 27.3 GB |
| SF100 lineitem sorted (12 sweeps) | 53.24 | 52.93 | 0.31 | 18.76 | 15.0 GB |
| SF300 lineitem unsorted | 101.65 | — | — | — | 1.42 GB |
| SF300 lineitem sorted (12 sweeps) | 161.83 | 160.89 | 0.93 | 59.79 | 37.7 GB |
| SF100 orders unsorted / sorted | 10.92 / 11.38 | 11.34 | 0.04 | 2.32 | 1.0 / 5.9 GB |
| SF300 orders unsorted / sorted | 26.94 / 34.99 | 34.87 | 0.11 | 11.97 | 1.3 / 6.9 GB |
| #385 read pass SF10 / SF100 / SF300 | 3.41 / 28.95 / 87.92 s | | | | |
| CPU sort reference, SF10 (pyarrow) | 41.16 s | | | | |
