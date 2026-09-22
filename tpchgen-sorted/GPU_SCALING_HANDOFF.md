# Handoff: scaling benchmarks — sort-free CPU generation vs. GPU rewrite (#385)

**For:** the agent/operator on the GPU benchmark machine
**From:** Qic's POC branch `poc/sorted-tpch-gen` on `github.com/qbacpey/tpchgen-rs`
**Date:** 2026-09-22

## Mission

Measure wall-clock generation time of two ways to produce a **date-clustered, sorted TPC-H
dataset** (`orders` by `(o_orderdate, o_orderkey)`, `lineitem` by `(l_shipdate, l_orderkey)`),
at **SF100, SF1000, SF3000** (SF10000 optional, SF30000 only with sharding — see "Reality check"):

- **Approach A (this branch):** `tpchgen-sorted` — generates sorted output directly on CPU.
  No reads, no sort, no GPU. Baselines already measured on a 24-core/62GB box:
  SF10 = 5.37s, SF100 = 48.65s, SF300 = 161.8s (all `--verify`-passing).
- **Approach B (Greg's):** [rapidsai/velox-testing#385](https://github.com/rapidsai/velox-testing/pull/385)
  — rewrites an existing unsorted dataset with pylibcudf in a RAPIDS container: one filtered
  full-table read per output partition (read amplification = file count), GPU sort, write.

Report the results table at the end of this document, with logs preserved.

## Machine requirements

| | Approach A (ours) | Approach B (Greg's) |
|---|---|---|
| GPU | none | 1 NVIDIA GPU + docker + nvidia-container-toolkit |
| RAM | see config table (≤100 GB for SF≤3K) | GPU RAM bounded by `--batch-rows` (default 15M rows) |
| Disk | dataset × 1 | dataset × 2 (source + output) |
| Container | none | pulls `rapidsai/base:26.06-cuda13-py3.13` |

Disk planning (lineitem + orders, Snappy, 48 files): SF100 ≈ 33 GiB, SF1K ≈ 330 GiB,
SF3K ≈ 1 TiB, SF10K ≈ 3.3 TiB **per dataset copy**. Approach B needs source + output;
Approach A needs only its output. Budget accordingly (both sides + source: SF3K ≈ 3 TiB).

## Step 0 — build this branch

```bash
git clone git@github.com:qbacpey/tpchgen-rs.git -b poc/sorted-tpch-gen tpchgen-sorted
cd tpchgen-sorted
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh   # if no cargo
export PATH="$HOME/.cargo/bin:$PATH"
cargo build --release -p tpchgen-sorted -p tpcgen-cli
# binaries: target/release/tpchgen-sorted, target/release/tpcgen-cli
```

Sanity check (must end with "verified ... complete"):

```bash
./target/release/tpchgen-sorted -s 1 -t lineitem -o /tmp/sanity -f 8 --verify
```

## Step 1 — generate the unsorted source datasets (needed by BOTH sides)

Approach B rewrites an existing dataset; Approach A is measured against the same writer.
Use this branch's `tpcgen-cli` so the writer/flags are identical on both sides:

```bash
for SF in 100 1000 3000; do
  /usr/bin/time -v ./target/release/tpcgen-cli tpch parquet \
    -s $SF -T lineitem -T orders -o /data/unsorted_sf$SF -p 48 \
    2>&1 | tee logs/unsorted_sf$SF.log
done
```

(`-p 48` = 48 files, matching Approach A's `-f 48`. Approach B preserves the source file
count 1:1, so this also fixes its output at 48 files.)

## Step 2 — Approach A: sorted generation (CPU)

Config rule: peak memory ≈ `rows_per_pass × ~250B + ~4GB`, where
`rows_per_pass = 6M × SF × files_per_pass / 48`. More files-per-pass = fewer sweeps = faster,
if RAM allows. Verified-measured configs:

| SF | `-f` | `--files-per-pass` | Sweeps | Est. peak RSS | Est. lineitem time |
|---|---|---|---|---|---|
| 100 | 48 | 8 | 6 | 27 GB (measured) | ~49 s (measured) |
| 1000 | 48 | 2 | 24 | ~66 GB | ~20 min |
| 1000 | 48 | 1 | 48 | ~35 GB | ~30 min |
| 3000 | 48 | 1 | 48 | ~98 GB | ~90 min |
| 10000 | 96 | 1 | 96 | ~160 GB | ~8 h (optional) |

```bash
SF=1000  # repeat per scale factor
/usr/bin/time -v ./target/release/tpchgen-sorted \
  -s $SF -t lineitem -o /data/sorted_sf$SF -f 48 --files-per-pass 2 --verify \
  2>&1 | tee logs/sorted_lineitem_sf$SF.log
/usr/bin/time -v ./target/release/tpchgen-sorted \
  -s $SF -t orders -o /data/sorted_sf$SF -f 48 --files-per-pass 2 --verify \
  2>&1 | tee logs/sorted_orders_sf$SF.log
```

- **Always keep `--verify`** — it reads the output back and proves sortedness, partition
  disjointness, and completeness against a stream-derived fingerprint. A run that does not
  end in "verified ... complete" is a failed run; capture the log and stop.
- If the process is OOM-killed, halve `--files-per-pass` and rerun (note it in the log).
- The tool writes `<output>/<table>/` subdirectories itself.

## Step 3 — Approach B: GPU rewrite (Greg's #385 scripts)

The PR is closed/unmerged; fetch its head:

```bash
git clone https://github.com/rapidsai/velox-testing.git
cd velox-testing
git fetch origin pull/385/head:pr-385 && git checkout pr-385
# files: benchmark_data_tools/sort_partition_table.py
#        benchmark_data_tools/scripts/sort_tpch_table.sh
```

Run it (wrapper clones sibling tables as symlinks, then rewrites orders + lineitem in a
RAPIDS container):

```bash
export RAID_MOUNT=/data          # host path bind-mounted into the container; the source dataset must live under it
export GPU_DEVICE=0
SF=1000
/usr/bin/time -v benchmark_data_tools/scripts/sort_tpch_table.sh \
  --source /data/unsorted_sf$SF --output /data/greg_sorted_sf$SF \
  2>&1 | tee logs/greg_sf$SF.log
```

Notes:

- `--dry-run` prints the partition plan without writing. `--resume` skips existing files.
- The wrapper forwards only `--resume`/`--dry-run`. For the identity-rewrite control or a
  smaller GPU-memory batch, call `sort_partition_table.py` inside the container directly:
  `python sort_partition_table.py --source-table ... --output-table ... --identity`
  or `--batch-rows 5000000`.
- His partition boundaries are **equal-day**, so `lineitem` output files will be visibly
  unbalanced (sparse ends of the ship-date domain). Expected; not an error.
- His script verifies row count/schema/codec consistency itself at the end.
- Expect tens of minutes to hours: the per-partition loop re-reads all source files
  (48 partitions = 48 full filtered scans). The log prints per-file scanned row counts —
  keep it, it demonstrates the read amplification directly.

## Step 4 — what to report back

Fill this table (wall = real time from `/usr/bin/time -v`; RSS = Maximum resident set size):

| SF | Table | A: unsorted gen (s) | A: sorted gen (s) | A: peak RSS | A: verify | B: GPU rewrite (s) | B: peak RSS | B: verify |
|---|---|---|---|---|---|---|---|---|
| 100 | lineitem | | | | | | | |
| 100 | orders | | | | | | | |
| 1000 | lineitem | | | | | | | |
| 1000 | orders | | | | | | | |
| 3000 | lineitem | | | | | | | |
| 3000 | orders | | | | | | | |

Plus, per dataset, the pruning-quality stat (run once per sorted output):

```bash
python3 - <<'EOF'
import pyarrow.parquet as pq, glob
files = sorted(glob.glob("/data/sorted_sf1000/lineitem/*.parquet"))
idx = pq.ParquetFile(files[0]).schema_arrow.names.index("l_shipdate")
spans = sorted((m.row_group(g).column(idx).statistics.max -
                m.row_group(g).column(idx).statistics.min).days
               for f in files for m in [pq.ParquetFile(f).metadata]
               for g in range(m.num_row_groups))
print(f"{len(spans)} row groups; span min {spans[0]}, median {spans[len(spans)//2]}, max {spans[-1]} days")
EOF
```

And the read-floor for Approach B's cost model (CPU-side lower bound):

```bash
python3 tpchgen-sorted/scripts/post_process_cost.py /data/unsorted_sf$SF/lineitem --partitions 48
```

**Rules:** never run the two sides concurrently (I/O contention); same file count (48) and
Snappy on both sides; keep all `logs/*.log` files.

## Reality check for SF10K / SF30K

- **SF10K** (60B rows, ~2.6 TiB lineitem): feasible only as `-f 96 --files-per-pass 1`
  (~160 GB peak, ~8 h). Sweep amplification is the cost: 96 sweeps × ~4 min of key-only
  rejection each.
- **SF30K** (180B rows, ~7.8 TiB): **not practical on one node** — one day is ~77M rows
  (~19 GB, the memory floor, fine) but ~2,345 day-granularity files means ~2,345 sweeps
  ≈ 20 days of rejection overhead. That scale needs partition-level sharding across
  runs/machines (day-range shards stitch byte-identically by design), which is **not
  implemented in this branch** — flag it as future work, don't attempt it by hand.

## Reference numbers already measured (24-core / 62 GB CPU-only box)

| SF | Unsorted | Sorted (ours) | #385 read floor (48 × full pass) |
|---|---|---|---|
| 10 | 4.77 s | 5.37 s | ≥ 164 s |
| 100 | 37.10 s | 48.65 s | ≥ 1,390 s |
| 300 | 101.65 s | 161.83 s | ≥ 4,220 s |

Full context: `tpchgen-sorted/SORTED_TPCH_BENCHMARK_REPORT.md` in this branch.
