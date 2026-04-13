[![Hydra - the open source data warehouse](https://github.com/ryrobes/columnar/blob/main/header.png?raw=true)]

# 🐘🤘 Hydra Columnar (PG18 Fork)

This is a fork of the (apparently abandoned) [Hydra Columnar](https://github.com/hydradatabase/hydra)
PostgreSQL 14 extension. The upstream last saw meaningful activity in
early 2024; this fork picks it up and moves it forward.

## 🆕 What's Changed From Upstream

### PostgreSQL 18 Support

The biggest change: **the extension now compiles and runs on PostgreSQL 18.3**.
Upstream only supported PG13–16. The port touches ~20 API surfaces across
the extension:

- **`pg_version_constants.h`** — Added `PG_VERSION_17`/`PG_VERSION_18`, dropped PG13
- **`configure.in`** — Accepts PG14–PG18; dropped PG13
- **TableAM callbacks for PG17+** — `scan_analyze_next_block` uses the new
  `ReadStream *` signature
- **TableAM struct changes for PG18** — `scan_bitmap_next_block`/`_tuple`
  were removed from `TableAmRoutine`
- **`RelationCreateStorage`** — PG15+ gained a `register_delete` parameter
- **`pgstat_report_vacuum`** — PG18 added a `starttime` parameter
- **`vac_update_relstats`** — PG18 added `num_all_frozen_pages`
- **`ExecInitRangeTable`** — PG18 added `unpruned_relids` parameter
- **`index_parallelscan_initialize` / `index_beginscan_parallel`** — PG18
  added instrumentation parameters
- **`TupleHashEntryData.additional`** — Removed in PG18, replaced with
  `TupleHashEntryGetAdditional()` accessor in the vectorized aggregator
- **`BuildTupleHashTableExt` → `BuildTupleHashTable`** — merged in PG18
- **`ExecFreeExprContext` → `FreeExprContext`** — moved in PG18
- **`heap_inplace_update` → `simple_heap_update`** — PG18 removed the
  in-place variant
- **`commands/explain_format.h`** — New header in PG18 for EXPLAIN
  property functions
- **`MemoryContextResetAndDeleteChildren` → `MemoryContextDeleteChildren`**
- **`tupdesc->attrs[i]` → `TupleDescAttr(tupdesc, i)`** — enforced in PG18
- **11 `smgropen()` call sites** consolidated to `RelationGetSmgr(rel)` —
  eliminates the `BackendId → ProcNumber` concern from PG17 entirely

### DML Improvements — Columnar for General-Purpose Tables

Upstream Hydra recommended columnar only for append-only workloads. This
fork makes `UPDATE`/`DELETE` substantially more usable:

1. **Stripe-level locking** — Replaced the table-wide
   `pg_advisory_xact_lock_int8(storageId)` with per-stripe locks using a
   multiplicative hash of `(storageId, stripeId)`. Two sessions updating
   different stripes no longer block each other.

2. **Correct live-tuple counting** — `ColumnarTableTupleCount` now
   subtracts deleted rows, so `VACUUM` writes correct `pg_class.reltuples`
   and the planner gets accurate row estimates.

3. **Chunk group skip for fully-deleted groups** — When every row in a
   chunk group is marked deleted, the reader skips the entire group
   without decompressing column data or loading the row mask. Applied to
   both the scalar and vector read paths.

4. **No per-row `CommandCounterIncrement()`** — `UpdateRowMask` no longer
   calls CCI on every single delete. The in-memory write-state cache
   provides visibility within the command; the catalog-flush CCI handles
   inter-command visibility. Dramatically speeds up bulk `DELETE`s.

5. **`columnar_fetch_row_version` rewrite** — The old-row-fetch path used
   by PG18's `ExecUpdate` had several latent bugs that became crashes on
   PG18:
   - Used `slot->tts_tupleDescriptor` (which may project only WHERE-clause
     columns) instead of the full relation descriptor
   - Stale cached read state with narrow projection
   - Missing `ColumnarReadFlushPendingWrites()` call
   - Dangling varlena pointers into chunk data buffers

   Fixed by always projecting all columns, using
   `GetTransactionSnapshot()`, flushing pending writes, and copying the
   result into a self-contained `HeapTuple` via `heap_form_tuple` +
   `ExecForceStoreHeapTuple` before the read state is torn down.

6. **ANALYZE on PG17+** — The new `ReadStream`-based
   `scan_analyze_next_block` callback was implemented. Physical block
   count is tiny for compressed columnar tables, so the callback ensures a
   minimum of 1000 virtual blocks to collect a representative sample.

7. **Cached row-version reads for UPDATE** — The old-row-fetch path now
   reuses a transaction-scoped random-access read state when the target row
   is already in a flushed stripe. It also avoids flushing pending
   appended rows unless the requested row could be in an in-progress stripe.
   This removes the one-stripe-per-row UPDATE pathology and cuts the PG18
   smoke benchmark's columnar UPDATE from roughly **25-27s** to **0.099s**
   on the local x86_64 Docker test image.

8. **Conservative VACUUM safety for row-masked stripes** — Plain `VACUUM`
   no longer rewrites candidate stripes that contain row-mask deletions
   through the old compaction path. That path could drop live rows after a
   `DELETE` + `VACUUM`; now `VACUUM` preserves rows and still updates
   `pg_class.reltuples` correctly. Row-mask-aware physical compaction is
   left as future work rather than risking data loss.

9. **Vector aggregate null fast path** — Vector columns now track whether a
   batch contains any NULLs. Common aggregate kernels like `count(*)` and
   integer `sum` skip per-row null checks for NOT NULL batches. A focused
   1M-row NOT NULL aggregate test improved from **35.690ms** to
   **33.519ms** median, about **6.1% faster**.

### Build & Run — PG18 Docker Image

New self-contained build pipeline for PG18:

- **`Dockerfile.pg18`** — Multi-stage build that compiles both columnar
  and pgvector (v0.8.2) from source against PG18.3
- **`docker-compose.pg18.yml`** — Service config with columnar as the
  default `table_access_method` (so `CREATE TABLE foo(...)` creates
  columnar; `USING heap` overrides)
- **pgvector preinstalled** — `CREATE EXTENSION vector` works out of the
  box, with HNSW and IVFFlat indexes

Build and run:

```bash
docker compose -f docker-compose.pg18.yml up -d --build
psql postgresql://postgres:postgres@127.0.0.1:5432/postgres
```

### Benchmark Harness

The `bench/` harness was simplified and updated:

- Dropped `hybrid_hot_cold` and `hybrid_partitioned` layouts — no longer
  needed since columnar can handle DML directly
- Replaced the single `--vanilla-dsn` flag with repeatable
  `--compare label=dsn` so you can benchmark against any number of
  external Postgres instances simultaneously (Neon, Supabase, AlloyDB,
  vanilla PG14/15/16/17/18, etc.)
- Added `n_distinct` hints in `ANALYZE` for columns with known
  cardinalities — works around a PG18 quirk where `compute_scalar_stats`
  produces `n_distinct = -Infinity` for the columnar scan's sample,
  which would otherwise make the planner pick Sort+GroupAggregate
  instead of HashAggregate on GROUP BY queries
- Added `bench/vector_benchmark.py` for pgvector/pgvectorscale workloads.
  It compares exact search, pgvector HNSW, and pgvectorscale DiskANN
  when available, reports recall against exact top-k, and forces vector
  tables to `USING heap` so ANN indexes are tested on a reliable storage
  path.
- Added Docker targets for vector baselines:
  `image_pgvector_baseline` builds vanilla PG15/PG18 + pgvector, and
  `image_pgvectorscale_baseline` builds an optional PG18 +
  pgvectorscale image for DiskANN experiments.
- Added ClickHouse benchmark support. `--compare-clickhouse label=url`
  runs the same synthetic workload against a ClickHouse HTTP endpoint
  using a native `MergeTree` table, and the `Makefile` includes local
  start/stop and smoke/25M benchmark targets.
- Added AlloyDB Omni columnar-engine benchmark support.
  `--compare-alloydb-columnar label=dsn` expects an Omni instance started
  with `google_columnar_engine.enabled=on`, populates the benchmark query
  columns with `google_columnar_engine_add`, waits for the columnar job,
  records `g_columnar_relations`, adds `columnar_store` bytes to the size
  report, and records whether each executed plan used AlloyDB's
  `columnar scan`.

For reader/storage experiments, `columnar.enable_scan_diagnostics` can be
enabled for `EXPLAIN ANALYZE`. It is off by default and reports per-scan
vector filter selectivity, selected/deserialized chunk groups, loaded
column chunks, compressed bytes read, and decompressed value bytes. When
using it to reason about storage-layer work, disable parallel columnar
execution so the counters describe a single scan node:

```sql
SET columnar.enable_parallel_execution = off;
SET columnar.enable_scan_diagnostics = on;
EXPLAIN (ANALYZE, BUFFERS)
SELECT ...
FROM my_columnar_table
WHERE ...;
```

Example:
```bash
make bench_storage BENCH_ARGS="--rows 1000000 --query-runs 3 \
  --compare vanilla18=postgresql://user:pass@host/db \
  --compare alloydb=postgresql://user:pass@alloydb/db \
  --compare-alloydb-columnar alloydb_columnar=postgresql://user:pass@alloydb-columnar/db \
  --output tmp/bench.json"
```

### Regression Tests

Added `columnar_dml_improvements.sql` covering the DML fixes:
- Correct tuple count after `DELETE` + `VACUUM`
- Chunk group skip for fully-deleted groups
- Multi-stripe UPDATE/DELETE within a single transaction
- Bulk DML (10K+ rows), rollback correctness, interleaved DML+SELECT

`columnar_update_delete.sql` also includes a focused regression for
`DELETE` followed by plain `VACUUM`, which used to be able to hide every
live row in a row-masked stripe. `columnar_vectorization.sql` covers the
new vector aggregate NULL and NOT NULL fast paths.

## 🚀 Run Locally

### From the repo (dev workflow)

```bash
git clone https://github.com/ryrobes/columnar && cd columnar
docker compose -f docker-compose.pg18.yml up -d --build
psql postgresql://postgres:postgres@127.0.0.1:5432/postgres
```

### As a standalone image (no repo needed)

Once you've built and pushed the image to a registry, anyone (including
yourself on another machine) can run it with a single `docker run`:

```bash
docker run -d --name columnar-pg18 \
  -p 5432:5432 \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=postgres \
  -e COLUMNAR_DEFAULT_TABLE_ACCESS_METHOD=columnar \
  -v columnar_pg18_data:/var/lib/postgresql \
  ryrobes/hydra-columnar-pg18:latest

psql postgresql://postgres:postgres@127.0.0.1:5432/postgres
```

By default, `CREATE TABLE foo(...)` creates a columnar table. Use
`USING heap` for row-store tables. `CREATE EXTENSION vector` is also
already installed.

### 📦 Build & Publish Your Own Image

The `Makefile` has one-shot targets for the whole build/publish/run
cycle so you don't have to remember `docker build` flags:

```bash
# Build locally (tags as ryrobes/hydra-columnar-pg18:latest + :pg18)
make image_build

# Run the built image (no compose, no repo context needed)
make image_run

# Push to your registry (set IMAGE_REPO to your own)
make image_push IMAGE_REPO=yourname/hydra-columnar-pg18

# Multi-arch build (amd64 + arm64, pushes directly)
make image_build_multiarch IMAGE_REPO=yourname/hydra-columnar-pg18

# Clean up
make image_stop
```

To push to GitHub Container Registry instead of Docker Hub:

```bash
echo $GHCR_PAT | docker login ghcr.io -u yourname --password-stdin
make image_push IMAGE_REPO=ghcr.io/yourname/hydra-columnar-pg18
```

The resulting image is ~500 MB, self-contained, and includes:
- PostgreSQL 18.3
- columnar extension (this fork)
- pgvector 0.8.2
- auto-init: creates both extensions and sets
  `default_table_access_method = columnar` on first startup

## 💪 Benchmark Results

**25 million rows, 5 query runs each**, against vanilla PG15, vanilla
PG18, and AlloyDB. Medians in milliseconds, smaller is better. The
`heap` and `columnar` columns are this fork running on PG18.

Reproduce:
```bash
make bench_storage BENCH_ARGS="--rows 25000000 --query-runs 5 \
  --compare vanilla_pg15=postgresql://postgres:postgres@localhost:5415/postgres \
  --compare vanilla_pg18=postgresql://postgres:postgres@localhost:5418/postgres \
  --compare alloydb=postgresql://postgres:postgres@localhost:5434/postgres \
  --output tmp/comparison4.json"
```

### Storage & Write Path

| metric               | heap     | columnar  | vanilla_pg15 | vanilla_pg18 | alloydb   |
|----------------------|---------:|----------:|-------------:|-------------:|----------:|
| total_size_mb        | 6303.508 | **381.531** | 6303.508     | 6303.508     | 6330.477  |
| load_seconds         | 49.297   | 62.926    | 60.825       | 66.274       | 63.985    |
| append_total_seconds | 0.222    | 0.285     | 0.237        | 0.887        | 0.504     |
| update_seconds       | 0.900    | **0.248** | 1.397        | 0.924        | 2.186     |

Columnar hits **381 MB** vs ~6300 MB for row storage — a **~16.5x
compression ratio**.

The UPDATE path is now faster than the row-store comparison targets on
this benchmark's hot-slice update: **0.248s** for columnar vs **0.900s**
for this fork's heap layout and **0.924s** for vanilla PG18. The speedup
comes from keeping the PG18-correct old-row materialization path while
removing the avoidable per-row setup:

- Stripe-level advisory locking — concurrent UPDATEs to different
  stripes no longer block each other.
- `columnar_fetch_row_version` fetches a consistent full old tuple via
  `heap_form_tuple` + `ExecForceStoreHeapTuple` — no more use-after-free
  on varlena data.
- Cached row-version read states avoid per-row reader setup when old
  rows live in flushed stripes.
- Pending appended rows are flushed only when needed, so an UPDATE of
  100 existing rows leaves 2 stripes instead of 101.
- Correct `ColumnarTableTupleCount` so `VACUUM` writes accurate
  `pg_class.reltuples`.

On the smaller PG18 smoke benchmark (`50k` rows, `--cleanup`), the same
change moved the columnar UPDATE path from roughly **25-27s** to
**0.099s**. The 25M-row comparison above confirms that the improvement
holds at larger scale.

### Analytic Query Performance (medians, ms)

| query               | heap     | columnar    | vanilla_pg15 | vanilla_pg18 | alloydb    |
|---------------------|---------:|------------:|-------------:|-------------:|-----------:|
| count_all           | 872.410  | **41.821**  | 842.846      | 668.173      | 1204.778   |
| distinct_users      | 1160.017 | **850.007** | 2801.828     | 1096.335     | 1536.177   |
| filtered_count      | 862.658  | **115.859** | 1057.100     | 806.889      | 1354.271   |
| hot_work_slice      | 490.507  | **12.057**  | 586.343      | 397.260      | 910.522    |
| latency_rollup      | 933.343  | **182.509** | 1069.464     | 842.217      | 1411.207   |
| recent_window       | 483.517  | **11.829**  | 611.072      | 432.822      | 1101.584   |
| region_day_rollup   | 1689.014 | **501.875** | 1824.279     | 1639.293     | 2061.662   |
| search_phrase_topn  | 969.352  | **227.706** | 1154.421     | 908.530      | 1712.020   |
| service_topn        | 1337.433 | **430.010** | 1569.955     | 1335.628     | 1635.201   |
| tenant_error_rollup | 1707.181 | **535.808** | 1937.736     | 1668.978     | 2076.820   |
| url_like            | 1489.978 | **295.046** | 1564.546     | 1399.642     | 1763.450   |
| wide_sum            | 1715.764 | **530.874** | 1947.960     | 1682.641     | 2137.293   |

Columnar wins every single analytic query, typically **3-40x faster**
than heap / vanilla PG / AlloyDB while using a fraction of the disk.

### ClickHouse Comparison

Measured against `clickhouse/clickhouse-server:25.3`
(ClickHouse 25.3.14.14) over HTTP. The benchmark creates a native
`MergeTree` table with `ORDER BY event_id`, loads the same synthetic
rows, runs the same analytical query templates, and times ClickHouse
queries with `FORMAT Null`. PostgreSQL timings use `EXPLAIN ANALYZE`, so
the ClickHouse numbers include HTTP round-trip overhead.

Reproduce:
```bash
make clickhouse_bench_start
make bench_storage_clickhouse_25m
```

25 million rows, 3 query runs each:

| metric               | columnar | clickhouse |
|----------------------|---------:|-----------:|
| total_size_mb        | 381.531  | 824.585    |
| load_seconds         | 69.938   | 9.116      |
| append_total_seconds | 0.236    | 0.061      |
| update_seconds       | 0.277    | 1.398      |

| query               | columnar | clickhouse |
|---------------------|---------:|-----------:|
| count_all           | 71.725   | 2.810      |
| distinct_users      | 945.884  | 174.327    |
| filtered_count      | 204.053  | 26.500     |
| hot_work_slice      | 12.136   | 13.155     |
| latency_rollup      | 285.409  | 40.740     |
| recent_window       | 18.770   | 19.578     |
| region_day_rollup   | 745.971  | 93.131     |
| search_phrase_topn  | 374.358  | 115.689    |
| service_topn        | 536.723  | 39.862     |
| tenant_error_rollup | 672.875  | 129.698    |
| url_like            | 423.642  | 122.746    |
| wide_sum            | 711.325  | 9.609      |

ClickHouse is much faster on almost every read query and loads data much
faster. This fork's columnar storage is denser on this workload and its
hot-slice UPDATE path is faster than ClickHouse's synchronous mutation.
So the honest comparison is: ClickHouse is the read-performance target
to chase, while this fork is currently strongest when the goal is
PostgreSQL compatibility, compact storage, and transactional DML inside
Postgres.

### AlloyDB Omni Columnar Engine

The plain `--compare alloydb=...` target creates a normal Postgres row
table. That is useful as an Omni row-store baseline, but it does not
exercise Google's columnar engine unless the server is started with the
engine enabled and the table is populated into the column store. AlloyDB
Omni's docs describe those as separate steps:
[columnar engine overview](https://cloud.google.com/alloydb/omni/docs/columnar-engine/overview)
and
[columnar engine configuration](https://docs.cloud.google.com/alloydb/omni/containers/current/docs/columnar-engine/configure).

The harness now has an explicit target for that path:

```bash
make alloydb_columnar_bench_start
make bench_storage_alloydb_columnar_smoke
```

The Docker target starts `google/alloydbomni` with:

```bash
postgres -c google_columnar_engine.enabled=on \
  -c google_columnar_engine.memory_size_in_mb=2048
```

It also sets `--shm-size=2g`; without a larger `/dev/shm`, Omni can warn
about insufficient dynamic shared memory while generating columnar
formats inside Docker. The 25M-row target is:

```bash
make bench_storage_alloydb_columnar_25m
```

Smoke result from the local 50k-row check:

| metric               | columnar | alloydb | alloydb_columnar |
|----------------------|---------:|--------:|-----------------:|
| total_size_mb        | 0.797    | 14.555  | 18.680           |
| load_seconds         | 0.178    | 0.160   | 0.165            |
| append_total_seconds | 0.019    | 0.026   | 0.034            |
| update_seconds       | 0.119    | 0.070   | 0.084            |

| query               | columnar | alloydb | alloydb_columnar |
|---------------------|---------:|--------:|-----------------:|
| count_all           | 1.266    | 3.766   | 0.085            |
| distinct_users      | 4.097    | 5.685   | 0.842            |
| filtered_count      | 2.424    | 6.685   | 0.201            |
| hot_work_slice      | 2.967    | 5.392   | 0.383            |
| latency_rollup      | 3.795    | 7.030   | 0.399            |
| recent_window       | 5.548    | 10.653  | 0.791            |
| region_day_rollup   | 9.059    | 10.612  | 1.494            |
| search_phrase_topn  | 8.194    | 8.503   | 2.998            |
| service_topn        | 8.281    | 8.615   | 0.877            |
| tenant_error_rollup | 10.629   | 11.871  | 10.579           |
| url_like            | 6.377    | 9.074   | 1.317            |
| wide_sum            | 9.564    | 7.514   | 3.777            |

For that smoke run, `g_columnar_relations` reported `Usable`, all
1,855 table blocks were in the columnar cache, and all 12 benchmark
queries had executed plans containing AlloyDB's `columnar scan`. The
reported `alloydb_columnar` size includes the heap table plus the
columnar store, so its footprint is expected to be larger than the
plain row-store target.

### Upstream Hydra PG14 Comparison

Measured against `ghcr.io/hydradatabase/hydra:latest` on port `5499`.
That container reports PostgreSQL 14.13, upstream `columnar 11.1-12`,
and `default_table_access_method = columnar`; the benchmark's
`--compare` table creation therefore measures upstream Hydra columnar.

Reproduce:
```bash
python3 bench/local_storage_benchmark.py \
  --dsn postgresql://postgres:postgres@127.0.0.1:5432/postgres \
  --rows 5000000 --query-runs 3 --append-batches 5 --append-rows 10000 \
  --compare hydra_pg14_upstream=postgresql://postgres:postgres@127.0.0.1:5499/postgres \
  --cleanup --output tmp/hydra-pg14-5m.json
```

5 million rows, 3 query runs each:

| metric               | heap      | columnar | hydra_pg14_upstream |
|----------------------|----------:|---------:|--------------------:|
| total_size_mb        | 1260.734  | 76.328   | 76.328              |
| load_seconds         | 8.787     | 13.061   | 13.222              |
| append_total_seconds | 0.159     | 0.223    | 0.240               |
| update_seconds       | 0.261     | 0.184    | 0.121               |

| query               | heap    | columnar | hydra_pg14_upstream |
|---------------------|--------:|---------:|--------------------:|
| count_all           | 140.797 | 16.874   | 11.935              |
| distinct_users      | 443.647 | 149.021  | 144.727             |
| filtered_count      | 178.087 | 28.747   | 29.368              |
| hot_work_slice      | 86.550  | 6.729    | 6.724               |
| latency_rollup      | 187.201 | 39.684   | 43.380              |
| recent_window       | 101.900 | 8.893    | 9.805               |
| region_day_rollup   | 374.589 | 114.526  | 119.246             |
| search_phrase_topn  | 206.639 | 72.605   | 60.192              |
| service_topn        | 274.316 | 99.868   | 100.648             |
| tenant_error_rollup | 352.520 | 124.065  | 122.428             |
| url_like            | 296.673 | 71.376   | 76.692              |
| wide_sum            | 342.540 | 121.208  | 122.741             |

Read performance is still within a few percent of upstream Hydra PG14 on
most queries. The point of this fork is not a wholesale replacement of
upstream's read algorithm; the win is keeping comparable read/storage
behavior while adding PG18 support, safer DML, correct row counts and
VACUUM behavior, scan diagnostics, pgvector packaging, and reproducible
baseline benchmarks.

### Vector / ANN Experiment

The vector benchmark uses heap-backed vector tables on every target,
including the PG18 columnar image. This is intentional: ANN index access
methods should not be treated as reliable on columnar tables yet.

50k rows, 64 dimensions, 6 query vectors, 3 timing runs:

| target          | extension(s)                 | HNSW unfiltered | HNSW category | HNSW tenant | recall |
|-----------------|------------------------------|----------------:|--------------:|------------:|-------:|
| columnar_pg18   | vector 0.8.2                 | 0.357 ms        | 0.652 ms      | 1.874 ms    | 1.000  |
| pg18_vector     | vector 0.8.2                 | 0.620 ms        | 0.900 ms      | 2.262 ms    | 1.000  |
| pg15_vector     | vector 0.8.2                 | 0.451 ms        | 0.720 ms      | 1.891 ms    | 1.000  |
| alloydb         | vector 0.8.1.google-1        | 0.444 ms        | 0.720 ms      | 1.737 ms    | 1.000  |

An optional PG18 pgvectorscale image builds and runs DiskANN. On this
synthetic filtered workload, plain DiskANN was fast but needed tuning to
reach acceptable filtered recall, while label-aware DiskANN was rejected
for now because it produced poor recall on selective label filters even
with higher candidate/rescore settings.

### Takeaways

- **OLAP**: columnar is dramatically faster *and* uses ~16x less disk.
- **Vector search**: keep embedding tables heap-backed today; use the
  new vector benchmark to validate HNSW/DiskANN latency *and* recall
  before moving a workload to an ANN index.
- **Bulk hot-slice UPDATEs**: the cached row-version path makes columnar
  faster than heap and vanilla PG on this benchmark's update workload.
- **OLTP point updates**: heap can still be the right choice for very
  small, latency-sensitive updates because columnar still uses
  delete+insert semantics.
- **Correctness vs. shortcuts**: upstream's fast updates only worked
  because they skipped work that PG17+ requires. This fork keeps the
  correct old-row materialization path, but now caches the expensive
  reader state and avoids unnecessary write flushing. Reads are within a
  few percent of upstream, so if you're on PG14 today the reasons to
  move are PG18 support, safe concurrent writes, correct planner stats,
  no table-level advisory lock on DML, and much better UPDATE behavior
  than the first PG18-safe implementation.

For the upstream ClickBench results, see
[BENCHMARKS](https://github.com/hydradatabase/hydra/blob/main/BENCHMARKS.md).

## 🙋 FAQs

### Q: What's different between this fork and upstream Hydra Columnar?

A: See "What's Changed From Upstream" above. Short version: PG18
support, DML works without the append-only caveats, cleaner Docker
build with pgvector included, and a better benchmark harness.

### Q: Can columnar be the default table type?

A: Yes. The `docker-compose.pg18.yml` sets
`default_table_access_method = columnar`, so `CREATE TABLE` creates
columnar tables by default. Use `USING heap` to opt into row storage
for OLTP tables.

### Q: What Postgres features are unsupported on columnar?

- Logical replication
- Columnar tables support `btree` and `hash` indexes only (no GiST,
  GIN, SP-GiST, BRIN)
- No `SELECT FOR SHARE`/`FOR UPDATE`
- No serializable isolation level
- No foreign keys, unique, or exclusion constraints on columnar tables
- No `AFTER ROW` triggers
- `UNLOGGED` tables not supported
- TOAST: columnar stores large values inline rather than in a TOAST
  relation

### Q: Known limitations / TODOs

- PG18 `ANALYZE` produces `n_distinct = -Infinity` for columnar tables
  due to how the `ReadStream`-based sampling interacts with
  `compute_scalar_stats`. Workaround: set `n_distinct` explicitly via
  `ALTER TABLE ... ALTER COLUMN ... SET (n_distinct = N)` for columns
  with known cardinalities, or the planner picks bad GROUP BY
  strategies. The benchmark harness does this automatically.
- `UPDATE` on columnar tables still uses delete+insert semantics. The
  row-version read-state cache makes bulk updates fast, but columnar
  updates still create new columnar rows, so benchmark point-update
  latency before using it for OLTP-heavy workloads.
- Plain `VACUUM` is intentionally conservative for row-masked stripes.
  It preserves live rows and updates `reltuples`, but physical
  row-mask-aware stripe compaction still needs a dedicated safe rewrite
  path.
- `vacuum_columnar_table()` UDF is intentionally conservative — it
  relocates one stripe per call. Run in a loop for full compaction.

## :technologist: Developer Resources

- `bench/README.md` — benchmark harness docs
- Regression tests in `columnar/src/test/regress/sql/`
- [Upstream CHANGELOG](https://github.com/hydradatabase/columnar/blob/main/CHANGELOG.md)
  for pre-fork history

## 📝 License

- [AGPL 3.0](columnar/LICENSE) for the columnar extension
- All other code is Apache 2.0
- Upstream copyrights preserved (Citus Data and Hydra, Inc.)

Built on top of PostgreSQL (the Postgres license) and includes pgvector
(Postgres license) in the Docker image.
