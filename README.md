[![Hydra - the open source data warehouse](https://raw.githubusercontent.com/hydradatabase/hydra/main/.images/header.png)](https://hydra.so)

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

Example:
```bash
make bench_storage BENCH_ARGS="--rows 1000000 --query-runs 3 \
  --compare vanilla18=postgresql://user:pass@host/db \
  --compare alloydb=postgresql://user:pass@alloydb/db \
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
PG18, AlloyDB, and the original Hydra PG14 columnar extension. Medians
in milliseconds, smaller is better. Asterisks (\*) mark this fork.

Reproduce:
```bash
make bench_storage BENCH_ARGS="--rows 25000000 --query-runs 5 \
  --compare vanilla_pg15=postgresql://postgres:postgres@localhost:5415/postgres \
  --compare vanilla_pg18=postgresql://postgres:postgres@localhost:5418/postgres \
  --compare alloydb=postgresql://postgres:postgres@localhost:5434/postgres \
  --compare older_hydra_pg14=postgresql://postgres:postgres@localhost:5499/postgres \
  --output tmp/bench.json"
```

### Storage & Write Path

| metric              | hydra18_heap\* | hydra18_columnar\* | vanilla_pg15 | vanilla_pg18 | alloydb | older_hydra_pg14 |
|---------------------|---------------:|-------------------:|-------------:|-------------:|--------:|-----------------:|
| total_size_mb       | 6303.5         | **381.5**          | 6303.5       | 6303.5       | 6330.5  | 381.5            |
| load_seconds        | 47.7           | 62.8               | 58.6         | 56.2         | 62.7    | 64.5             |
| append_seconds      | 0.68           | 0.29               | 0.19         | 0.22         | 0.54    | 0.32             |
| update_seconds      | 0.86           | 9.48               | 1.39         | 0.89         | 1.38    | 0.16             |

Both columnar variants (ours on PG18, upstream on PG14) hit the same
**381 MB** footprint vs ~6300 MB for row storage — a **~16.5x
compression ratio**.

**About those update numbers.** Upstream's 0.16s update time on PG14
looks impressive but is misleading. The old implementation:

- Held a **table-wide advisory lock** for every UPDATE, serializing all
  concurrent writers on the same relation.
- Tombstoned the old row (flipped a bit in the row mask, inserted a
  new row) without fetching the old tuple's full column values, which
  worked on PG14's simpler executor but **crashes on PG18** — PG18's
  `ExecUpdate` calls `table_tuple_fetch_row_version` to materialize old
  column values for the new tuple projection, and the upstream
  implementation returned dangling varlena pointers into chunk data
  buffers.
- Produced incorrect `reltuples` because deleted rows weren't
  subtracted, distorting planner estimates.

The 9.5s update number above is from the first correct PG18 DML
implementation before row-version read-state caching. That version was
safe, but it rebuilt a random-access columnar reader for every updated
row and forced pending writes into many tiny stripes. The current
implementation remains **correct and concurrency-safe on PG18** while
removing that avoidable cost:

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

On the local PG18 smoke benchmark (`50k` rows, `--cleanup`), the
columnar UPDATE path now runs in **0.099s**, versus roughly **25-27s**
before the cache. A full 25M-row benchmark should be rerun before
replacing the table above, but the targeted experiment removes the
dominant DML overhead without weakening correctness.

### Analytic Query Performance (medians, ms)

| query               | hydra18_heap\* | hydra18_columnar\* | vanilla_pg15 | vanilla_pg18 | alloydb  | older_hydra_pg14 |
|---------------------|---------------:|-------------------:|-------------:|-------------:|---------:|-----------------:|
| count_all           | 656            | **42**             | 842          | 673          | 537      | 46               |
| distinct_users      | 1136           | **811**            | 2799         | 1113         | 1115     | 747              |
| filtered_count      | 869            | **112**            | 1051         | 845          | 960      | 130              |
| hot_work_slice      | 401            | **10**             | 578          | 421          | 458      | 11               |
| latency_rollup      | 893            | **178**            | 1064         | 885          | 990      | 244              |
| recent_window       | 428            | **12**             | 603          | 461          | 480      | 13               |
| region_day_rollup   | 1653           | **495**            | 1834         | 1669         | 1568     | 502              |
| search_phrase_topn  | 943            | **225**            | 1135         | 974          | 960      | 218              |
| service_topn        | 1319           | **419**            | 1561         | 1354         | 1244     | 455              |
| tenant_error_rollup | 1671           | **528**            | 1935         | 1675         | 1646     | 532              |
| url_like            | 1460           | **296**            | 1573         | 1414         | 1269     | 291              |
| wide_sum            | 1673           | **527**            | 1917         | 1743         | 1694     | 517              |

Columnar wins every single analytic query, typically **3-40x faster**
than heap / vanilla PG / AlloyDB. Our PG18 port and the original
Hydra PG14 columnar perform within a few percent of each other on
reads — confirming we haven't regressed anything while making DML
work correctly on PG18.

### Takeaways

- **OLAP**: columnar is dramatically faster *and* uses ~16x less disk.
- **OLTP point updates**: heap is still the right choice — columnar's
  delete+insert semantics mean single-row UPDATEs will always be
  slower than heap's in-place updates.
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
- `UPDATE` on columnar tables still uses delete+insert semantics, so
  heap remains the right choice for OLTP-heavy point-update workloads.
  The row-version read-state cache removes the worst per-row overhead
  for bulk updates, but columnar updates still create new columnar rows.
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
