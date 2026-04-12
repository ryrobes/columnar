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

## 🚀 Run Locally

```bash
git clone https://github.com/ryrobes/columnar && cd columnar
docker compose -f docker-compose.pg18.yml up -d --build
psql postgresql://postgres:postgres@127.0.0.1:5432/postgres
```

By default, `CREATE TABLE foo(...)` creates a columnar table. Use
`USING heap` for row-store tables.

## 💪 Benchmark Results

Against vanilla PG15, PG18, and AlloyDB on a 1.4M-row analytic dataset
(Dec 2026):

| metric | heap | columnar | vanilla15 | vanilla18 | alloydb |
|---|---|---|---|---|---|
| total_size_mb | 1260 | **76** | 1260 | 1260 | 1265 |
| count_all (ms) | 136 | **12** | 169 | 143 | 109 |
| distinct_users (ms) | 434 | **146** | 547 | 453 | 523 |
| filtered_count (ms) | 176 | **26** | 210 | 174 | 190 |
| hot_work_slice (ms) | 97 | **7** | 118 | 88 | 95 |
| latency_rollup (ms) | 188 | **39** | 214 | 179 | 198 |
| recent_window (ms) | 93 | **9** | 123 | 102 | 100 |
| region_day_rollup (ms) | 335 | **7** | 364 | 321 | 307 |
| search_phrase_topn (ms) | 207 | **70** | 243 | 204 | 220 |
| service_topn (ms) | 266 | **7** | 313 | 272 | 250 |
| tenant_error_rollup (ms) | 344 | **8** | 392 | 347 | 329 |
| url_like (ms) | 293 | **71** | 314 | 282 | 255 |
| wide_sum (ms) | 349 | **117** | 384 | 352 | 338 |

Columnar wins on all 12 analytic queries, often 5-50x faster, and uses
~16x less disk space thanks to compression.

Update latency is still higher than heap (columnar uses delete+insert
semantics), so heap remains the right choice for high-frequency OLTP
mutation patterns. But for general-purpose tables, columnar is now a
viable default.

For the full ClickBench benchmark from the upstream project, see
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
- `UPDATE` on columnar tables is still much slower than heap
  (delete+insert semantics). Stripe-level locking helps concurrency,
  but update throughput won't match heap.
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
