# Local Storage Layout Benchmark

Benchmarks columnar vs heap storage on the Hydra-fork PG18 instance,
and optionally compares against external Postgres-compatible servers.

## What It Measures

* initial bulk load time
* `ANALYZE` time
* logical table size
* analytical query timings via `EXPLAIN (ANALYZE, FORMAT JSON)`
* append batch timing
* a hot-slice `UPDATE` timing (columnar now supports DML directly)

## Quick Start

Make sure the PG18 container is running:

```bash
docker compose -f docker-compose.pg18.yml up -d
```

Smoke test (fast, small dataset):

```bash
make bench_storage_smoke
```

Full run:

```bash
make bench_storage BENCH_ARGS="--rows 1000000 --query-runs 3 --output tmp/storage-bench.json"
```

## Comparing Against External Postgres Instances

Use `--compare label=dsn` to benchmark one or more external servers
alongside the Hydra instance. Each gets the same data, queries, and
workload:

```bash
make bench_storage BENCH_ARGS="--rows 500000 --query-runs 3 \
  --compare neon=postgresql://user:pass@neon-host/db \
  --compare supabase=postgresql://user:pass@supabase-host/db \
  --output tmp/comparison.json"
```

Direct invocation:

```bash
python3 bench/local_storage_benchmark.py \
  --dsn postgresql://postgres:postgres@127.0.0.1:5418/testdb \
  --compare vanilla=postgresql://postgres:postgres@127.0.0.1:5432/postgres \
  --rows 500000 \
  --query-runs 3 \
  --output tmp/storage-bench.json
```

## Layouts

On the Hydra instance (default `--layouts heap,columnar`):

* **heap** -- standard PostgreSQL row storage
* **columnar** -- columnar storage with compression, chunk group filtering,
  and vectorized aggregation

External servers (via `--compare`) always use the server's default table
storage (typically heap).

## Notes

* Benchmark schemas are kept after the run for manual inspection.
  Add `--cleanup` to drop them.
* Query timings are warm-cache timings. Use them for relative layout
  comparison, not absolute numbers.
* The console summary includes a per-query cross-layout matrix so you
  can compare like for like across all targets.

## Vector Benchmark

`vector_benchmark.py` exercises pgvector and, when available,
pgvectorscale. It uses deterministic synthetic embeddings, records
`EXPLAIN (ANALYZE, FORMAT JSON)` execution time, and reports recall
against exact top-k results with indexes disabled.

The benchmark intentionally creates the vector table `USING heap`.
The PG18 columnar image defaults to columnar storage, but ANN index
access methods should stay on heap-backed vector tables for now.

Build vanilla PG + pgvector baselines:

```bash
make image_pgvector_baseline PG_MAJOR=18
make image_pgvector_baseline PG_MAJOR=15
```

Build an optional PG18 pgvector + pgvectorscale image for DiskANN:

```bash
make image_pgvectorscale_baseline PG_MAJOR=18
```

Run a 50k-row comparison against local targets:

```bash
make bench_vector VECTOR_BENCH_ARGS="\
  --target columnar_pg18=postgresql://postgres:postgres@127.0.0.1:5432/postgres \
  --target pg18_vector=postgresql://postgres:postgres@127.0.0.1:5518/postgres \
  --target pg15_vector=postgresql://postgres:postgres@127.0.0.1:5515/postgres \
  --target alloydb=postgresql://postgres:postgres@127.0.0.1:5434/postgres \
  --rows 50000 --dims 64 --queries 6 --query-runs 3 \
  --variants exact,hnsw,diskann,diskann_labels \
  --cleanup --output tmp/vector-baseline-50k.json"
```

Useful ANN tuning flags:

* `--maintenance-work-mem`: index-build memory for HNSW; default is
  conservative so fresh Docker containers do not need a larger
  `/dev/shm`.
* `--hnsw-ef-search` and `--hnsw-max-scan-tuples`: pgvector HNSW recall
  and filtered-scan knobs.
* `--diskann-num-neighbors`, `--diskann-build-search-list-size`,
  `--diskann-query-search-list-size`, and `--diskann-query-rescore`:
  pgvectorscale DiskANN build/query recall knobs.
* `--candidate-multiplier`: for ANN variants, fetch
  `top_k * multiplier` candidates in a materialized CTE and rerank them
  exactly before returning top-k.

Local 50k-row findings:

* pgvector HNSW was correct on PG15, PG18, AlloyDB, and the columnar
  PG18 image when the vector table was forced to heap storage.
* HNSW median latency was sub-millisecond for unfiltered/category
  searches and around 1.5-2.4 ms for the 1/128 tenant filter.
* pgvectorscale DiskANN built and ran, but label-aware DiskANN produced
  poor recall on the selective label workload even after higher
  candidate/rescore settings. Keep it as an experiment until workload
  data proves recall is acceptable.
