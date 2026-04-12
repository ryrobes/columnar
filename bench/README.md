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
