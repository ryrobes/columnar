# Local Storage Layout Benchmark

This directory contains a local benchmark harness for comparing Hydra
storage layouts on one instance:

* `heap`
* `columnar`
* `hybrid_hot_cold`
* `hybrid_partitioned`

It can also connect to a second Postgres-compatible server and report a
normal-table baseline as `vanilla_postgres` in the same output matrix.

The harness uses synthetic event/log data and a query mix inspired by
[ClickBench](https://github.com/ClickHouse/ClickBench). It is meant to
be a fast local decision tool, not a replacement for Hydra's larger
release benchmarking.

## What It Measures

The runner measures:

* initial bulk load time
* hybrid transition time:
  * hot/cold archive from heap to columnar
  * partition conversion from heap partitions to columnar partitions
* `ANALYZE` time
* logical table size, including a cross-layout `total_size_mb` matrix row
* analytical query timings via `EXPLAIN (ANALYZE, FORMAT JSON)`
* append batch timing
* a hot-slice `UPDATE` timing

That mix is deliberate: the analytical queries mirror the OLAP side of
Hydra, while append and update timings show where a heap hot path can
protect mutable workloads from columnar mutation costs.

## Run It

The runner uses `DATABASE_URL` if set. Otherwise it builds a DSN from
the repo-local `.env` file. Pass `--vanilla-dsn` or
`BENCH_VANILLA_DSN` to include the secondary vanilla baseline.

Smoke test:

```bash
make bench_storage_smoke
```

Larger run:

```bash
make bench_storage BENCH_ARGS="--rows 1000000 --query-runs 3 --output tmp/storage-bench.json"
```

Larger run with a secondary vanilla Postgres-compatible server:

```bash
make bench_storage \
  BENCH_VANILLA_DSN="postgresql://postgres:postgres@127.0.0.1:55432/postgres" \
  BENCH_ARGS="--rows 1000000 --query-runs 3 --output tmp/storage-bench.json"
```

Direct invocation:

```bash
python3 bench/local_storage_benchmark.py \
  --dsn postgresql://postgres:notofox@127.0.0.1:5432/postgres \
  --vanilla-dsn postgresql://postgres:postgres@127.0.0.1:55432/postgres \
  --rows 500000 \
  --query-runs 3 \
  --output tmp/storage-bench.json
```

## Notes

* The benchmark keeps schemas after the run so you can inspect plans and
  storage manually. Add `--cleanup` if you want them dropped.
* `hybrid_hot_cold` is implemented with plain SQL `INSERT ... SELECT`
  plus `DELETE`, and `hybrid_partitioned` uses plain SQL
  `DETACH PARTITION` plus `columnar.alter_table_set_access_method(...)`
  plus `ATTACH PARTITION`, so the harness works on older Hydra
  instances too.
* The `vanilla_postgres` target intentionally uses normal PostgreSQL
  table storage but reports it as a vanilla baseline, not as another
  Hydra layout.
* Query timings are warm-cache timings inside one running instance.
  They are best used for layout comparison on the same machine, not for
  publishing absolute numbers.
* Every layout runs the same analytical query file. The console summary
  now includes a per-query cross-layout table so you can compare like
  for like instead of only seeing each layout's slowest queries.
