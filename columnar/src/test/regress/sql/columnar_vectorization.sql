CREATE TABLE t (id int, ts timestamp) USING columnar;
INSERT INTO t SELECT id, now() + id * '1 day'::interval FROM generate_series(1, 100000) id;
EXPLAIN (costs off) SELECT * FROM t WHERE ts between '2026-01-01'::timestamp and '2026-02-01'::timestamp;
DROP TABLE t;

CREATE TABLE t (id int, ts timestamptz) USING columnar;
INSERT INTO t SELECT id, now() + id * '1 day'::interval FROM generate_series(1, 100000) id;
EXPLAIN (costs off) SELECT * FROM t WHERE ts between '2026-01-01'::timestamptz and '2026-02-01'::timestamptz;
DROP TABLE t;

SET max_parallel_workers_per_gather TO 0;

CREATE TABLE vector_not_null_agg (a int not null, b int not null) USING columnar;
INSERT INTO vector_not_null_agg SELECT i, i % 10 FROM generate_series(1, 1000) i;
SELECT SUM(a), AVG(b), COUNT(b) FROM vector_not_null_agg;
DROP TABLE vector_not_null_agg;

CREATE TABLE vector_null_agg (a int, b bigint) USING columnar;
INSERT INTO vector_null_agg VALUES (1, 10), (NULL, 20), (3, NULL), (NULL, NULL);
SELECT SUM(a), AVG(a), COUNT(a), SUM(b), AVG(b), COUNT(b), COUNT(*) FROM vector_null_agg;
DROP TABLE vector_null_agg;

RESET max_parallel_workers_per_gather;
