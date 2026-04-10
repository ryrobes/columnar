# Introduction

Citus Columnar offers a per-table option for columnar storage to
reduce IO requirements though compression and projection pushdown.

# Design Trade-Offs

Existing PostgreSQL row tables work well for OLTP:

* Support `UPDATE`/`DELETE` efficiently
* Efficient single-tuple lookups

The Citus Columnar tables work best for analytic or DW workloads:

* Compression
* Doesn't read unnecessary columns
* Efficient `VACUUM`

# Next generation of cstore_fdw

Citus Columnar is the next generation of
[cstore_fdw](https://github.com/citusdata/cstore_fdw/).

Benefits of Citus Columnar over cstore_fdw:

* Citus Columnar is based on the [Table Access Method
  API](https://www.postgresql.org/docs/current/tableam.html), which
  allows it to behave exactly like an ordinary heap (row) table for
  most operations.
* Supports Write-Ahead Log (WAL).
* Supports ``ROLLBACK``.
* Supports physical replication.
* Supports recovery, including Point-In-Time Restore (PITR).
* Supports ``pg_dump`` and ``pg_upgrade`` without the need for special
  options or extra steps.
* Better user experience; simple ``USING``clause.
* Supports more features that work on ordinary heap (row) tables.

# Limitations

* Append-only (no ``UPDATE``/``DELETE`` support)
* No space reclamation (e.g. rolled-back transactions may still
  consume disk space)
* No bitmap index scans
* No tidscans
* No sample scans
* No TOAST support (large values supported inline)
* No support for [``ON
  CONFLICT``](https://www.postgresql.org/docs/12/sql-insert.html#SQL-ON-CONFLICT)
  statements (except ``DO NOTHING`` actions with no target specified).
* No support for tuple locks (``SELECT ... FOR SHARE``, ``SELECT
  ... FOR UPDATE``)
* No support for serializable isolation level
* Support for PostgreSQL server versions 12+ only
* No support for foreign keys, unique constraints, or exclusion
  constraints
* No support for logical decoding
* No support for intra-node parallel scans
* No support for ``AFTER ... FOR EACH ROW`` triggers
* No `UNLOGGED` columnar tables

Future iterations will incrementally lift the limitations listed above.

# User Experience

Create a Columnar table by specifying ``USING columnar`` when creating
the table.

```sql
CREATE TABLE my_columnar_table
(
    id INT,
    i1 INT,
    i2 INT8,
    n NUMERIC,
    t TEXT
) USING columnar;
```

Insert data into the table and read from it like normal (subject to
the limitations listed above).

To see internal statistics about the table, use ``VACUUM
VERBOSE``. Note that ``VACUUM`` (without ``FULL``) is much faster on a
columnar table, because it scans only the metadata, and not the actual
data.

## Options

Set options using:

```sql
alter_columnar_table_set(
    relid REGCLASS,
    chunk_group_row_limit INT4 DEFAULT NULL,
    stripe_row_limit INT4 DEFAULT NULL,
    compression NAME DEFAULT NULL,
    compression_level INT4)
```

For example:

```sql
SELECT alter_columnar_table_set(
    'my_columnar_table',
    compression => 'none',
    stripe_row_limit => 10000);
```

The following options are available:

* **compression**: `[none|pglz|zstd|lz4|lz4hc]` - set the compression type
  for _newly-inserted_ data. Existing data will not be
  recompressed/decompressed. The default value is `zstd` (if support
  has been compiled in).
* **compression_level**: ``<integer>`` - Sets compression level. Valid
  settings are from 1 through 19. If the compression method does not
  support the level chosen, the closest level will be selected
  instead.
* **stripe_row_limit**: ``<integer>`` - the maximum number of rows per
  stripe for _newly-inserted_ data. Existing stripes of data will not
  be changed and may have more rows than this maximum value. The
  default value is `150000`.
* **chunk_group_row_limit**: ``<integer>`` - the maximum number of rows per
  chunk for _newly-inserted_ data. Existing chunks of data will not be
  changed and may have more rows than this maximum value. The default
  value is `10000`.

## Hybrid Storage Patterns

For mixed OLTP / OLAP workloads, a practical pattern is to keep recent
or mutable data in heap tables and use columnar for sealed or archival
data.

One option is mixed-access-method partitioning on a single parent
table:

```sql
CREATE TABLE events (
    event_time timestamptz,
    run_id bigint,
    payload jsonb
) PARTITION BY RANGE (event_time);

CREATE TABLE events_2026_04_10
  PARTITION OF events
  FOR VALUES FROM ('2026-04-10') TO ('2026-04-11')
  USING heap;

CREATE TABLE events_2026_04_09
  PARTITION OF events
  FOR VALUES FROM ('2026-04-09') TO ('2026-04-10')
  USING columnar;
```

Another option is a hot/cold split where the hot table stays mutable in
heap and completed work is archived into a cold columnar table.

```sql
CREATE TABLE run_state_hot (
    run_id bigint,
    step_no int,
    status text,
    finished_at timestamptz
);

CREATE TABLE run_state_cold
  (LIKE run_state_hot INCLUDING DEFAULTS)
  USING columnar;

SELECT columnar.create_hot_cold_view(
    'public.run_state_read',
    'run_state_hot',
    'run_state_cold');

SELECT columnar.archive_to_cold(
    'run_state_hot',
    'run_state_cold',
    'run_id = 42');
```

`columnar.archive_to_cold` can also be used with `delete_from_hot =>
false` to copy rows into a columnar mirror before a separate prune step.

### Policy-Driven Hot/Cold Archival

For repeated archival workflows, the extension also provides a small
metadata layer in the `columnar` schema.

```sql
CREATE OR REPLACE FUNCTION app.finished_run_selector(
    policy_name text,
    hot_table regclass,
    cold_table regclass)
RETURNS text
LANGUAGE plpgsql
AS $$
DECLARE
    ready_run_ids text;
BEGIN
    EXECUTE format(
        'SELECT string_agg(run_id::text, '','' ORDER BY run_id) ' ||
        'FROM (SELECT DISTINCT run_id FROM %s WHERE finished_at IS NOT NULL) ready',
        hot_table)
        INTO ready_run_ids;

    IF ready_run_ids IS NULL THEN
        RETURN NULL;
    END IF;

    RETURN format('run_id IN (%s)', ready_run_ids);
END;
$$;

SELECT columnar.create_archive_policy(
    'run_state_policy',
    'app.run_state_hot',
    'app.run_state_cold',
    'app.finished_run_selector(text,regclass,regclass)',
    'app.run_state_read');

SELECT columnar.run_archive_policy('run_state_policy');

SELECT * FROM columnar.run_archive_policies();
```

Policies are stored in `columnar.archive_policy`, and successful runs are
logged in `columnar.archive_run_log`.

### Policy-Driven Time Partitioning

For range-partitioned time-series tables, the extension can also
manage mixed-access-method partitions. The current policy layer
supports a single `RANGE` partition key on `date`, `timestamp`, or
`timestamptz`, and expects at least one existing partition so it can
extend the current partition series.

```sql
CREATE TABLE app.events (
    event_time timestamptz not null,
    run_id bigint not null,
    payload jsonb
) PARTITION BY RANGE (event_time);

CREATE TABLE app.events_2026_04_10
  PARTITION OF app.events
  FOR VALUES FROM ('2026-04-10 00:00:00+00') TO ('2026-04-11 00:00:00+00')
  USING heap;

CREATE TABLE app.events_2026_04_11
  PARTITION OF app.events
  FOR VALUES FROM ('2026-04-11 00:00:00+00') TO ('2026-04-12 00:00:00+00')
  USING heap;

SELECT columnar.create_partition_policy(
    'events_daily',
    'app.events',
    '1 day',
    '12 hours',
    2);

SELECT * FROM columnar.run_partition_policy(
    'events_daily',
    '2026-04-11 12:00:00+00');

SELECT * FROM columnar.run_partition_policies();
```

In this example, partitions whose upper bound is at least `12 hours`
older than the reference time are rewritten to columnar, and the policy
keeps two future heap partitions ready ahead of the current partition.
Policies are stored in `columnar.partition_policy`, and successful runs
are logged in `columnar.partition_run_log`.

### Manual Example

If you want to do the same thing by hand without the metadata layer:

```sql
CREATE TABLE app.run_state_hot (
    run_id bigint,
    step_no int,
    status text,
    finished_at timestamptz
) USING heap;

CREATE TABLE app.run_state_cold
  (LIKE app.run_state_hot INCLUDING DEFAULTS)
  USING columnar;

SELECT columnar.create_hot_cold_view(
    'app.run_state_read',
    'app.run_state_hot',
    'app.run_state_cold');

INSERT INTO app.run_state_hot VALUES
    (42, 1, 'done',    '2026-04-10 12:00:00+00'),
    (42, 2, 'done',    '2026-04-10 12:00:00+00'),
    (43, 1, 'running', NULL);

SELECT columnar.archive_to_cold(
    'app.run_state_hot',
    'app.run_state_cold',
    'run_id = 42 AND finished_at IS NOT NULL');
```

For time-partitioned tables, the equivalent manual flow is to create
new heap partitions ahead of time and convert sealed partitions one at a
time:

```sql
CREATE TABLE app.events (
    event_time timestamptz not null,
    run_id bigint not null,
    payload jsonb
) PARTITION BY RANGE (event_time);

CREATE TABLE app.events_2026_04_10
  PARTITION OF app.events
  FOR VALUES FROM ('2026-04-10 00:00:00+00') TO ('2026-04-11 00:00:00+00')
  USING heap;

CREATE TABLE app.events_2026_04_11
  PARTITION OF app.events
  FOR VALUES FROM ('2026-04-11 00:00:00+00') TO ('2026-04-12 00:00:00+00')
  USING heap;

CREATE TABLE app.events_2026_04_12
  PARTITION OF app.events
  FOR VALUES FROM ('2026-04-12 00:00:00+00') TO ('2026-04-13 00:00:00+00')
  USING heap;

ALTER TABLE app.events
  DETACH PARTITION app.events_2026_04_10;

SELECT columnar.alter_table_set_access_method(
    'app.events_2026_04_10',
    'columnar');

ALTER TABLE app.events
  ATTACH PARTITION app.events_2026_04_10
  FOR VALUES FROM ('2026-04-10 00:00:00+00') TO ('2026-04-11 00:00:00+00');
```

View options for all tables with:

```sql
SELECT * FROM columnar.options;
```

You can also adjust options with a `SET` command of one of the
following GUCs:

* `columnar.compression`
* `columnar.compression_level`
* `columnar.stripe_row_limit`
* `columnar.chunk_group_row_limit`

GUCs only affect newly-created *tables*, not any newly-created
*stripes* on an existing table.

## Partitioning

Columnar tables can be used as partitions; and a partitioned table may
be made up of any combination of row and columnar partitions.

```sql
CREATE TABLE parent(ts timestamptz, i int, n numeric, s text)
  PARTITION BY RANGE (ts);

-- columnar partition
CREATE TABLE p0 PARTITION OF parent
  FOR VALUES FROM ('2020-01-01') TO ('2020-02-01')
  USING COLUMNAR;
-- columnar partition
CREATE TABLE p1 PARTITION OF parent
  FOR VALUES FROM ('2020-02-01') TO ('2020-03-01')
  USING COLUMNAR;
-- row partition
CREATE TABLE p2 PARTITION OF parent
  FOR VALUES FROM ('2020-03-01') TO ('2020-04-01');

INSERT INTO parent VALUES ('2020-01-15', 10, 100, 'one thousand'); -- columnar
INSERT INTO parent VALUES ('2020-02-15', 20, 200, 'two thousand'); -- columnar
INSERT INTO parent VALUES ('2020-03-15', 30, 300, 'three thousand'); -- row
```

When performing operations on a partitioned table with a mix of row
and columnar partitions, take note of the following behaviors for
operations that are supported on row tables but not columnar
(e.g. ``UPDATE``, ``DELETE``, tuple locks, etc.):

* If the operation is targeted at a specific row partition
  (e.g. ``UPDATE p2 SET i = i + 1``), it will succeed; if targeted at
  a specified columnar partition (e.g. ``UPDATE p1 SET i = i + 1``),
  it will fail.
* If the operation is targeted at the partitioned table and has a
  ``WHERE`` clause that excludes all columnar partitions
  (e.g. ``UPDATE parent SET i = i + 1 WHERE ts = '2020-03-15'``), it
  will succeed.
* If the operation is targeted at the partitioned table, but does not
  exclude all columnar partitions, it will fail; even if the actual
  data to be updated only affects row tables (e.g. ``UPDATE parent SET
  i = i + 1 WHERE n = 300``).

Note that Citus Columnar supports `btree` and `hash `indexes (and
the constraints requiring them) but does not support `gist`, `gin`,
`spgist` and `brin` indexes.
For this reason, if some partitions are columnar and if the index is
not supported by Citus Columnar, then it's impossible to create indexes
on the partitioned (parent) table directly. In that case, you need to
create the index on the individual row partitions. Similarly for the
constraints that require indexes, e.g.:

```sql
CREATE INDEX p2_ts_idx ON p2 (ts);
CREATE UNIQUE INDEX p2_i_unique ON p2 (i);
ALTER TABLE p2 ADD UNIQUE (n);
```

## Converting Between Row and Columnar

Note: ensure that you understand any advanced features that may be
used with the table before converting it (e.g. row-level security,
storage options, constraints, inheritance, etc.), and ensure that they
are reproduced in the new table or partition appropriately. ``LIKE``,
used below, is a shorthand that works only in simple cases.

```sql
CREATE TABLE my_table(i INT8 DEFAULT '7');
INSERT INTO my_table VALUES(1);
-- convert to columnar
SELECT alter_table_set_access_method('my_table', 'columnar');
-- back to row
SELECT alter_table_set_access_method('my_table', 'heap');
```

# Performance Microbenchmark

*Important*: This microbenchmark is not intended to represent any real
 workload. Compression ratios, and therefore performance, will depend
 heavily on the specific workload. This is only for the purpose of
 illustrating a "columnar friendly" contrived workload that showcases
 the benefits of columnar.

## Schema

```sql
CREATE TABLE perf_row(
    id INT8,
    ts TIMESTAMPTZ,
    customer_id INT8,
    vendor_id INT8,
    name TEXT,
    description TEXT,
    value NUMERIC,
    quantity INT4
);

CREATE TABLE perf_columnar(LIKE perf_row) USING COLUMNAR;
```

## Data

```sql
CREATE OR REPLACE FUNCTION random_words(n INT4) RETURNS TEXT LANGUAGE plpython2u AS $$
import random
t = ''
words = ['zero','one','two','three','four','five','six','seven','eight','nine','ten']
for i in xrange(0,n):
  if (i != 0):
    t += ' '
  r = random.randint(0,len(words)-1)
  t += words[r]
return t
$$;
```

```sql
INSERT INTO perf_row
   SELECT
    g, -- id
    '2020-01-01'::timestamptz + ('1 minute'::interval * g), -- ts
    (random() * 1000000)::INT4, -- customer_id
    (random() * 100)::INT4, -- vendor_id
    random_words(7), -- name
    random_words(100), -- description
    (random() * 100000)::INT4/100.0, -- value
    (random() * 100)::INT4 -- quantity
   FROM generate_series(1,75000000) g;

INSERT INTO perf_columnar SELECT * FROM perf_row;
```

## Compression Ratio

```
=> SELECT pg_total_relation_size('perf_row')::numeric/pg_total_relation_size('perf_columnar') AS compression_ratio;
 compression_ratio
--------------------
 5.3958044063457513
(1 row)
```

The overall compression ratio of columnar table, versus the same data
stored with row storage, is **5.4X**.

```
=> VACUUM VERBOSE perf_columnar;
INFO:  statistics for "perf_columnar":
storage id: 10000000000
total file size: 8761368576, total data size: 8734266196
compression rate: 5.01x
total row count: 75000000, stripe count: 500, average rows per stripe: 150000
chunk count: 60000, containing data for dropped columns: 0, zstd compressed: 60000
```

``VACUUM VERBOSE`` reports a smaller compression ratio, because it
only averages the compression ratio of the individual chunks, and does
not account for the metadata savings of the columnar format.

## System

* Azure VM: Standard D2s v3 (2 vcpus, 8 GiB memory)
* Linux (ubuntu 18.04)
* Data Drive: Standard HDD (512GB, 500 IOPS Max, 60 MB/s Max)
* PostgreSQL 13 (``--with-llvm``, ``--with-python``)
* ``shared_buffers = 128MB``
* ``max_parallel_workers_per_gather = 0``
* ``jit = on``

Note: because this was run on a system with enough physical memory to
hold a substantial fraction of the table, the IO benefits of columnar
won't be entirely realized by the query runtime unless the data size
is substantially increased.

## Query

```sql
-- OFFSET 1000 so that no rows are returned, and we collect only timings

SELECT vendor_id, SUM(quantity) FROM perf_row GROUP BY vendor_id OFFSET 1000;
SELECT vendor_id, SUM(quantity) FROM perf_row GROUP BY vendor_id OFFSET 1000;
SELECT vendor_id, SUM(quantity) FROM perf_row GROUP BY vendor_id OFFSET 1000;
SELECT vendor_id, SUM(quantity) FROM perf_columnar GROUP BY vendor_id OFFSET 1000;
SELECT vendor_id, SUM(quantity) FROM perf_columnar GROUP BY vendor_id OFFSET 1000;
SELECT vendor_id, SUM(quantity) FROM perf_columnar GROUP BY vendor_id OFFSET 1000;
```

Timing (median of three runs):
 * row: 436s
 * columnar: 16s
 * speedup: **27X**
