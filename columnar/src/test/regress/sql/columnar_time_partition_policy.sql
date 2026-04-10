CREATE SCHEMA time_policy;
SET search_path TO time_policy, public;
SET timezone TO 'UTC';
SET datestyle TO ISO, MDY;
SET intervalstyle TO postgres;

CREATE TABLE events (
  ts TIMESTAMPTZ NOT NULL,
  run_id INT NOT NULL,
  payload TEXT
) PARTITION BY RANGE (ts);

CREATE TABLE events_20200401 PARTITION OF events
  FOR VALUES FROM ('2020-04-01 00:00:00+00') TO ('2020-04-02 00:00:00+00')
  USING heap;

CREATE TABLE events_20200402 PARTITION OF events
  FOR VALUES FROM ('2020-04-02 00:00:00+00') TO ('2020-04-03 00:00:00+00')
  USING heap;

INSERT INTO events VALUES
  ('2020-04-01 01:00:00+00', 1, 'cold soon'),
  ('2020-04-02 01:00:00+00', 2, 'still hot');

SELECT columnar.create_partition_policy(
  'events_policy',
  'events',
  '1 day',
  '12 hours',
  2);

SELECT policy_name,
       parent_table::text,
       partition_interval,
       columnar_after,
       premake_count,
       enabled
FROM columnar.partition_policy
ORDER BY policy_name;

SELECT * FROM columnar.run_partition_policy(
  'events_policy',
  '2020-04-02 12:00:00+00');

SELECT COALESCE(child_am.amname, 'heap') AS access_method,
       pg_get_expr(child.relpartbound, child.oid) AS bound_definition
FROM pg_inherits inheritance
JOIN pg_class child ON child.oid = inheritance.inhrelid
LEFT JOIN pg_am child_am ON child_am.oid = child.relam
WHERE inheritance.inhparent = 'events'::regclass
ORDER BY bound_definition;

SELECT policy_name,
       partitions_created,
       partitions_converted,
       reference_time
FROM columnar.partition_run_log
ORDER BY id;

SELECT columnar.create_partition_policy(
  'disabled_events_policy',
  'events',
  '1 day',
  '12 hours',
  1,
  false);

SELECT * FROM columnar.run_partition_policies('2020-04-02 12:00:00+00');

SELECT policy_name,
       partitions_created,
       partitions_converted
FROM columnar.partition_run_log
ORDER BY id;

CREATE TABLE metric_parent(i int, v text) PARTITION BY RANGE (i);
CREATE TABLE metric_parent_p0 PARTITION OF metric_parent FOR VALUES FROM (0) TO (10);

SELECT columnar.create_partition_policy(
  'bad_metric_policy',
  'metric_parent',
  '1 day',
  '1 day');

SELECT columnar.drop_partition_policy('disabled_events_policy');
SELECT columnar.drop_partition_policy('events_policy');

DROP SCHEMA time_policy CASCADE;
