CREATE SCHEMA hybrid_policy;
SET search_path TO hybrid_policy, public;

CREATE TABLE run_state_hot (
  run_id INT NOT NULL,
  step_no INT NOT NULL,
  status TEXT NOT NULL,
  finished_at TIMESTAMPTZ
) USING heap;

CREATE TABLE run_state_cold
  (LIKE run_state_hot INCLUDING DEFAULTS)
  USING columnar;

CREATE OR REPLACE FUNCTION hybrid_policy.finished_run_selector(
  policy_name TEXT,
  hot_table REGCLASS,
  cold_table REGCLASS)
RETURNS TEXT
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
  ready_run_ids TEXT;
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

CREATE OR REPLACE FUNCTION hybrid_policy.bad_selector()
RETURNS TEXT
LANGUAGE sql
AS $$ SELECT 'run_id = 1' $$;

INSERT INTO run_state_hot VALUES
  (1, 1, 'done', '2026-04-10 00:00:00+00'),
  (1, 2, 'done', '2026-04-10 00:00:00+00'),
  (2, 1, 'running', NULL),
  (3, 1, 'done', '2026-04-10 00:00:00+00');

SELECT columnar.create_archive_policy(
  'run_state_policy',
  'run_state_hot',
  'run_state_cold',
  'hybrid_policy.finished_run_selector(text,regclass,regclass)',
  'hybrid_policy.run_state_read');

SELECT policy_name, read_view, delete_from_hot, enabled
FROM columnar.archive_policy
ORDER BY policy_name;

SELECT COUNT(*) FROM run_state_read;

SELECT columnar.run_archive_policy('run_state_policy');

SELECT COUNT(*) FROM run_state_hot;
SELECT COUNT(*) FROM run_state_cold;
SELECT COUNT(*) FROM run_state_read;

SELECT policy_name, rows_moved, where_clause
FROM columnar.archive_run_log
ORDER BY id;

SELECT columnar.create_archive_policy(
  'disabled_policy',
  'run_state_hot',
  'run_state_cold',
  'hybrid_policy.finished_run_selector(text,regclass,regclass)',
  NULL,
  true,
  false);

SELECT * FROM columnar.run_archive_policies();

SELECT policy_name, rows_moved, where_clause IS NULL AS empty_where_clause
FROM columnar.archive_run_log
ORDER BY id;

SELECT columnar.create_archive_policy(
  'bad_policy',
  'run_state_hot',
  'run_state_cold',
  'hybrid_policy.bad_selector()');

SELECT columnar.drop_archive_policy('disabled_policy');
SELECT columnar.drop_archive_policy('run_state_policy', true);

DROP SCHEMA hybrid_policy CASCADE;
