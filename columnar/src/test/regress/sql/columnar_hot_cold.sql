CREATE SCHEMA hybrid_storage;
SET search_path TO hybrid_storage, public;

CREATE TABLE run_state_hot (
  run_id INT NOT NULL,
  step_no INT NOT NULL,
  status TEXT NOT NULL,
  finished_at TIMESTAMPTZ
) USING heap;

CREATE TABLE run_state_cold
  (LIKE run_state_hot INCLUDING DEFAULTS)
  USING columnar;

CREATE TABLE run_state_mirror
  (LIKE run_state_hot INCLUDING DEFAULTS)
  USING columnar;

CREATE TABLE run_state_not_columnar
  (LIKE run_state_hot INCLUDING DEFAULTS)
  USING heap;

CREATE TABLE run_state_bad_cold (
  run_id INT NOT NULL,
  status TEXT NOT NULL
) USING columnar;

INSERT INTO run_state_hot VALUES
  (1, 1, 'done', '2026-04-10 00:00:00+00'),
  (1, 2, 'done', '2026-04-10 00:00:00+00'),
  (2, 1, 'running', NULL),
  (2, 2, 'running', NULL);

SELECT columnar.create_hot_cold_view(
  'hybrid_storage.run_state_read',
  'run_state_hot',
  'run_state_cold');

SELECT COUNT(*) FROM run_state_read;

SELECT columnar.archive_to_cold(
  'run_state_hot',
  'run_state_cold',
  'run_id = 1');

SELECT COUNT(*) FROM run_state_hot;
SELECT COUNT(*) FROM run_state_cold;
SELECT COUNT(*) FROM run_state_read;

SELECT run_id, step_no, status
FROM run_state_read
ORDER BY run_id, step_no;

UPDATE run_state_hot
SET status = 'waiting'
WHERE run_id = 2 AND step_no = 1;

SELECT run_id, step_no, status
FROM run_state_read
WHERE run_id = 2
ORDER BY run_id, step_no;

SELECT columnar.archive_to_cold(
  'run_state_hot',
  'run_state_mirror',
  'run_id = 2',
  false);

SELECT COUNT(*) FROM run_state_hot;
SELECT COUNT(*) FROM run_state_mirror;

SELECT columnar.archive_to_cold(
  'run_state_hot',
  'run_state_cold',
  '   ');

SELECT columnar.archive_to_cold(
  'run_state_hot',
  'run_state_not_columnar',
  'run_id = 2');

SELECT columnar.create_hot_cold_view(
  'hybrid_storage.run_state_bad_read',
  'run_state_hot',
  'run_state_bad_cold');

DROP SCHEMA hybrid_storage CASCADE;
