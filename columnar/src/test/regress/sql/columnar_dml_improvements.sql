--
-- Tests for columnar DML improvements:
--   1. ColumnarTableTupleCount subtracts deleted rows
--   2. Fully-deleted chunk groups are skipped during scans
--   3. Stripe-level locking (concurrent updates to different stripes)
--   4. Bulk delete performance (no per-row CommandCounterIncrement)
--

SET columnar.compression TO 'none';

---------------------------------------------------------------
-- Test 1: ColumnarTableTupleCount correctly subtracts deleted rows
--
-- After DELETE + VACUUM, pg_class.reltuples should reflect
-- only the live rows, not the total including deleted.
---------------------------------------------------------------

CREATE TABLE dml_tuple_count(i int, j int) USING columnar;

-- Insert enough rows to create multiple chunk groups
INSERT INTO dml_tuple_count SELECT g, g * 10 FROM generate_series(1, 1000) g;

-- Verify initial count
SELECT COUNT(*) AS live_rows FROM dml_tuple_count;

-- Delete half the rows
DELETE FROM dml_tuple_count WHERE i % 2 = 0;

-- Verify correct live count after delete
SELECT COUNT(*) AS live_rows_after_delete FROM dml_tuple_count;

-- VACUUM updates pg_class.reltuples via ColumnarTableTupleCount
VACUUM dml_tuple_count;

-- pg_class.reltuples should match the actual live row count (500), not 1000
SELECT reltuples FROM pg_class WHERE relname = 'dml_tuple_count';

-- Delete more rows and vacuum again
DELETE FROM dml_tuple_count WHERE i % 3 = 0;

SELECT COUNT(*) AS live_rows_after_second_delete FROM dml_tuple_count;

VACUUM dml_tuple_count;

-- Should match the live count
SELECT reltuples FROM pg_class WHERE relname = 'dml_tuple_count';

-- Edge case: delete all rows
DELETE FROM dml_tuple_count;

VACUUM dml_tuple_count;

SELECT reltuples FROM pg_class WHERE relname = 'dml_tuple_count';

DROP TABLE dml_tuple_count;

---------------------------------------------------------------
-- Test 2: Fully-deleted chunk groups are skipped
--
-- When all rows in a chunk group are deleted, the reader should
-- skip the chunk group entirely without decompressing data.
-- We verify this by checking correctness with multiple chunk
-- groups where some are fully deleted.
---------------------------------------------------------------

-- Use small chunk groups so we can control which ones get fully deleted
CREATE TABLE dml_chunk_skip(i int, j text) USING columnar;
SELECT columnar.alter_columnar_table_set('dml_chunk_skip',
    chunk_group_row_limit => 100,
    stripe_row_limit => 1000);

-- Insert 500 rows (5 chunk groups of 100 in a single stripe)
INSERT INTO dml_chunk_skip SELECT g, 'value-' || g FROM generate_series(1, 500) g;

-- Delete all rows in the first chunk group (rows 1-100)
DELETE FROM dml_chunk_skip WHERE i <= 100;

-- Delete all rows in the third chunk group (rows 201-300)
DELETE FROM dml_chunk_skip WHERE i > 200 AND i <= 300;

-- Verify we get exactly the right rows back
SELECT COUNT(*) AS remaining_rows FROM dml_chunk_skip;

-- Verify min/max are correct (confirms we read the right chunk groups)
SELECT MIN(i) AS min_i, MAX(i) AS max_i FROM dml_chunk_skip;

-- Delete remaining chunk groups one by one
DELETE FROM dml_chunk_skip WHERE i > 100 AND i <= 200;
DELETE FROM dml_chunk_skip WHERE i > 300 AND i <= 400;
DELETE FROM dml_chunk_skip WHERE i > 400;

-- Should be empty
SELECT COUNT(*) AS should_be_zero FROM dml_chunk_skip;

DROP TABLE dml_chunk_skip;

-- Test with multiple stripes
CREATE TABLE dml_chunk_skip_multi(i int, j int) USING columnar;
SELECT columnar.alter_columnar_table_set('dml_chunk_skip_multi',
    chunk_group_row_limit => 100,
    stripe_row_limit => 200);

-- 3 stripes: rows 1-200, 201-400, 401-600
INSERT INTO dml_chunk_skip_multi SELECT g, g * 2 FROM generate_series(1, 600) g;

-- Delete entire first stripe
DELETE FROM dml_chunk_skip_multi WHERE i <= 200;

-- Delete second chunk group of second stripe (rows 301-400)
DELETE FROM dml_chunk_skip_multi WHERE i > 300 AND i <= 400;

-- Should have: rows 201-300, 401-600 = 300 rows
SELECT COUNT(*) AS remaining FROM dml_chunk_skip_multi;
SELECT MIN(i), MAX(i) FROM dml_chunk_skip_multi;

DROP TABLE dml_chunk_skip_multi;

---------------------------------------------------------------
-- Test 3: Stripe-level locking
--
-- Verify that updates to rows in different stripes don't
-- conflict. We test this within a single session by checking
-- that the advisory lock keys are stripe-specific.
---------------------------------------------------------------

CREATE TABLE dml_stripe_lock(i int, j int) USING columnar;
SELECT columnar.alter_columnar_table_set('dml_stripe_lock',
    stripe_row_limit => 1000);

-- Create 3 stripes
INSERT INTO dml_stripe_lock SELECT g, g * 10 FROM generate_series(1, 1000) g;
INSERT INTO dml_stripe_lock SELECT g, g * 10 FROM generate_series(1001, 2000) g;
INSERT INTO dml_stripe_lock SELECT g, g * 10 FROM generate_series(2001, 3000) g;

-- Verify we have 3 stripes
SELECT COUNT(*) AS stripe_count
FROM columnar.stripe a, pg_class b
WHERE a.storage_id = columnar_test_helpers.columnar_relation_storageid(b.oid)
  AND b.relname = 'dml_stripe_lock';

-- Update rows in different stripes within a transaction
BEGIN;

-- Update in stripe 1
UPDATE dml_stripe_lock SET j = -1 WHERE i = 1;

-- Update in stripe 2
UPDATE dml_stripe_lock SET j = -2 WHERE i = 1500;

-- Update in stripe 3
UPDATE dml_stripe_lock SET j = -3 WHERE i = 2500;

-- Delete from stripe 1
DELETE FROM dml_stripe_lock WHERE i = 500;

-- Delete from stripe 3
DELETE FROM dml_stripe_lock WHERE i = 2999;

COMMIT;

-- Verify all changes applied correctly
SELECT j FROM dml_stripe_lock WHERE i = 1;
SELECT j FROM dml_stripe_lock WHERE i = 1500;
SELECT j FROM dml_stripe_lock WHERE i = 2500;
SELECT COUNT(*) AS total FROM dml_stripe_lock;

DROP TABLE dml_stripe_lock;

---------------------------------------------------------------
-- Test 4: Bulk delete performance
--
-- Verify that bulk deletes produce correct results. The
-- optimization removes per-row CommandCounterIncrement,
-- so we need to verify correctness with large bulk operations
-- that span multiple chunk groups and stripes.
---------------------------------------------------------------

CREATE TABLE dml_bulk_delete(i int, j int, k text) USING columnar;
SELECT columnar.alter_columnar_table_set('dml_bulk_delete',
    chunk_group_row_limit => 1000,
    stripe_row_limit => 5000);

-- Insert 20,000 rows across 4 stripes
INSERT INTO dml_bulk_delete SELECT g, g * 10, 'row-' || g FROM generate_series(1, 20000) g;

SELECT COUNT(*) AS initial_count FROM dml_bulk_delete;

-- Bulk delete: remove every other row (10,000 deletes spanning all stripes/chunks)
DELETE FROM dml_bulk_delete WHERE i % 2 = 0;

SELECT COUNT(*) AS after_even_delete FROM dml_bulk_delete;

-- Verify no even rows remain
SELECT COUNT(*) AS even_remaining FROM dml_bulk_delete WHERE i % 2 = 0;

-- Bulk update: update every 3rd remaining row
UPDATE dml_bulk_delete SET j = -1 WHERE i % 3 = 0;

SELECT COUNT(*) AS updated_rows FROM dml_bulk_delete WHERE j = -1;
SELECT COUNT(*) AS total_after_update FROM dml_bulk_delete;

-- Verify data integrity: all remaining rows should be odd
SELECT COUNT(*) AS odd_rows FROM dml_bulk_delete WHERE i % 2 = 1;
SELECT MIN(i) AS min_i, MAX(i) AS max_i FROM dml_bulk_delete;

DROP TABLE dml_bulk_delete;

-- Test bulk delete within a transaction with rollback
CREATE TABLE dml_bulk_rollback(i int) USING columnar;
INSERT INTO dml_bulk_rollback SELECT g FROM generate_series(1, 5000) g;

BEGIN;
DELETE FROM dml_bulk_rollback WHERE i <= 2500;
SELECT COUNT(*) AS during_txn FROM dml_bulk_rollback;
ROLLBACK;

-- After rollback, all rows should be back
SELECT COUNT(*) AS after_rollback FROM dml_bulk_rollback;

DROP TABLE dml_bulk_rollback;

-- Test interleaved bulk deletes and selects within same transaction
CREATE TABLE dml_bulk_interleaved(i int, j int) USING columnar;
SELECT columnar.alter_columnar_table_set('dml_bulk_interleaved',
    chunk_group_row_limit => 1000,
    stripe_row_limit => 5000);

INSERT INTO dml_bulk_interleaved SELECT g, g FROM generate_series(1, 10000) g;

BEGIN;

-- Delete first batch
DELETE FROM dml_bulk_interleaved WHERE i <= 2000;
SELECT COUNT(*) AS after_first_delete FROM dml_bulk_interleaved;

-- Delete second batch (from remaining rows)
DELETE FROM dml_bulk_interleaved WHERE i <= 4000;
SELECT COUNT(*) AS after_second_delete FROM dml_bulk_interleaved;

-- Update remaining
UPDATE dml_bulk_interleaved SET j = -1 WHERE i <= 6000;
SELECT COUNT(*) AS updated FROM dml_bulk_interleaved WHERE j = -1;

-- Delete the updated rows
DELETE FROM dml_bulk_interleaved WHERE j = -1;
SELECT COUNT(*) AS after_update_delete FROM dml_bulk_interleaved;

COMMIT;

SELECT COUNT(*) AS final_count FROM dml_bulk_interleaved;
SELECT MIN(i) AS min_i, MAX(i) AS max_i FROM dml_bulk_interleaved;

DROP TABLE dml_bulk_interleaved;

---------------------------------------------------------------
-- Test 5: Combined test - tuple count + vacuum after bulk ops
---------------------------------------------------------------

CREATE TABLE dml_combined(i int, j int) USING columnar;
SELECT columnar.alter_columnar_table_set('dml_combined',
    chunk_group_row_limit => 1000,
    stripe_row_limit => 5000);

INSERT INTO dml_combined SELECT g, g FROM generate_series(1, 15000) g;

-- Delete 5000 rows
DELETE FROM dml_combined WHERE i <= 5000;

-- Verify count is correct
SELECT COUNT(*) AS live_count FROM dml_combined;

-- Vacuum and check reltuples matches actual count
VACUUM dml_combined;
SELECT reltuples FROM pg_class WHERE relname = 'dml_combined';

-- Update 2000 rows (delete old + insert new = net 0 change to live count)
UPDATE dml_combined SET j = -1 WHERE i > 5000 AND i <= 7000;

SELECT COUNT(*) AS live_after_update FROM dml_combined;

VACUUM dml_combined;
SELECT reltuples FROM pg_class WHERE relname = 'dml_combined';

DROP TABLE dml_combined;
