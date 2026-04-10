--
-- Testing ALTER TABLE on columnar tables.
--

CREATE TABLE test_alter_table (a int, b int, c text) USING columnar;

WITH sample_data AS (VALUES
    (1, 2, '3'),
    (4, 5, '6')
)
INSERT INTO test_alter_table SELECT * FROM sample_data;

WITH sample_data AS (VALUES
    (5, 9, '11'),
    (12, 83, '93')
)
INSERT INTO test_alter_table SELECT * FROM sample_data;

ALTER TABLE test_alter_table ALTER COLUMN a TYPE jsonb USING row_to_json(row(a));
SELECT * FROM test_alter_table ORDER BY a;

ALTER TABLE test_alter_table ALTER COLUMN c TYPE int USING c::integer;
SELECT sum(c) FROM test_alter_table;

ALTER TABLE test_alter_table ALTER COLUMN b TYPE bigint;
SELECT * FROM test_alter_table ORDER BY a;

ALTER TABLE test_alter_table ALTER COLUMN b TYPE float USING (b::float + 0.5);
SELECT * FROM test_alter_table ORDER BY a;

DROP TABLE test_alter_table;

-- Make sure that the correct table options are used when rewriting the table.
-- Verify this through metadata instead of VACUUM VERBOSE output because the
-- raw storage ids vary with prior test state.
CREATE TABLE test(i int) USING columnar;
SELECT columnar.alter_columnar_table_set('test', compression => 'lz4');
INSERT INTO test VALUES(1);

SELECT storage_id AS old_storage_id
FROM columnar_test_helpers.columnar_storage_info('test') \gset

SELECT count(*) > 0 AND bool_and(value_compression_type = 2) AS old_storage_uses_lz4
FROM columnar.chunk
WHERE storage_id = :old_storage_id;

ALTER TABLE test ALTER COLUMN i TYPE int8;

SELECT storage_id AS new_storage_id
FROM columnar_test_helpers.columnar_storage_info('test') \gset

SELECT :old_storage_id <> :new_storage_id AS storage_changed;

SELECT count(*) > 0 AND bool_and(value_compression_type = 2) AS new_storage_uses_lz4
FROM columnar.chunk
WHERE storage_id = :new_storage_id;

DROP TABLE test;
