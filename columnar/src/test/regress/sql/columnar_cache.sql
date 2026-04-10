CREATE TABLE big_table (
  id INT,
  firstname TEXT,
  lastname TEXT
) USING columnar;

INSERT INTO big_table (id, firstname, lastname)
  SELECT i,
         CONCAT('firstname-', i),
         CONCAT('lastname-', i)
    FROM generate_series(1, 1000000) as i;

-- capture a baseline that spans multiple chunks and scan nodes
CREATE TEMP TABLE big_table_baseline AS
SELECT firstname,
       lastname,
       SUM(id) AS total_id
  FROM big_table
 WHERE id < 1000
 GROUP BY firstname,
       lastname
UNION
SELECT firstname,
       lastname,
       SUM(id) AS total_id
  FROM big_table
 WHERE id BETWEEN 15000 AND 16000
 GROUP BY firstname,
       lastname;

SELECT COUNT(*)
  FROM big_table_baseline;


-- enable caching
SET columnar.enable_column_cache = 't';

-- the cached results should match the baseline exactly
SELECT COUNT(*)
  FROM (
    SELECT *
      FROM big_table_baseline
    EXCEPT
    SELECT *
      FROM (
        SELECT firstname,
               lastname,
               SUM(id) AS total_id
          FROM big_table
         WHERE id < 1000
         GROUP BY firstname,
               lastname
        UNION
        SELECT firstname,
               lastname,
               SUM(id) AS total_id
          FROM big_table
         WHERE id BETWEEN 15000 AND 16000
         GROUP BY firstname,
               lastname
      ) cached_rows
  ) baseline_minus_cached;

SELECT COUNT(*)
  FROM (
    SELECT *
      FROM (
        SELECT firstname,
               lastname,
               SUM(id) AS total_id
          FROM big_table
         WHERE id < 1000
         GROUP BY firstname,
               lastname
        UNION
        SELECT firstname,
               lastname,
               SUM(id) AS total_id
          FROM big_table
         WHERE id BETWEEN 15000 AND 16000
         GROUP BY firstname,
               lastname
      ) cached_rows
    EXCEPT
    SELECT *
      FROM big_table_baseline
  ) cached_minus_baseline;

DROP TABLE big_table_baseline;

-- disable caching
SET columnar.enable_column_cache = 'f';

-- regular single-row inserts should not leak the disabled cache state
SET columnar.enable_column_cache = 't';
CREATE TABLE cache_flag_probe (i INT) USING columnar;
INSERT INTO cache_flag_probe VALUES (1);
SELECT current_setting('columnar.enable_column_cache');
DROP TABLE cache_flag_probe;
SET columnar.enable_column_cache = 'f';

CREATE TABLE test_2 (
  value INT,
  updated_value INT
) USING columnar;

INSERT INTO test_2 (value)
  SELECT generate_series(1, 1000000, 1);

BEGIN;
SELECT SUM(value)
  FROM test_2;

UPDATE test_2
   SET updated_value = value * 2;

SELECT SUM(updated_value)
  FROM test_2;

DELETE FROM test_2
 WHERE value % 2 = 0;

SELECT SUM(value)
  FROM test_2;
COMMIT;

DROP TABLE test_2;

set columnar.enable_column_cache = 't';

CREATE TABLE test_2 (
  value INT,
  updated_value INT
) USING columnar;

INSERT INTO test_2 (value)
  SELECT generate_series(1, 1000000, 1);

BEGIN;
SELECT SUM(value)
  FROM test_2;

UPDATE test_2
   SET updated_value = value * 2;

SELECT SUM(updated_value)
  FROM test_2;

DELETE FROM test_2
 WHERE value % 2 = 0;

SELECT SUM(value)
  FROM test_2;
COMMIT;

DROP TABLE test_2;

SET columnar.enable_column_cache = 'f';

CREATE TABLE t1 (i int) USING columnar;

INSERT INTO t1 SELECT generate_series(1, 1000000, 1);
EXPLAIN SELECT COUNT(*) FROM t1;
DROP TABLE t1;
