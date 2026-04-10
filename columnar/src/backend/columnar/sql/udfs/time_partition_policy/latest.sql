CREATE TABLE columnar.partition_policy (
    policy_name TEXT PRIMARY KEY CHECK (btrim(policy_name) <> ''),
    parent_table REGCLASS NOT NULL,
    partition_interval INTERVAL NOT NULL,
    columnar_after INTERVAL NOT NULL,
    premake_count INTEGER NOT NULL DEFAULT 1 CHECK (premake_count >= 0),
    enabled BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE columnar.partition_policy
IS 'Metadata for policy-driven management of hot heap partitions and cold columnar partitions';


CREATE TABLE columnar.partition_run_log (
    id BIGSERIAL PRIMARY KEY,
    policy_name TEXT NOT NULL REFERENCES columnar.partition_policy(policy_name) ON DELETE CASCADE,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ NOT NULL,
    reference_time TIMESTAMPTZ NOT NULL,
    partitions_created BIGINT NOT NULL,
    partitions_converted BIGINT NOT NULL
);

CREATE INDEX partition_run_log_policy_name_started_at_idx
    ON columnar.partition_run_log(policy_name, started_at DESC);

COMMENT ON TABLE columnar.partition_run_log
IS 'Audit log for time partition policies that have been executed';


CREATE OR REPLACE FUNCTION columnar._time_partition_policy_info(parent_table REGCLASS)
RETURNS TABLE(parent_schema TEXT, parent_name TEXT, key_type REGTYPE)
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $info$
DECLARE
    partition_strategy "char";
    partition_key_count SMALLINT;
    partition_key_attnum SMALLINT;
BEGIN
    SELECT parent_ns.nspname,
           parent.relname
      INTO parent_schema,
           parent_name
      FROM pg_class parent
      JOIN pg_namespace parent_ns ON parent_ns.oid = parent.relnamespace
     WHERE parent.oid = parent_table;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'parent table % does not exist', parent_table::text;
    END IF;

    SELECT part.partstrat,
           part.partnatts,
           part_att.attnum,
           att.atttypid::regtype
      INTO partition_strategy,
           partition_key_count,
           partition_key_attnum,
           key_type
      FROM pg_partitioned_table part
      JOIN LATERAL unnest(part.partattrs::smallint[]) WITH ORDINALITY AS part_att(attnum, ord)
        ON part_att.ord = 1
 LEFT JOIN pg_attribute att
        ON att.attrelid = part.partrelid
       AND att.attnum = part_att.attnum
     WHERE part.partrelid = parent_table;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'parent table % must be partitioned', parent_table::text;
    END IF;

    IF partition_strategy IS DISTINCT FROM 'r' THEN
        RAISE EXCEPTION 'parent table % must use RANGE partitioning', parent_table::text;
    END IF;

    IF partition_key_count IS DISTINCT FROM 1
       OR partition_key_attnum IS NULL
       OR partition_key_attnum = 0
       OR key_type IS NULL THEN
        RAISE EXCEPTION
            'parent table % must use a single direct partition key column',
            parent_table::text;
    END IF;

    IF key_type NOT IN ('date'::regtype,
                        'timestamp without time zone'::regtype,
                        'timestamp with time zone'::regtype) THEN
        RAISE EXCEPTION
            'parent table % must partition on date, timestamp, or timestamptz',
            parent_table::text;
    END IF;

    RETURN NEXT;
END;
$info$;


CREATE OR REPLACE FUNCTION columnar._time_partition_bound_values(
    partition_table REGCLASS,
    key_type REGTYPE)
RETURNS TABLE(lower_value TEXT, upper_value TEXT)
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $bounds$
DECLARE
    bound_definition TEXT;
    bound_matches TEXT[];
BEGIN
    SELECT pg_get_expr(partition.relpartbound, partition.oid)
      INTO bound_definition
      FROM pg_class partition
     WHERE partition.oid = partition_table;

    IF bound_definition IS NULL THEN
        RAISE EXCEPTION 'table % is not a partition', partition_table::text;
    END IF;

    bound_matches := regexp_match(
        bound_definition,
        E'^FOR VALUES FROM \\((.+)\\) TO \\((.+)\\)$');

    IF bound_matches IS NULL OR cardinality(bound_matches) <> 2 THEN
        RAISE EXCEPTION
            'table % must use a simple finite range partition bound',
            partition_table::text;
    END IF;

    IF btrim(bound_matches[1]) IN ('MINVALUE', 'MAXVALUE')
       OR btrim(bound_matches[2]) IN ('MINVALUE', 'MAXVALUE') THEN
        RAISE EXCEPTION
            'table % must use finite range bounds',
            partition_table::text;
    END IF;

    EXECUTE format(
        'SELECT ((%s)::%s)::text, ((%s)::%s)::text',
        bound_matches[1],
        key_type::text,
        bound_matches[2],
        key_type::text)
       INTO lower_value,
            upper_value;

    RETURN NEXT;
END;
$bounds$;


CREATE OR REPLACE FUNCTION columnar._time_partition_value_cmp(
    left_value TEXT,
    right_value TEXT,
    key_type REGTYPE,
    comparison_operator TEXT)
RETURNS BOOLEAN
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $cmp$
DECLARE
    comparison_result BOOLEAN;
BEGIN
    IF comparison_operator NOT IN ('<', '<=', '>', '>=', '=') THEN
        RAISE EXCEPTION 'unsupported comparison operator %', comparison_operator;
    END IF;

    EXECUTE format(
        'SELECT (%L::%s %s %L::%s)',
        left_value,
        key_type::text,
        comparison_operator,
        right_value,
        key_type::text)
       INTO comparison_result;

    RETURN comparison_result;
END;
$cmp$;


CREATE OR REPLACE FUNCTION columnar._time_partition_add_interval(
    base_value TEXT,
    delta INTERVAL,
    key_type REGTYPE)
RETURNS TEXT
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $add_interval$
DECLARE
    result_value TEXT;
BEGIN
    EXECUTE format(
        'SELECT (((%L)::%s + $1)::%s)::text',
        base_value,
        key_type::text,
        key_type::text)
       INTO result_value
       USING delta;

    RETURN result_value;
END;
$add_interval$;


CREATE OR REPLACE FUNCTION columnar._time_partition_subtract_interval(
    base_value TEXT,
    delta INTERVAL,
    key_type REGTYPE)
RETURNS TEXT
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $subtract_interval$
DECLARE
    result_value TEXT;
BEGIN
    EXECUTE format(
        'SELECT (((%L)::%s - $1)::%s)::text',
        base_value,
        key_type::text,
        key_type::text)
       INTO result_value
       USING delta;

    RETURN result_value;
END;
$subtract_interval$;


CREATE OR REPLACE FUNCTION columnar._time_partition_reference_value(
    reference_time TIMESTAMPTZ,
    key_type REGTYPE)
RETURNS TEXT
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $reference$
DECLARE
    result_value TEXT;
BEGIN
    EXECUTE format(
        'SELECT (($1)::%s)::text',
        key_type::text)
       INTO result_value
       USING reference_time;

    RETURN result_value;
END;
$reference$;


CREATE OR REPLACE FUNCTION columnar._time_partition_generated_name(
    parent_table REGCLASS,
    lower_value TEXT,
    upper_value TEXT)
RETURNS TEXT
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $generated_name$
DECLARE
    parent_name TEXT;
BEGIN
    SELECT relname
      INTO parent_name
      FROM pg_class
     WHERE oid = parent_table;

    RETURN substr(parent_name, 1, 44)
        || '_p'
        || substr(md5(parent_table::text || ':' || lower_value || ':' || upper_value), 1, 16);
END;
$generated_name$;


CREATE OR REPLACE FUNCTION columnar._alter_partition_set_access_method(
    partition_table REGCLASS,
    method TEXT)
RETURNS BOOLEAN
LANGUAGE plpgsql
AS $alter_partition$
DECLARE
    partition_schema TEXT;
    partition_name TEXT;
    parent_schema TEXT;
    parent_name TEXT;
    partition_bound TEXT;
BEGIN
    SELECT partition_ns.nspname,
           partition.relname,
           parent_ns.nspname,
           parent.relname,
           pg_get_expr(partition.relpartbound, partition.oid)
      INTO partition_schema,
           partition_name,
           parent_schema,
           parent_name,
           partition_bound
      FROM pg_class partition
      JOIN pg_namespace partition_ns ON partition_ns.oid = partition.relnamespace
      JOIN pg_inherits inheritance ON inheritance.inhrelid = partition.oid
      JOIN pg_class parent ON parent.oid = inheritance.inhparent
      JOIN pg_namespace parent_ns ON parent_ns.oid = parent.relnamespace
     WHERE partition.oid = partition_table;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'table % is not an attached partition', partition_table::text;
    END IF;

    EXECUTE format(
        'ALTER TABLE %I.%I DETACH PARTITION %I.%I',
        parent_schema,
        parent_name,
        partition_schema,
        partition_name);

    PERFORM columnar.alter_table_set_access_method(
        format('%I.%I', partition_schema, partition_name),
        method);

    EXECUTE format(
        'ALTER TABLE %I.%I ATTACH PARTITION %I.%I %s',
        parent_schema,
        parent_name,
        partition_schema,
        partition_name,
        partition_bound);

    RETURN true;
END;
$alter_partition$;


CREATE OR REPLACE FUNCTION columnar.create_partition_policy(
    target_policy_name TEXT,
    parent_table REGCLASS,
    partition_interval INTERVAL,
    columnar_after INTERVAL,
    premake_count INTEGER DEFAULT 1,
    enabled BOOLEAN DEFAULT true)
RETURNS BOOLEAN
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $create_policy$
DECLARE
    key_type REGTYPE;
    partition_record RECORD;
    child_partition_count BIGINT;
BEGIN
    IF btrim(target_policy_name) = '' THEN
        RAISE EXCEPTION 'target_policy_name must not be empty';
    END IF;

    IF partition_interval <= interval '0' THEN
        RAISE EXCEPTION 'partition_interval must be greater than zero';
    END IF;

    IF columnar_after < interval '0' THEN
        RAISE EXCEPTION 'columnar_after must not be negative';
    END IF;

    IF premake_count < 0 THEN
        RAISE EXCEPTION 'premake_count must not be negative';
    END IF;

    SELECT info.key_type
      INTO key_type
      FROM columnar._time_partition_policy_info(parent_table) AS info;

    SELECT count(*)
      INTO child_partition_count
      FROM pg_inherits
     WHERE inhparent = parent_table;

    IF child_partition_count = 0 THEN
        RAISE EXCEPTION
            'parent table % must have at least one partition to seed automation',
            parent_table::text;
    END IF;

    FOR partition_record IN
        SELECT child.oid::regclass AS partition_table
          FROM pg_inherits inheritance
          JOIN pg_class child ON child.oid = inheritance.inhrelid
         WHERE inheritance.inhparent = parent_table
         ORDER BY child.oid
    LOOP
        PERFORM *
          FROM columnar._time_partition_bound_values(
              partition_record.partition_table,
              key_type);
    END LOOP;

    INSERT INTO columnar.partition_policy(
        policy_name,
        parent_table,
        partition_interval,
        columnar_after,
        premake_count,
        enabled)
    VALUES(
        target_policy_name,
        parent_table,
        partition_interval,
        columnar_after,
        premake_count,
        enabled)
    ON CONFLICT (policy_name) DO UPDATE SET
        parent_table = EXCLUDED.parent_table,
        partition_interval = EXCLUDED.partition_interval,
        columnar_after = EXCLUDED.columnar_after,
        premake_count = EXCLUDED.premake_count,
        enabled = EXCLUDED.enabled;

    RETURN true;
END;
$create_policy$;

COMMENT ON FUNCTION columnar.create_partition_policy(
    target_policy_name TEXT,
    parent_table REGCLASS,
    partition_interval INTERVAL,
    columnar_after INTERVAL,
    premake_count INTEGER,
    enabled BOOLEAN)
IS 'Creates or updates a time partition policy for hot heap partitions and cold columnar partitions';


CREATE OR REPLACE FUNCTION columnar.drop_partition_policy(target_policy_name TEXT)
RETURNS BOOLEAN
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $drop_policy$
BEGIN
    DELETE FROM columnar.partition_policy
     WHERE policy_name = target_policy_name;

    IF NOT FOUND THEN
        RETURN false;
    END IF;

    RETURN true;
END;
$drop_policy$;

COMMENT ON FUNCTION columnar.drop_partition_policy(target_policy_name TEXT)
IS 'Deletes a time partition policy and its run log rows';


CREATE OR REPLACE FUNCTION columnar.run_partition_policy(
    target_policy_name TEXT,
    reference_time TIMESTAMPTZ DEFAULT now())
RETURNS TABLE(partitions_created BIGINT, partitions_converted BIGINT)
LANGUAGE plpgsql
AS $run_policy$
DECLARE
    policy_record RECORD;
    parent_schema TEXT;
    parent_name TEXT;
    key_type REGTYPE;
    partition_record RECORD;
    premake_step INTEGER;
    lower_value TEXT;
    upper_value TEXT;
    latest_upper_value TEXT;
    anchor_upper_value TEXT;
    reference_value TEXT;
    cutoff_value TEXT;
    target_upper_value TEXT;
    new_partition_name TEXT;
    new_upper_value TEXT;
    started_at TIMESTAMPTZ;
    finished_at TIMESTAMPTZ;
    partitions_to_convert TEXT[] := ARRAY[]::TEXT[];
    partition_to_convert TEXT;
    contains_reference BOOLEAN;
BEGIN
    partitions_created := 0;
    partitions_converted := 0;

    SELECT policy_name,
           parent_table,
           partition_interval,
           columnar_after,
           premake_count
      INTO policy_record
      FROM columnar.partition_policy
     WHERE policy_name = target_policy_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'partition policy % does not exist', target_policy_name;
    END IF;

    SELECT info.parent_schema,
           info.parent_name,
           info.key_type
      INTO parent_schema,
           parent_name,
           key_type
      FROM columnar._time_partition_policy_info(policy_record.parent_table) AS info;

    reference_value := columnar._time_partition_reference_value(reference_time, key_type);
    cutoff_value := columnar._time_partition_subtract_interval(
        reference_value,
        policy_record.columnar_after,
        key_type);

    started_at := clock_timestamp();

    FOR partition_record IN
        SELECT child.oid::regclass AS partition_table,
               child_ns.nspname AS partition_schema,
               child.relname AS partition_name,
               COALESCE(child_am.amname, 'heap') AS access_method
          FROM pg_inherits inheritance
          JOIN pg_class child ON child.oid = inheritance.inhrelid
          JOIN pg_namespace child_ns ON child_ns.oid = child.relnamespace
     LEFT JOIN pg_am child_am ON child_am.oid = child.relam
         WHERE inheritance.inhparent = policy_record.parent_table
         ORDER BY child.oid
    LOOP
        SELECT bounds.lower_value,
               bounds.upper_value
          INTO lower_value,
               upper_value
          FROM columnar._time_partition_bound_values(
                   partition_record.partition_table,
                   key_type) AS bounds;

        IF latest_upper_value IS NULL
           OR columnar._time_partition_value_cmp(
                  upper_value,
                  latest_upper_value,
                  key_type,
                  '>') THEN
            latest_upper_value := upper_value;
        END IF;

        contains_reference :=
            columnar._time_partition_value_cmp(
                lower_value,
                reference_value,
                key_type,
                '<=')
            AND
            columnar._time_partition_value_cmp(
                reference_value,
                upper_value,
                key_type,
                '<');

        IF anchor_upper_value IS NULL AND contains_reference THEN
            anchor_upper_value := upper_value;
        END IF;

        IF partition_record.access_method IS DISTINCT FROM 'columnar'
           AND columnar._time_partition_value_cmp(
                   upper_value,
                   cutoff_value,
                   key_type,
                   '<=') THEN
            partitions_to_convert := array_append(
                partitions_to_convert,
                format('%I.%I',
                       partition_record.partition_schema,
                       partition_record.partition_name));
        END IF;
    END LOOP;

    IF latest_upper_value IS NULL THEN
        RAISE EXCEPTION
            'partition policy % requires at least one existing partition',
            target_policy_name;
    END IF;

    IF anchor_upper_value IS NULL THEN
        anchor_upper_value := latest_upper_value;
    END IF;

    target_upper_value := anchor_upper_value;

    FOR premake_step IN 1..policy_record.premake_count LOOP
        target_upper_value := columnar._time_partition_add_interval(
            target_upper_value,
            policy_record.partition_interval,
            key_type);
    END LOOP;

    WHILE columnar._time_partition_value_cmp(
              latest_upper_value,
              target_upper_value,
              key_type,
              '<')
    LOOP
        new_upper_value := columnar._time_partition_add_interval(
            latest_upper_value,
            policy_record.partition_interval,
            key_type);
        new_partition_name := columnar._time_partition_generated_name(
            policy_record.parent_table,
            latest_upper_value,
            new_upper_value);

        EXECUTE format(
            'CREATE TABLE %I.%I PARTITION OF %I.%I ' ||
            'FOR VALUES FROM (%L) TO (%L) USING heap',
            parent_schema,
            new_partition_name,
            parent_schema,
            parent_name,
            latest_upper_value,
            new_upper_value);

        partitions_created := partitions_created + 1;
        latest_upper_value := new_upper_value;
    END LOOP;

    IF array_length(partitions_to_convert, 1) IS NOT NULL THEN
        FOREACH partition_to_convert IN ARRAY partitions_to_convert
        LOOP
            PERFORM columnar._alter_partition_set_access_method(
                partition_to_convert::regclass,
                'columnar');
            partitions_converted := partitions_converted + 1;
        END LOOP;
    END IF;

    finished_at := clock_timestamp();

    INSERT INTO columnar.partition_run_log(
        policy_name,
        started_at,
        finished_at,
        reference_time,
        partitions_created,
        partitions_converted)
    VALUES(
        policy_record.policy_name,
        started_at,
        finished_at,
        reference_time,
        partitions_created,
        partitions_converted);

    RETURN NEXT;
END;
$run_policy$;

COMMENT ON FUNCTION columnar.run_partition_policy(
    target_policy_name TEXT,
    reference_time TIMESTAMPTZ)
IS 'Runs a named time partition policy regardless of its enabled flag and returns created and converted partition counts';


CREATE OR REPLACE FUNCTION columnar.run_partition_policies(
    reference_time TIMESTAMPTZ DEFAULT now())
RETURNS TABLE(
    policy_name TEXT,
    partitions_created BIGINT,
    partitions_converted BIGINT)
LANGUAGE plpgsql
AS $run_all$
DECLARE
    policy_record RECORD;
BEGIN
    FOR policy_record IN
        SELECT partition_policy.policy_name
          FROM columnar.partition_policy AS partition_policy
         WHERE partition_policy.enabled
         ORDER BY partition_policy.policy_name
    LOOP
        SELECT result.partitions_created,
               result.partitions_converted
          INTO partitions_created,
               partitions_converted
          FROM columnar.run_partition_policy(
                   policy_record.policy_name,
                   reference_time) AS result;

        policy_name := policy_record.policy_name;
        RETURN NEXT;
    END LOOP;

    RETURN;
END;
$run_all$;

COMMENT ON FUNCTION columnar.run_partition_policies(reference_time TIMESTAMPTZ)
IS 'Runs all enabled time partition policies and returns per-policy partition counts';
