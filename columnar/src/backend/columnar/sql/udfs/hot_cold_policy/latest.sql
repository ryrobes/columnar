CREATE TABLE columnar.archive_policy (
    policy_name TEXT PRIMARY KEY CHECK (btrim(policy_name) <> ''),
    hot_table REGCLASS NOT NULL,
    cold_table REGCLASS NOT NULL,
    read_view TEXT CHECK (read_view IS NULL OR btrim(read_view) <> ''),
    selector_function REGPROCEDURE NOT NULL,
    delete_from_hot BOOLEAN NOT NULL DEFAULT true,
    enabled BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE columnar.archive_policy
IS 'Metadata for policy-driven archival from hot heap tables into cold columnar tables';


CREATE TABLE columnar.archive_run_log (
    id BIGSERIAL PRIMARY KEY,
    policy_name TEXT NOT NULL REFERENCES columnar.archive_policy(policy_name) ON DELETE CASCADE,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ NOT NULL,
    where_clause TEXT,
    rows_moved BIGINT NOT NULL
);

CREATE INDEX archive_run_log_policy_name_started_at_idx
    ON columnar.archive_run_log(policy_name, started_at DESC);

COMMENT ON TABLE columnar.archive_run_log
IS 'Audit log for archive policies that have been executed';


CREATE OR REPLACE FUNCTION columnar.create_archive_policy(
    target_policy_name TEXT,
    hot_table REGCLASS,
    cold_table REGCLASS,
    selector_function REGPROCEDURE,
    read_view TEXT DEFAULT NULL,
    delete_from_hot BOOLEAN DEFAULT true,
    enabled BOOLEAN DEFAULT true)
RETURNS BOOLEAN
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $policy$
DECLARE
    selector_schema TEXT;
    selector_name TEXT;
    selector_arg_signature TEXT;
    selector_return_type TEXT;
    cold_access_method TEXT;
    normalized_read_view TEXT;
BEGIN
    IF btrim(target_policy_name) = '' THEN
        RAISE EXCEPTION 'target_policy_name must not be empty';
    END IF;

    normalized_read_view := NULLIF(btrim(read_view), '');

    IF columnar._table_column_signature(hot_table) IS DISTINCT FROM
       columnar._table_column_signature(cold_table) THEN
        RAISE EXCEPTION
            'hot table % and cold table % must have matching visible columns',
            hot_table::text, cold_table::text;
    END IF;

    SELECT cold_am.amname
      INTO cold_access_method
      FROM pg_class cold
 LEFT JOIN pg_am cold_am ON cold_am.oid = cold.relam
     WHERE cold.oid = cold_table;

    IF cold_access_method IS DISTINCT FROM 'columnar' THEN
        RAISE EXCEPTION
            'cold table % must use the columnar access method',
            cold_table::text;
    END IF;

    SELECT proc_ns.nspname,
           proc.proname,
           oidvectortypes(proc.proargtypes),
           proc.prorettype::regtype::text
      INTO selector_schema,
           selector_name,
           selector_arg_signature,
           selector_return_type
      FROM pg_proc proc
      JOIN pg_namespace proc_ns ON proc_ns.oid = proc.pronamespace
     WHERE proc.oid = selector_function;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'selector function % does not exist', selector_function::text;
    END IF;

    IF selector_arg_signature IS DISTINCT FROM 'text, regclass, regclass'
       OR selector_return_type IS DISTINCT FROM 'text' THEN
        RAISE EXCEPTION
            'selector function %.% must have signature (text, regclass, regclass) RETURNS text',
            selector_schema, selector_name;
    END IF;

    INSERT INTO columnar.archive_policy(
        policy_name,
        hot_table,
        cold_table,
        read_view,
        selector_function,
        delete_from_hot,
        enabled)
    VALUES(
        target_policy_name,
        hot_table,
        cold_table,
        normalized_read_view,
        selector_function,
        delete_from_hot,
        enabled)
    ON CONFLICT (policy_name) DO UPDATE SET
        hot_table = EXCLUDED.hot_table,
        cold_table = EXCLUDED.cold_table,
        read_view = EXCLUDED.read_view,
        selector_function = EXCLUDED.selector_function,
        delete_from_hot = EXCLUDED.delete_from_hot,
        enabled = EXCLUDED.enabled;

    IF normalized_read_view IS NOT NULL THEN
        PERFORM columnar.create_hot_cold_view(normalized_read_view, hot_table, cold_table);
    END IF;

    RETURN true;
END;
$policy$;

COMMENT ON FUNCTION columnar.create_archive_policy(
    target_policy_name TEXT,
    hot_table REGCLASS,
    cold_table REGCLASS,
    selector_function REGPROCEDURE,
    read_view TEXT,
    delete_from_hot BOOLEAN,
    enabled BOOLEAN)
IS 'Creates or updates a metadata-driven archive policy for moving data from hot heap tables to cold columnar tables';


CREATE OR REPLACE FUNCTION columnar.drop_archive_policy(
    target_policy_name TEXT,
    drop_read_view BOOLEAN DEFAULT false)
RETURNS BOOLEAN
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $drop_policy$
DECLARE
    stored_read_view TEXT;
    view_parts TEXT[];
    target_schema TEXT := 'public';
    target_name TEXT;
BEGIN
    SELECT read_view
      INTO stored_read_view
      FROM columnar.archive_policy
     WHERE policy_name = target_policy_name;

    IF NOT FOUND THEN
        RETURN false;
    END IF;

    DELETE FROM columnar.archive_policy
     WHERE policy_name = target_policy_name;

    IF drop_read_view AND stored_read_view IS NOT NULL THEN
        view_parts := parse_ident(stored_read_view);

        CASE cardinality(view_parts)
            WHEN 1 THEN
                target_name := view_parts[1];
            WHEN 2 THEN
                target_schema := view_parts[1];
                target_name := view_parts[2];
            ELSE
                RAISE EXCEPTION
                    'read_view for policy % must be stored as view or schema.view',
                    target_policy_name;
        END CASE;

        EXECUTE format('DROP VIEW IF EXISTS %I.%I', target_schema, target_name);
    END IF;

    RETURN true;
END;
$drop_policy$;

COMMENT ON FUNCTION columnar.drop_archive_policy(
    target_policy_name TEXT,
    drop_read_view BOOLEAN)
IS 'Deletes an archive policy, its run log rows, and optionally its read view';


CREATE OR REPLACE FUNCTION columnar.run_archive_policy(target_policy_name TEXT)
RETURNS BIGINT
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $run$
DECLARE
    policy_record RECORD;
    selector_schema TEXT;
    selector_name TEXT;
    started_at TIMESTAMPTZ;
    finished_at TIMESTAMPTZ;
    where_clause TEXT;
    moved_count BIGINT := 0;
BEGIN
    SELECT policy_name,
           hot_table,
           cold_table,
           selector_function,
           delete_from_hot
      INTO policy_record
      FROM columnar.archive_policy
     WHERE policy_name = target_policy_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'archive policy % does not exist', target_policy_name;
    END IF;

    SELECT proc_ns.nspname,
           proc.proname
      INTO selector_schema,
           selector_name
      FROM pg_proc proc
      JOIN pg_namespace proc_ns ON proc_ns.oid = proc.pronamespace
     WHERE proc.oid = policy_record.selector_function;

    started_at := clock_timestamp();

    EXECUTE format('SELECT %I.%I($1, $2, $3)', selector_schema, selector_name)
       INTO where_clause
       USING policy_record.policy_name,
             policy_record.hot_table,
             policy_record.cold_table;

    IF where_clause IS NOT NULL AND btrim(where_clause) <> '' THEN
        moved_count := columnar.archive_to_cold(
            policy_record.hot_table,
            policy_record.cold_table,
            where_clause,
            policy_record.delete_from_hot);
    END IF;

    finished_at := clock_timestamp();

    INSERT INTO columnar.archive_run_log(
        policy_name,
        started_at,
        finished_at,
        where_clause,
        rows_moved)
    VALUES(
        policy_record.policy_name,
        started_at,
        finished_at,
        where_clause,
        moved_count);

    RETURN moved_count;
END;
$run$;

COMMENT ON FUNCTION columnar.run_archive_policy(target_policy_name TEXT)
IS 'Runs a named archive policy regardless of its enabled flag and returns the number of rows moved';


CREATE OR REPLACE FUNCTION columnar.run_archive_policies()
RETURNS TABLE(policy_name TEXT, rows_moved BIGINT)
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $run_all$
DECLARE
    policy_record RECORD;
BEGIN
    FOR policy_record IN
        SELECT archive_policy.policy_name
          FROM columnar.archive_policy AS archive_policy
         WHERE archive_policy.enabled
         ORDER BY archive_policy.policy_name
    LOOP
        policy_name := policy_record.policy_name;
        rows_moved := columnar.run_archive_policy(policy_record.policy_name);
        RETURN NEXT;
    END LOOP;

    RETURN;
END;
$run_all$;

COMMENT ON FUNCTION columnar.run_archive_policies()
IS 'Runs all enabled archive policies and returns per-policy row counts';
