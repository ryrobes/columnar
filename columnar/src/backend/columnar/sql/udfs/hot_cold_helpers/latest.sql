CREATE OR REPLACE FUNCTION columnar._table_column_signature(rel REGCLASS)
RETURNS TEXT[]
LANGUAGE sql
STABLE
SET search_path = pg_catalog
AS $$
    SELECT COALESCE(
        array_agg(
            format('%s:%s:%s:%s', attnum, attname, atttypid, atttypmod)
            ORDER BY attnum
        ),
        ARRAY[]::text[]
    )
    FROM pg_attribute
    WHERE attrelid = rel
      AND attnum > 0
      AND NOT attisdropped;
$$;


CREATE OR REPLACE FUNCTION columnar.archive_to_cold(
    hot_table REGCLASS,
    cold_table REGCLASS,
    where_clause TEXT,
    delete_from_hot BOOLEAN DEFAULT true)
RETURNS BIGINT
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $archive$
DECLARE
    hot_schema TEXT;
    hot_name TEXT;
    cold_schema TEXT;
    cold_name TEXT;
    cold_access_method TEXT;
    moved_count BIGINT;
BEGIN
    IF btrim(where_clause) = '' THEN
        RAISE EXCEPTION 'where_clause must not be empty';
    END IF;

    IF columnar._table_column_signature(hot_table) IS DISTINCT FROM
       columnar._table_column_signature(cold_table) THEN
        RAISE EXCEPTION
            'hot table % and cold table % must have matching visible columns',
            hot_table::text, cold_table::text;
    END IF;

    SELECT hot_ns.nspname, hot.relname
      INTO hot_schema, hot_name
      FROM pg_class hot
      JOIN pg_namespace hot_ns ON hot_ns.oid = hot.relnamespace
     WHERE hot.oid = hot_table;

    SELECT cold_ns.nspname, cold.relname, cold_am.amname
      INTO cold_schema, cold_name, cold_access_method
      FROM pg_class cold
      JOIN pg_namespace cold_ns ON cold_ns.oid = cold.relnamespace
 LEFT JOIN pg_am cold_am ON cold_am.oid = cold.relam
     WHERE cold.oid = cold_table;

    IF cold_access_method IS DISTINCT FROM 'columnar' THEN
        RAISE EXCEPTION
            'cold table % must use the columnar access method',
            cold_table::text;
    END IF;

    IF delete_from_hot THEN
        EXECUTE format(
            'WITH moved_rows AS (' ||
            '  DELETE FROM %I.%I WHERE %s RETURNING *' ||
            ') ' ||
            'INSERT INTO %I.%I SELECT * FROM moved_rows',
            hot_schema, hot_name, where_clause, cold_schema, cold_name);
    ELSE
        EXECUTE format(
            'INSERT INTO %I.%I ' ||
            'SELECT * FROM %I.%I WHERE %s',
            cold_schema, cold_name, hot_schema, hot_name, where_clause);
    END IF;

    GET DIAGNOSTICS moved_count = ROW_COUNT;
    RETURN moved_count;
END;
$archive$;

COMMENT ON FUNCTION columnar.archive_to_cold(
    hot_table REGCLASS,
    cold_table REGCLASS,
    where_clause TEXT,
    delete_from_hot BOOLEAN)
IS 'Moves or copies rows from a hot table into a cold columnar table using a SQL WHERE clause';


CREATE OR REPLACE FUNCTION columnar.create_hot_cold_view(
    view_name TEXT,
    hot_table REGCLASS,
    cold_table REGCLASS)
RETURNS BOOLEAN
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $view$
DECLARE
    hot_schema TEXT;
    hot_name TEXT;
    cold_schema TEXT;
    cold_name TEXT;
    view_parts TEXT[];
    target_schema TEXT = 'public';
    target_name TEXT;
BEGIN
    IF columnar._table_column_signature(hot_table) IS DISTINCT FROM
       columnar._table_column_signature(cold_table) THEN
        RAISE EXCEPTION
            'hot table % and cold table % must have matching visible columns',
            hot_table::text, cold_table::text;
    END IF;

    view_parts := parse_ident(view_name);

    CASE cardinality(view_parts)
        WHEN 1 THEN
            target_name := view_parts[1];
        WHEN 2 THEN
            target_schema := view_parts[1];
            target_name := view_parts[2];
        ELSE
            RAISE EXCEPTION
                'view_name must be provided as view or schema.view';
    END CASE;

    SELECT hot_ns.nspname, hot.relname
      INTO hot_schema, hot_name
      FROM pg_class hot
      JOIN pg_namespace hot_ns ON hot_ns.oid = hot.relnamespace
     WHERE hot.oid = hot_table;

    SELECT cold_ns.nspname, cold.relname
      INTO cold_schema, cold_name
      FROM pg_class cold
      JOIN pg_namespace cold_ns ON cold_ns.oid = cold.relnamespace
     WHERE cold.oid = cold_table;

    EXECUTE format(
        'CREATE OR REPLACE VIEW %I.%I AS ' ||
        'SELECT * FROM %I.%I ' ||
        'UNION ALL ' ||
        'SELECT * FROM %I.%I',
        target_schema, target_name,
        hot_schema, hot_name,
        cold_schema, cold_name);

    RETURN true;
END;
$view$;

COMMENT ON FUNCTION columnar.create_hot_cold_view(
    view_name TEXT,
    hot_table REGCLASS,
    cold_table REGCLASS)
IS 'Creates or replaces a UNION ALL view over hot and cold tables for hybrid read paths';
