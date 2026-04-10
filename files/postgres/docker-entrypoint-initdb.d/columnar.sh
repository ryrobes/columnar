#!/bin/bash

set -e

DEFAULT_TABLE_ACCESS_METHOD="${COLUMNAR_DEFAULT_TABLE_ACCESS_METHOD:-heap}"

case "${DEFAULT_TABLE_ACCESS_METHOD}" in
  heap|columnar)
    ;;
  *)
    echo "COLUMNAR_DEFAULT_TABLE_ACCESS_METHOD must be 'heap' or 'columnar'" >&2
    exit 1
    ;;
esac

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
  CREATE EXTENSION IF NOT EXISTS columnar;
  ALTER EXTENSION columnar UPDATE;
  ALTER DATABASE "${POSTGRES_DB}" SET default_table_access_method = '${DEFAULT_TABLE_ACCESS_METHOD}';
EOSQL
