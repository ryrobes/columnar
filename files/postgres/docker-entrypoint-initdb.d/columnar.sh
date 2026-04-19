#!/bin/bash

set -e

DEFAULT_TABLE_ACCESS_METHOD="${COLUMNAR_DEFAULT_TABLE_ACCESS_METHOD:-columnar}"

case "${DEFAULT_TABLE_ACCESS_METHOD}" in
  heap|columnar)
    ;;
  *)
    echo "COLUMNAR_DEFAULT_TABLE_ACCESS_METHOD must be 'heap' or 'columnar'" >&2
    exit 1
    ;;
esac

configure_database() {
  local database="$1"

  psql -v ON_ERROR_STOP=1 \
    --username "$POSTGRES_USER" \
    --dbname "$database" \
    --set=database="$database" \
    --set=default_table_access_method="$DEFAULT_TABLE_ACCESS_METHOD" <<-'EOSQL'
CREATE EXTENSION IF NOT EXISTS columnar;
ALTER EXTENSION columnar UPDATE;
CREATE EXTENSION IF NOT EXISTS vector;
ALTER DATABASE :"database" SET default_table_access_method = :'default_table_access_method';
EOSQL
}

while IFS= read -r database; do
  configure_database "$database"
done < <(
  psql -v ON_ERROR_STOP=1 \
    --username "$POSTGRES_USER" \
    --dbname "$POSTGRES_DB" \
    --tuples-only \
    --no-align \
    --command "SELECT datname FROM pg_database WHERE datallowconn ORDER BY datname"
)

psql -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  --set=default_table_access_method="$DEFAULT_TABLE_ACCESS_METHOD" <<-'EOSQL'
ALTER SYSTEM SET default_table_access_method = :'default_table_access_method';
SELECT pg_reload_conf();
EOSQL
