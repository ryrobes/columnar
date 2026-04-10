#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import math
import os
import statistics
import subprocess
import sys
import textwrap
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote


BASE_EVENT_TIME = datetime(2026, 1, 1, tzinfo=timezone.utc)
QUERY_FILE = Path(__file__).with_name("queries") / "clickbench_like.sql"


TABLE_COLUMNS = textwrap.dedent(
    """
    event_id bigint not null,
    work_id bigint not null,
    tenant_id integer not null,
    event_time timestamptz not null,
    event_date date not null,
    user_id bigint not null,
    session_id bigint not null,
    region_id integer not null,
    service text not null,
    kind text not null,
    status integer not null,
    severity integer not null,
    device_type text not null,
    url text not null,
    title text not null,
    search_phrase text not null,
    payload text not null,
    payload_bytes integer not null,
    duration_ms integer not null,
    is_error boolean not null,
    is_refresh boolean not null,
    revenue_cents bigint not null
    """
).strip()


@dataclass
class LayoutContext:
    name: str
    schema: str
    logical_table: str
    write_table: str
    size_tables: list[str]
    cold_table: str | None = None
    hot_start_time: datetime | None = None
    partition_bounds: list[tuple[str, datetime, datetime]] | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Benchmark heap, columnar, and hybrid hot/cold layouts with "
            "synthetic ClickBench-style event data."
        )
    )
    parser.add_argument(
        "--dsn",
        help="PostgreSQL DSN. Defaults to DATABASE_URL or a DSN built from .env.",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=250_000,
        help="Initial synthetic row count per layout. Default: %(default)s",
    )
    parser.add_argument(
        "--work-rows",
        type=int,
        default=500,
        help="Rows per synthetic work unit. Default: %(default)s",
    )
    parser.add_argument(
        "--hot-fraction",
        type=float,
        default=0.20,
        help="Fraction of work units kept hot in the hybrid layout. Default: %(default)s",
    )
    parser.add_argument(
        "--layouts",
        default="heap,columnar,hybrid_hot_cold,hybrid_partitioned",
        help="Comma-separated layouts to benchmark. Default: %(default)s",
    )
    parser.add_argument(
        "--query-runs",
        type=int,
        default=3,
        help="Number of times to run each analytical query. Default: %(default)s",
    )
    parser.add_argument(
        "--append-batches",
        type=int,
        default=10,
        help="Number of append batches after the read suite. Default: %(default)s",
    )
    parser.add_argument(
        "--append-rows",
        type=int,
        default=5_000,
        help="Rows per append batch. Default: %(default)s",
    )
    parser.add_argument(
        "--output",
        help="Optional path to write a JSON report.",
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Drop benchmark schemas after the run completes.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print SQL step names as they execute.",
    )

    args = parser.parse_args()

    if args.rows <= 0:
        parser.error("--rows must be positive")
    if args.work_rows <= 0:
        parser.error("--work-rows must be positive")
    if not 0 < args.hot_fraction < 1:
        parser.error("--hot-fraction must be between 0 and 1")
    if args.query_runs <= 0:
        parser.error("--query-runs must be positive")
    if args.append_batches < 0:
        parser.error("--append-batches must be non-negative")
    if args.append_rows <= 0:
        parser.error("--append-rows must be positive")

    return args


def load_dotenv(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip("'").strip('"')

    return values


def default_dsn() -> str:
    if os.getenv("DATABASE_URL"):
        return os.environ["DATABASE_URL"]

    env = load_dotenv(Path.cwd() / ".env")
    user = env.get("POSTGRES_USER", "postgres")
    password = env.get("POSTGRES_PASSWORD", "postgres")
    port = env.get("POSTGRES_PORT", "5432")
    database = env.get("POSTGRES_DB", "postgres")

    return (
        f"postgresql://{quote(user)}:{quote(password)}@127.0.0.1:{port}/{quote(database)}"
    )


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def layout_schema(layout: str) -> str:
    return f"bench_{layout}"


def table_columns_clause() -> str:
    return ",\n    ".join(line.strip().rstrip(",") for line in TABLE_COLUMNS.splitlines())


def synthetic_select(start_id: int, end_id: int, work_rows: int) -> str:
    return textwrap.dedent(
        f"""
        SELECT
            gs AS event_id,
            ((gs - 1) / {work_rows}) + 1 AS work_id,
            ((gs - 1) % 64) + 1 AS tenant_id,
            {sql_literal(BASE_EVENT_TIME.isoformat())}::timestamptz
                + ((gs - 1) * interval '1 second') AS event_time,
            ({sql_literal(BASE_EVENT_TIME.date().isoformat())}::date
                + (((gs - 1) / 86400)::int)) AS event_date,
            1000000000 + (gs % 250000) AS user_id,
            500000000 + (gs % 50000) AS session_id,
            ((gs - 1) % 128) + 1 AS region_id,
            CASE gs % 6
                WHEN 0 THEN 'api'
                WHEN 1 THEN 'ingest'
                WHEN 2 THEN 'worker'
                WHEN 3 THEN 'search'
                WHEN 4 THEN 'ui'
                ELSE 'cron'
            END AS service,
            CASE gs % 7
                WHEN 0 THEN 'state'
                WHEN 1 THEN 'event'
                WHEN 2 THEN 'metric'
                WHEN 3 THEN 'audit'
                WHEN 4 THEN 'search'
                WHEN 5 THEN 'alert'
                ELSE 'trace'
            END AS kind,
            CASE gs % 9
                WHEN 0 THEN 200
                WHEN 1 THEN 201
                WHEN 2 THEN 202
                WHEN 3 THEN 204
                WHEN 4 THEN 400
                WHEN 5 THEN 404
                WHEN 6 THEN 409
                WHEN 7 THEN 429
                ELSE 500
            END AS status,
            (gs % 5) AS severity,
            CASE gs % 4
                WHEN 0 THEN 'desktop'
                WHEN 1 THEN 'mobile'
                WHEN 2 THEN 'worker'
                ELSE 'server'
            END AS device_type,
            'https://app.example.com/'
                || CASE WHEN gs % 10 = 0 THEN 'api' ELSE 'resource' END
                || '/'
                || (gs % 20000) AS url,
            'title_' || (gs % 20000) || '_' || (gs % 97) AS title,
            CASE
                WHEN gs % 9 = 0 THEN 'google_' || (gs % 5000)
                WHEN gs % 11 = 0 THEN 'error_' || (gs % 2000)
                ELSE ''
            END AS search_phrase,
            repeat(chr(97 + (gs % 26)), 32 + (gs % 64)) AS payload,
            32 + (gs % 64) AS payload_bytes,
            ((gs * 13) % 3000) + 1 AS duration_ms,
            (gs % 13 = 0) AS is_error,
            (gs % 17 = 0) AS is_refresh,
            ((gs * 29) % 100000) AS revenue_cents
        FROM generate_series({start_id}, {end_id}) AS gs
        """
    ).strip()


def run_psql(dsn: str, sql: str, verbose: bool = False) -> str:
    if verbose:
        print(f"[sql] {sql.splitlines()[0][:100]}", file=sys.stderr)

    result = subprocess.run(
        ["psql", dsn, "-X", "-v", "ON_ERROR_STOP=1", "-qAt"],
        input=sql,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"psql failed with exit code {result.returncode}\n"
            f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )
    return result.stdout.strip()


def exec_sql(dsn: str, sql: str, verbose: bool = False) -> None:
    run_psql(dsn, sql, verbose=verbose)


def timed_sql(dsn: str, sql: str, verbose: bool = False) -> float:
    start = time.perf_counter()
    exec_sql(dsn, sql, verbose=verbose)
    return time.perf_counter() - start


def query_scalar_int(dsn: str, sql: str) -> int:
    output = run_psql(dsn, sql)
    return int(output.strip())


def query_scalar_json(dsn: str, sql: str) -> Any:
    output = run_psql(dsn, sql)
    return json.loads(output)


def explain_timing_ms(dsn: str, query: str) -> float:
    sql = textwrap.dedent(
        f"""
        SET client_min_messages TO warning;
        SET jit = off;
        EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
        {query}
        """
    )
    plan = query_scalar_json(dsn, sql)
    return float(plan[0]["Execution Time"])


def parse_queries(path: Path) -> list[tuple[str, str]]:
    queries: list[tuple[str, str]] = []
    current_name: str | None = None
    current_sql: list[str] = []

    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith("-- name:"):
            if current_name is not None:
                queries.append((current_name, "\n".join(current_sql).strip()))
            current_name = line.split(":", 1)[1].strip()
            current_sql = []
            continue

        if current_name is None:
            continue

        current_sql.append(line)

    if current_name is not None:
        queries.append((current_name, "\n".join(current_sql).strip()))

    return [(name, sql.rstrip(";")) for name, sql in queries if sql.strip()]

def build_partition_bounds(
    total_rows: int,
    target_partitions: int,
) -> list[tuple[datetime, datetime]]:
    if total_rows <= 0:
        raise ValueError("total_rows must be positive")

    span_seconds = math.ceil(total_rows / target_partitions)
    bounds: list[tuple[datetime, datetime]] = []
    start = BASE_EVENT_TIME
    end = BASE_EVENT_TIME + timedelta(seconds=total_rows)

    while start < end:
        upper = min(start + timedelta(seconds=span_seconds), end)
        bounds.append((start, upper))
        start = upper

    return bounds


def create_layout_schema(
    dsn: str,
    layout: str,
    rows: int,
    append_rows_total: int,
    hot_fraction: float,
    verbose: bool = False,
) -> LayoutContext:
    schema = layout_schema(layout)
    exec_sql(
        dsn,
        textwrap.dedent(
            f"""
            CREATE EXTENSION IF NOT EXISTS columnar;
            DROP SCHEMA IF EXISTS {schema} CASCADE;
            CREATE SCHEMA {schema};
            """
        ),
        verbose=verbose,
    )

    columns = table_columns_clause()

    if layout == "heap":
        exec_sql(
            dsn,
            textwrap.dedent(
                f"""
                CREATE TABLE {schema}.events (
                    {columns}
                ) USING heap;
                """
            ),
            verbose=verbose,
        )
        return LayoutContext(
            name=layout,
            schema=schema,
            logical_table=f"{schema}.events",
            write_table=f"{schema}.events",
            size_tables=["events"],
        )

    if layout == "columnar":
        exec_sql(
            dsn,
            textwrap.dedent(
                f"""
                CREATE TABLE {schema}.events (
                    {columns}
                ) USING columnar;
                """
            ),
            verbose=verbose,
        )
        return LayoutContext(
            name=layout,
            schema=schema,
            logical_table=f"{schema}.events",
            write_table=f"{schema}.events",
            size_tables=["events"],
        )

    if layout == "hybrid_hot_cold":
        exec_sql(
            dsn,
            textwrap.dedent(
                f"""
                CREATE TABLE {schema}.events_hot (
                    {columns}
                ) USING heap;

                CREATE TABLE {schema}.events_cold (
                    {columns}
                ) USING columnar;

                CREATE VIEW {schema}.events_read AS
                SELECT * FROM {schema}.events_hot
                UNION ALL
                SELECT * FROM {schema}.events_cold;
                """
            ),
            verbose=verbose,
        )
        return LayoutContext(
            name=layout,
            schema=schema,
            logical_table=f"{schema}.events_read",
            write_table=f"{schema}.events_hot",
            cold_table=f"{schema}.events_cold",
            size_tables=["events_hot", "events_cold"],
        )

    if layout == "hybrid_partitioned":
        total_rows = rows + append_rows_total
        target_partitions = min(max(4, math.ceil(total_rows / 20_000)), 32)
        hot_rows = max(
            math.ceil(rows * hot_fraction),
            math.ceil(total_rows / target_partitions),
        )
        hot_start_time = BASE_EVENT_TIME + timedelta(seconds=max(0, rows - hot_rows))
        partition_bounds = build_partition_bounds(total_rows, target_partitions)

        exec_sql(
            dsn,
            textwrap.dedent(
                f"""
                CREATE TABLE {schema}.events (
                    {columns}
                ) PARTITION BY RANGE (event_time);
                """
            ),
            verbose=verbose,
        )

        size_tables: list[str] = []
        detailed_bounds: list[tuple[str, datetime, datetime]] = []
        for index, (start_bound, end_bound) in enumerate(partition_bounds):
            partition_name = f"events_p{index:03d}"
            size_tables.append(partition_name)
            detailed_bounds.append((partition_name, start_bound, end_bound))
            exec_sql(
                dsn,
                textwrap.dedent(
                    f"""
                    CREATE TABLE {schema}.{partition_name}
                    PARTITION OF {schema}.events
                    FOR VALUES FROM ({sql_literal(start_bound.isoformat())})
                    TO ({sql_literal(end_bound.isoformat())})
                    USING heap;
                    """
                ),
                verbose=verbose,
            )

        return LayoutContext(
            name=layout,
            schema=schema,
            logical_table=f"{schema}.events",
            write_table=f"{schema}.events",
            size_tables=size_tables,
            hot_start_time=hot_start_time,
            partition_bounds=detailed_bounds,
        )

    raise ValueError(f"unsupported layout: {layout}")


def load_rows(
    dsn: str,
    table_name: str,
    start_id: int,
    end_id: int,
    work_rows: int,
    verbose: bool = False,
) -> float:
    sql = textwrap.dedent(
        f"""
        INSERT INTO {table_name}
        SELECT *
        FROM (
            {synthetic_select(start_id, end_id, work_rows)}
        ) AS synthetic_rows;
        """
    )
    return timed_sql(dsn, sql, verbose=verbose)


def analyze_layout(dsn: str, ctx: LayoutContext, verbose: bool = False) -> float:
    targets = [ctx.logical_table]
    if ctx.name == "hybrid_hot_cold":
        targets = [f"{ctx.schema}.events_hot", f"{ctx.schema}.events_cold"]

    start = time.perf_counter()
    for target in targets:
        exec_sql(dsn, f"ANALYZE {target};", verbose=verbose)
    return time.perf_counter() - start


def archive_hybrid(
    dsn: str,
    ctx: LayoutContext,
    rows: int,
    work_rows: int,
    hot_fraction: float,
    verbose: bool = False,
) -> dict[str, Any]:
    total_work_ids = math.ceil(rows / work_rows)
    hot_work_ids = max(1, math.floor(total_work_ids * hot_fraction))
    cold_cutoff = max(0, total_work_ids - hot_work_ids)

    if ctx.cold_table is None or cold_cutoff == 0:
        return {
            "kind": "archive_to_cold",
            "seconds": 0.0,
            "archived_work_id_cutoff": cold_cutoff,
            "hot_rows": rows,
            "cold_rows": 0,
        }

    sql = textwrap.dedent(
        f"""
        BEGIN;
        INSERT INTO {ctx.cold_table}
        SELECT *
        FROM {ctx.write_table}
        WHERE work_id <= {cold_cutoff};

        DELETE FROM {ctx.write_table}
        WHERE work_id <= {cold_cutoff};
        COMMIT;
        """
    )
    seconds = timed_sql(dsn, sql, verbose=verbose)

    hot_rows = query_scalar_int(dsn, f"SELECT COUNT(*) FROM {ctx.write_table};")
    cold_rows = query_scalar_int(dsn, f"SELECT COUNT(*) FROM {ctx.cold_table};")

    return {
        "kind": "archive_to_cold",
        "seconds": seconds,
        "archived_work_id_cutoff": cold_cutoff,
        "hot_rows": hot_rows,
        "cold_rows": cold_rows,
    }


def convert_partitioned_cold_partitions(
    dsn: str,
    ctx: LayoutContext,
    verbose: bool = False,
) -> dict[str, Any]:
    if ctx.partition_bounds is None or ctx.hot_start_time is None:
        return {"kind": "convert_cold_partitions", "seconds": 0.0, "converted_partitions": []}

    converted: list[str] = []
    start = time.perf_counter()

    for partition_name, start_bound, end_bound in ctx.partition_bounds:
        if end_bound > ctx.hot_start_time:
            continue

        exec_sql(
            dsn,
            textwrap.dedent(
                f"""
                ALTER TABLE {ctx.logical_table}
                DETACH PARTITION {ctx.schema}.{partition_name};

                SELECT columnar.alter_table_set_access_method(
                    {sql_literal(f"{ctx.schema}.{partition_name}")},
                    'columnar');

                ALTER TABLE {ctx.logical_table}
                ATTACH PARTITION {ctx.schema}.{partition_name}
                FOR VALUES FROM ({sql_literal(start_bound.isoformat())})
                TO ({sql_literal(end_bound.isoformat())});
                """
            ),
            verbose=verbose,
        )
        converted.append(partition_name)

    seconds = time.perf_counter() - start

    return {
        "kind": "convert_cold_partitions",
        "seconds": seconds,
        "converted_partitions": converted,
        "converted_partition_count": len(converted),
        "hot_start_time": ctx.hot_start_time.isoformat(),
    }


def relation_sizes(dsn: str, ctx: LayoutContext) -> dict[str, int]:
    parts = ",".join(sql_literal(name) for name in ctx.size_tables)
    sql = textwrap.dedent(
        f"""
        SELECT json_object_agg(relname, size_bytes)
        FROM (
            SELECT c.relname, pg_total_relation_size(c.oid) AS size_bytes
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = {sql_literal(ctx.schema)}
              AND c.relname IN ({parts})
            ORDER BY c.relname
        ) sized;
        """
    )
    return {key: int(value) for key, value in query_scalar_json(dsn, sql).items()}


def build_query_context(rows: int, work_rows: int, ctx: LayoutContext) -> dict[str, str]:
    total_work_ids = math.ceil(rows / work_rows)
    hot_work_upper = total_work_ids
    hot_work_lower = max(1, hot_work_upper - 24)

    recent_end = BASE_EVENT_TIME + timedelta(seconds=rows)
    recent_start = max(BASE_EVENT_TIME, recent_end - timedelta(hours=6))

    return {
        "logical_table": ctx.logical_table,
        "hot_work_lower": str(hot_work_lower),
        "hot_work_upper": str(hot_work_upper),
        "recent_window_start": sql_literal(recent_start.isoformat()),
        "recent_window_end": sql_literal(recent_end.isoformat()),
    }


def run_query_suite(
    dsn: str,
    ctx: LayoutContext,
    rows: int,
    work_rows: int,
    query_runs: int,
) -> dict[str, dict[str, Any]]:
    context = build_query_context(rows, work_rows, ctx)
    results: dict[str, dict[str, Any]] = {}

    for name, template in parse_queries(QUERY_FILE):
        query = template.format_map(context)
        timings = [explain_timing_ms(dsn, query) for _ in range(query_runs)]
        results[name] = {
            "query": query,
            "runs_ms": [round(value, 3) for value in timings],
            "median_ms": round(statistics.median(timings), 3),
            "min_ms": round(min(timings), 3),
            "max_ms": round(max(timings), 3),
        }

    return results


def append_batches(
    dsn: str,
    ctx: LayoutContext,
    start_row: int,
    work_rows: int,
    batch_rows: int,
    batch_count: int,
    verbose: bool = False,
) -> dict[str, Any]:
    timings: list[float] = []
    next_start = start_row + 1

    for _ in range(batch_count):
        next_end = next_start + batch_rows - 1
        timings.append(
            load_rows(
                dsn,
                ctx.write_table,
                next_start,
                next_end,
                work_rows,
                verbose=verbose,
            )
        )
        next_start = next_end + 1

    if not timings:
        return {"batches": 0, "rows_per_batch": batch_rows, "runs_seconds": []}

    return {
        "batches": batch_count,
        "rows_per_batch": batch_rows,
        "runs_seconds": [round(value, 4) for value in timings],
        "total_seconds": round(sum(timings), 4),
        "median_seconds": round(statistics.median(timings), 4),
    }


def update_hot_slice(
    dsn: str,
    ctx: LayoutContext,
    rows: int,
    work_rows: int,
    verbose: bool = False,
) -> dict[str, Any]:
    if ctx.name == "hybrid_partitioned" and ctx.hot_start_time is not None:
        predicate = f"event_time >= {sql_literal(ctx.hot_start_time.isoformat())}"
        sql = textwrap.dedent(
            f"""
            WITH touched AS (
                UPDATE {ctx.write_table}
                SET severity = (severity + 1) % 5,
                    status = CASE WHEN status = 200 THEN 202 ELSE status END,
                    is_refresh = NOT is_refresh
                WHERE {predicate}
                RETURNING 1
            )
            SELECT COUNT(*) FROM touched;
            """
        )

        start = time.perf_counter()
        updated = int(run_psql(dsn, sql, verbose=verbose) or "0")
        seconds = time.perf_counter() - start

        return {
            "seconds": round(seconds, 4),
            "rows_updated": updated,
            "predicate": predicate,
        }

    total_work_ids = math.ceil(rows / work_rows)
    hot_work_upper = total_work_ids
    hot_work_lower = max(1, hot_work_upper - 24)

    sql = textwrap.dedent(
        f"""
        WITH touched AS (
            UPDATE {ctx.write_table}
            SET severity = (severity + 1) % 5,
                status = CASE WHEN status = 200 THEN 202 ELSE status END,
                is_refresh = NOT is_refresh
            WHERE work_id BETWEEN {hot_work_lower} AND {hot_work_upper}
            RETURNING 1
        )
        SELECT COUNT(*) FROM touched;
        """
    )

    start = time.perf_counter()
    updated = int(run_psql(dsn, sql, verbose=verbose) or "0")
    seconds = time.perf_counter() - start

    return {
        "seconds": round(seconds, 4),
        "rows_updated": updated,
        "work_id_lower": hot_work_lower,
        "work_id_upper": hot_work_upper,
    }


def cleanup_layouts(dsn: str, layouts: list[str], verbose: bool = False) -> None:
    for layout in layouts:
        exec_sql(
            dsn,
            f"DROP SCHEMA IF EXISTS {layout_schema(layout)} CASCADE;",
            verbose=verbose,
        )


def print_summary(report: dict[str, Any]) -> None:
    print("\nStorage layout benchmark summary")
    print("=" * 34)
    print(f"Rows: {report['config']['rows']}")
    print(f"Query runs: {report['config']['query_runs']}")
    print(
        "Layouts: " + ", ".join(report["config"]["layouts"])
    )

    for layout_name, result in report["layouts"].items():
        print(f"\n[{layout_name}]")
        print(
            f"  load={result['load_seconds']:.3f}s "
            f"analyze={result['analyze_seconds']:.3f}s "
            f"append_total={result['append']['total_seconds'] if result['append'].get('total_seconds') is not None else 0:.3f}s "
            f"update={result['update']['seconds']:.3f}s"
        )
        transition = result.get("transition")
        if transition and transition.get("kind") == "archive_to_cold":
            print(
                f"  archive={transition['seconds']:.3f}s "
                f"hot_rows={transition['hot_rows']} "
                f"cold_rows={transition['cold_rows']}"
            )
        elif transition and transition.get("kind") == "convert_cold_partitions":
            print(
                f"  convert={transition['seconds']:.3f}s "
                f"converted_partitions={transition['converted_partition_count']} "
                f"hot_start={transition['hot_start_time']}"
            )
        print(
            "  size_bytes="
            + ", ".join(f"{name}:{size}" for name, size in result["sizes"].items())
        )
        top_queries = sorted(
            result["queries"].items(),
            key=lambda item: item[1]["median_ms"],
            reverse=True,
        )[:3]
        for query_name, query_result in top_queries:
            print(
                f"  query {query_name}: median={query_result['median_ms']:.3f} ms "
                f"runs={query_result['runs_ms']}"
            )

    layouts = list(report["layouts"].keys())
    if not layouts:
        return

    common_queries = sorted(
        set.intersection(
            *(
                set(report["layouts"][layout]["queries"].keys())
                for layout in layouts
            )
        )
    )

    if not common_queries:
        return

    print("\nPer-query medians (ms)")
    print("=" * 22)
    header = ["query"] + layouts
    widths = [max(24, len(header[0]))]
    widths.extend(max(18, len(name)) for name in layouts)

    def render_row(values: list[str]) -> str:
        return "  ".join(value.ljust(width) for value, width in zip(values, widths))

    print(render_row(header))
    print(render_row(["-" * len(item) for item in header]))

    for query_name in common_queries:
        row = [query_name]
        for layout in layouts:
            median = report["layouts"][layout]["queries"][query_name]["median_ms"]
            row.append(f"{median:.3f}")
        print(render_row(row))


def main() -> int:
    args = parse_args()
    dsn = args.dsn or default_dsn()
    layouts = [item.strip() for item in args.layouts.split(",") if item.strip()]

    report: dict[str, Any] = {
        "config": {
            "dsn": "<redacted>",
            "rows": args.rows,
            "work_rows": args.work_rows,
            "hot_fraction": args.hot_fraction,
            "layouts": layouts,
            "query_runs": args.query_runs,
            "append_batches": args.append_batches,
            "append_rows": args.append_rows,
            "query_file": str(QUERY_FILE),
        },
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "layouts": {},
    }

    try:
        for layout in layouts:
            ctx = create_layout_schema(
                dsn,
                layout,
                rows=args.rows,
                append_rows_total=(args.append_rows * args.append_batches),
                hot_fraction=args.hot_fraction,
                verbose=args.verbose,
            )

            load_seconds = load_rows(
                dsn,
                ctx.write_table,
                1,
                args.rows,
                args.work_rows,
                verbose=args.verbose,
            )

            transition_info: dict[str, Any] | None = None
            if layout == "hybrid_hot_cold":
                transition_info = archive_hybrid(
                    dsn,
                    ctx,
                    args.rows,
                    args.work_rows,
                    args.hot_fraction,
                    verbose=args.verbose,
                )
            elif layout == "hybrid_partitioned":
                transition_info = convert_partitioned_cold_partitions(
                    dsn,
                    ctx,
                    verbose=args.verbose,
                )

            analyze_seconds = analyze_layout(dsn, ctx, verbose=args.verbose)
            sizes = relation_sizes(dsn, ctx)
            query_results = run_query_suite(
                dsn,
                ctx,
                args.rows,
                args.work_rows,
                args.query_runs,
            )
            append_result = append_batches(
                dsn,
                ctx,
                args.rows,
                args.work_rows,
                args.append_rows,
                args.append_batches,
                verbose=args.verbose,
            )
            update_result = update_hot_slice(
                dsn,
                ctx,
                args.rows + (args.append_rows * args.append_batches),
                args.work_rows,
                verbose=args.verbose,
            )
            logical_rows = query_scalar_int(dsn, f"SELECT COUNT(*) FROM {ctx.logical_table};")

            report["layouts"][layout] = {
                "schema": ctx.schema,
                "logical_table": ctx.logical_table,
                "write_table": ctx.write_table,
                "load_seconds": round(load_seconds, 4),
                "transition": transition_info,
                "analyze_seconds": round(analyze_seconds, 4),
                "sizes": sizes,
                "logical_rows_after_benchmark": logical_rows,
                "queries": query_results,
                "append": append_result,
                "update": update_result,
            }

    finally:
        if args.cleanup:
            cleanup_layouts(dsn, layouts, verbose=args.verbose)

    print_summary(report)

    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(f"\nWrote JSON report to {output_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
