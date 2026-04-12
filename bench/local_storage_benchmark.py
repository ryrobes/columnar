#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import math
import os
import re
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
HYDRA_LAYOUTS = {"heap", "columnar"}
IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


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
class BenchmarkTarget:
    label: str
    layout: str
    dsn: str


@dataclass
class LayoutContext:
    name: str
    layout: str
    schema: str
    logical_table: str
    write_table: str
    size_tables: list[str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Benchmark columnar vs heap layouts on the Hydra-fork PG18 instance, "
            "and optionally compare against one or more external Postgres instances."
        )
    )
    parser.add_argument(
        "--dsn",
        help="Hydra PG18 DSN. Default: postgresql://postgres:postgres@127.0.0.1:5432/testdb",
    )
    parser.add_argument(
        "--compare",
        action="append",
        default=[],
        metavar="LABEL=DSN",
        help=(
            "Add an external Postgres instance to compare against. "
            "Can be repeated. Format: label=dsn  "
            "Example: --compare neon=postgresql://user:pass@host/db "
            "--compare supabase=postgresql://..."
        ),
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
        "--layouts",
        default="heap,columnar",
        help="Comma-separated Hydra layouts to benchmark. Default: %(default)s",
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
    if args.query_runs <= 0:
        parser.error("--query-runs must be positive")
    if args.append_batches < 0:
        parser.error("--append-batches must be non-negative")
    if args.append_rows <= 0:
        parser.error("--append-rows must be positive")

    hydra_layouts = [item.strip() for item in args.layouts.split(",") if item.strip()]
    unknown = sorted(set(hydra_layouts) - HYDRA_LAYOUTS)
    if unknown:
        parser.error("--layouts contains unsupported layouts: " + ", ".join(unknown))
    if not hydra_layouts:
        parser.error("--layouts must contain at least one layout")

    # Parse --compare entries
    compare_targets: list[tuple[str, str]] = []
    for entry in args.compare:
        if "=" not in entry:
            parser.error(f"--compare must be label=dsn, got: {entry}")
        label, dsn = entry.split("=", 1)
        label = label.strip()
        if not IDENTIFIER_RE.match(label):
            parser.error(f"--compare label must be a simple identifier: {label}")
        if label in hydra_layouts:
            parser.error(f"--compare label '{label}' conflicts with a Hydra layout name")
        compare_targets.append((label, dsn.strip()))
    args._compare_targets = compare_targets

    return args


def default_dsn() -> str:
    if os.getenv("DATABASE_URL"):
        return os.environ["DATABASE_URL"]

    env = load_dotenv(Path.cwd() / ".env")
    user = env.get("POSTGRES_USER", "postgres")
    password = env.get("POSTGRES_PASSWORD", "postgres")
    port = env.get("POSTGRES_PORT", "5432")
    database = env.get("POSTGRES_DB", "testdb")

    return (
        f"postgresql://{quote(user)}:{quote(password)}@127.0.0.1:{port}/{quote(database)}"
    )


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


def create_layout_schema(
    dsn: str,
    label: str,
    layout: str,
    verbose: bool = False,
) -> LayoutContext:
    schema = layout_schema(label)
    columns = table_columns_clause()

    if layout == "external_postgres":
        exec_sql(
            dsn,
            textwrap.dedent(
                f"""
                DROP SCHEMA IF EXISTS {schema} CASCADE;
                CREATE SCHEMA {schema};
                CREATE TABLE {schema}.events (
                    {columns}
                );
                """
            ),
            verbose=verbose,
        )
        return LayoutContext(
            name=label,
            layout=layout,
            schema=schema,
            logical_table=f"{schema}.events",
            write_table=f"{schema}.events",
            size_tables=["events"],
        )

    # Hydra layouts (heap or columnar) — ensure columnar extension exists
    am = "columnar" if layout == "columnar" else "heap"
    exec_sql(
        dsn,
        textwrap.dedent(
            f"""
            CREATE EXTENSION IF NOT EXISTS columnar;
            DROP SCHEMA IF EXISTS {schema} CASCADE;
            CREATE SCHEMA {schema};
            CREATE TABLE {schema}.events (
                {columns}
            ) USING {am};
            """
        ),
        verbose=verbose,
    )
    return LayoutContext(
        name=label,
        layout=layout,
        schema=schema,
        logical_table=f"{schema}.events",
        write_table=f"{schema}.events",
        size_tables=["events"],
    )


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


def analyze_layout(dsn: str, ctx: LayoutContext, rows: int = 0, verbose: bool = False) -> float:
    # Set n_distinct hints for columns with known cardinalities.
    # Columnar's ANALYZE produces degenerate n_distinct on PG17+ due to
    # block-based sampling not scaling totalrows correctly.
    n_distinct_hints = {
        "service": 6,
        "kind": 7,
        "device_type": 4,
        "region_id": 128,
        "tenant_id": 64,
        "severity": 5,
        "status": 9,
        "event_date": max(1, math.ceil(rows / 86400) + 1),
        "work_id": max(1, math.ceil(rows / 500)),
    }
    hint_sqls = [
        f"ALTER TABLE {ctx.logical_table} ALTER COLUMN {col} SET (n_distinct = {nd});"
        for col, nd in n_distinct_hints.items()
    ]
    try:
        exec_sql(dsn, "\n".join(hint_sqls), verbose=verbose)
    except RuntimeError:
        pass  # columns may not exist on external tables with different schemas

    start = time.perf_counter()
    exec_sql(dsn, f"ANALYZE {ctx.logical_table};", verbose=verbose)
    return time.perf_counter() - start


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
            load_rows(dsn, ctx.write_table, next_start, next_end, work_rows, verbose=verbose)
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


def cleanup_targets(targets: list[BenchmarkTarget], verbose: bool = False) -> None:
    for target in targets:
        exec_sql(
            target.dsn,
            f"DROP SCHEMA IF EXISTS {layout_schema(target.label)} CASCADE;",
            verbose=verbose,
        )


def print_summary(report: dict[str, Any]) -> None:
    print("\nStorage layout benchmark summary")
    print("=" * 34)
    print(f"Rows: {report['config']['rows']}")
    print(f"Query runs: {report['config']['query_runs']}")
    print("Layouts: " + ", ".join(report["config"]["layouts"]))

    for layout_name, result in report["layouts"].items():
        print(f"\n[{layout_name}]")
        append_total = result["append"].get("total_seconds", 0.0) or 0.0
        print(
            f"  load={result['load_seconds']:.3f}s "
            f"analyze={result['analyze_seconds']:.3f}s "
            f"append_total={append_total:.3f}s "
            f"update={result['update']['seconds']:.3f}s"
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

    print("\nLayout metrics")
    print("=" * 14)
    metric_rows = [
        ("total_size_mb", lambda r: bytes_to_mb(sum(r["sizes"].values()))),
        ("load_seconds", lambda r: r["load_seconds"]),
        ("append_total_seconds", lambda r: r["append"].get("total_seconds", 0.0) or 0.0),
        ("update_seconds", lambda r: r["update"]["seconds"]),
    ]
    render_matrix(
        ["metric"] + layouts,
        [
            [name] + [f"{fn(report['layouts'][l]):.3f}" for l in layouts]
            for name, fn in metric_rows
        ],
    )

    common_queries = sorted(
        set.intersection(
            *(set(report["layouts"][l]["queries"].keys()) for l in layouts)
        )
    )

    if not common_queries:
        return

    print("\nPer-query medians (ms)")
    print("=" * 22)
    render_matrix(
        ["query"] + layouts,
        [
            [qn] + [f"{report['layouts'][l]['queries'][qn]['median_ms']:.3f}" for l in layouts]
            for qn in common_queries
        ],
    )


def bytes_to_mb(value: int) -> float:
    return value / 1024 / 1024


def render_matrix(header: list[str], rows: list[list[str]]) -> None:
    widths = [max(24, len(header[0]))]
    widths.extend(max(18, len(name)) for name in header[1:])

    def render_row(values: list[str]) -> str:
        return "  ".join(value.ljust(width) for value, width in zip(values, widths))

    print(render_row(header))
    print(render_row(["-" * len(item) for item in header]))
    for row in rows:
        print(render_row(row))


def main() -> int:
    args = parse_args()
    dsn = args.dsn or default_dsn()
    hydra_layouts = [item.strip() for item in args.layouts.split(",") if item.strip()]

    targets = [
        BenchmarkTarget(label=layout, layout=layout, dsn=dsn)
        for layout in hydra_layouts
    ]
    for label, compare_dsn in args._compare_targets:
        targets.append(
            BenchmarkTarget(label=label, layout="external_postgres", dsn=compare_dsn)
        )

    report: dict[str, Any] = {
        "config": {
            "hydra_dsn": "<redacted>",
            "rows": args.rows,
            "work_rows": args.work_rows,
            "layouts": [t.label for t in targets],
            "hydra_layouts": hydra_layouts,
            "compare_targets": [label for label, _ in args._compare_targets],
            "query_runs": args.query_runs,
            "append_batches": args.append_batches,
            "append_rows": args.append_rows,
            "query_file": str(QUERY_FILE),
        },
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "layouts": {},
    }

    try:
        for target in targets:
            print(f"\n--- Benchmarking: {target.label} ({target.layout}) ---",
                  file=sys.stderr)

            ctx = create_layout_schema(
                target.dsn,
                label=target.label,
                layout=target.layout,
                verbose=args.verbose,
            )

            load_seconds = load_rows(
                target.dsn,
                ctx.write_table,
                1,
                args.rows,
                args.work_rows,
                verbose=args.verbose,
            )

            analyze_seconds = analyze_layout(target.dsn, ctx, rows=args.rows, verbose=args.verbose)
            sizes = relation_sizes(target.dsn, ctx)
            query_results = run_query_suite(
                target.dsn, ctx, args.rows, args.work_rows, args.query_runs,
            )
            append_result = append_batches(
                target.dsn, ctx, args.rows, args.work_rows,
                args.append_rows, args.append_batches, verbose=args.verbose,
            )
            try:
                update_result = update_hot_slice(
                    target.dsn, ctx,
                    args.rows + (args.append_rows * args.append_batches),
                    args.work_rows, verbose=args.verbose,
                )
            except RuntimeError as e:
                print(f"  UPDATE failed for {target.label}: {e}",
                      file=sys.stderr)
                update_result = {
                    "seconds": -1,
                    "rows_updated": 0,
                    "error": str(e)[:200],
                }
            try:
                logical_rows = query_scalar_int(
                    target.dsn, f"SELECT COUNT(*) FROM {ctx.logical_table};",
                )
            except RuntimeError:
                logical_rows = -1

            report["layouts"][target.label] = {
                "layout": target.layout,
                "schema": ctx.schema,
                "logical_table": ctx.logical_table,
                "write_table": ctx.write_table,
                "load_seconds": round(load_seconds, 4),
                "analyze_seconds": round(analyze_seconds, 4),
                "sizes": sizes,
                "logical_rows_after_benchmark": logical_rows,
                "queries": query_results,
                "append": append_result,
                "update": update_result,
            }

    finally:
        if args.cleanup:
            cleanup_targets(targets, verbose=args.verbose)

    print_summary(report)

    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(f"\nWrote JSON report to {output_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
