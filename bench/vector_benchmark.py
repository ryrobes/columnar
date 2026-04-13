#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import math
import statistics
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_VARIANTS = "exact,hnsw,diskann,diskann_labels"
QUERY_NAMES = [
    "unfiltered_topk",
    "category_filter",
    "tenant_filter",
    "label_category_filter",
    "label_tenant_filter",
]


@dataclass
class Target:
    label: str
    dsn: str


@dataclass(frozen=True)
class AnnTuning:
    maintenance_work_mem: str
    hnsw_ef_search: int
    hnsw_max_scan_tuples: int
    diskann_num_neighbors: int
    diskann_build_search_list_size: int
    diskann_query_search_list_size: int
    diskann_query_rescore: int
    candidate_multiplier: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark pgvector and pgvectorscale ANN paths with filtered embedding search."
    )
    parser.add_argument(
        "--target",
        action="append",
        default=[],
        metavar="LABEL=DSN",
        help="Benchmark target. Repeat for multiple databases.",
    )
    parser.add_argument("--rows", type=int, default=50_000)
    parser.add_argument("--dims", type=int, default=64)
    parser.add_argument("--queries", type=int, default=8)
    parser.add_argument("--query-runs", type=int, default=3)
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument("--variants", default=DEFAULT_VARIANTS)
    parser.add_argument(
        "--maintenance-work-mem",
        default="32MB",
        help="maintenance_work_mem used while building ANN indexes.",
    )
    parser.add_argument("--hnsw-ef-search", type=int, default=200)
    parser.add_argument("--hnsw-max-scan-tuples", type=int, default=200_000)
    parser.add_argument("--diskann-num-neighbors", type=int, default=20)
    parser.add_argument("--diskann-build-search-list-size", type=int, default=40)
    parser.add_argument("--diskann-query-search-list-size", type=int, default=100)
    parser.add_argument("--diskann-query-rescore", type=int, default=100)
    parser.add_argument(
        "--candidate-multiplier",
        type=int,
        default=1,
        help="For ANN variants, fetch top_k * N candidates and rerank them by exact distance.",
    )
    parser.add_argument("--output")
    parser.add_argument("--cleanup", action="store_true")
    parser.add_argument("--verbose", action="store_true")

    args = parser.parse_args()
    if not args.target:
        parser.error("at least one --target LABEL=DSN is required")
    if args.rows <= 0:
        parser.error("--rows must be positive")
    if args.dims <= 0:
        parser.error("--dims must be positive")
    if args.queries <= 0:
        parser.error("--queries must be positive")
    if args.query_runs <= 0:
        parser.error("--query-runs must be positive")
    if args.top_k <= 0:
        parser.error("--top-k must be positive")
    if args.hnsw_ef_search <= 0:
        parser.error("--hnsw-ef-search must be positive")
    if args.hnsw_max_scan_tuples <= 0:
        parser.error("--hnsw-max-scan-tuples must be positive")
    if args.diskann_num_neighbors <= 0:
        parser.error("--diskann-num-neighbors must be positive")
    if args.diskann_build_search_list_size <= 0:
        parser.error("--diskann-build-search-list-size must be positive")
    if args.diskann_query_search_list_size <= 0:
        parser.error("--diskann-query-search-list-size must be positive")
    if args.diskann_query_rescore < 0:
        parser.error("--diskann-query-rescore must be non-negative")
    if args.candidate_multiplier <= 0:
        parser.error("--candidate-multiplier must be positive")

    targets: list[Target] = []
    for item in args.target:
        if "=" not in item:
            parser.error(f"--target must be LABEL=DSN, got {item!r}")
        label, dsn = item.split("=", 1)
        label = label.strip()
        if not label or not label.replace("_", "").isalnum():
            parser.error(f"target label must be alphanumeric/underscore: {label!r}")
        targets.append(Target(label, dsn.strip()))
    args._targets = targets
    args._variants = [v.strip() for v in args.variants.split(",") if v.strip()]
    return args


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
            f"SQL:\n{sql[:1000]}\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )
    return result.stdout.strip()


def exec_sql(dsn: str, sql: str, verbose: bool = False) -> None:
    run_psql(dsn, sql, verbose=verbose)


def scalar_json(dsn: str, sql: str) -> Any:
    output = run_psql(dsn, sql)
    return json.loads(output)


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def extension_versions(dsn: str) -> dict[str, str | None]:
    sql = """
    SELECT coalesce(json_object_agg(name, installed_version), '{}'::json)
    FROM pg_available_extensions
    WHERE name IN ('vector', 'vectorscale');
    """
    values = scalar_json(dsn, sql)
    return {str(k): v for k, v in values.items()}


def create_extensions(dsn: str, verbose: bool = False) -> dict[str, str | None]:
    exec_sql(dsn, "CREATE EXTENSION IF NOT EXISTS vector;", verbose=verbose)
    available = extension_versions(dsn)
    if "vectorscale" in available:
        exec_sql(dsn, "CREATE EXTENSION IF NOT EXISTS vectorscale CASCADE;", verbose=verbose)
        available = extension_versions(dsn)
    return available


def embedding_values(row_id: int, dims: int) -> list[float]:
    values = [
        math.sin(row_id * 0.017 + dim * 0.131) +
        0.5 * math.cos((row_id % 97) * 0.023 + dim * 0.071)
        for dim in range(dims)
    ]
    norm = math.sqrt(sum(value * value for value in values)) or 1.0
    return [value / norm for value in values]


def embedding_literal(row_id: int, dims: int) -> str:
    return "[" + ",".join(f"{value:.6f}" for value in embedding_values(row_id, dims)) + "]"


def labels_for(row_id: int) -> list[int]:
    tenant_id = ((row_id - 1) % 128) + 1
    category_id = ((row_id - 1) % 32) + 1
    model_id = ((row_id - 1) % 4) + 1
    shard_id = ((row_id - 1) % 16) + 1
    return [tenant_id, 1000 + category_id, 2000 + model_id, 3000 + shard_id]


def label_literal(labels: list[int]) -> str:
    return "{" + ",".join(str(label) for label in labels) + "}"


def copy_rows(dsn: str, schema: str, rows: int, dims: int) -> float:
    copy_sql = (
        f"COPY {schema}.items "
        "(id, tenant_id, category_id, model_id, active, labels, embedding, content) "
        "FROM STDIN WITH (FORMAT text)"
    )
    start = time.perf_counter()
    proc = subprocess.Popen(
        ["psql", dsn, "-X", "-v", "ON_ERROR_STOP=1", "-q", "-c", copy_sql],
        stdin=subprocess.PIPE,
        text=True,
    )
    assert proc.stdin is not None
    for row_id in range(1, rows + 1):
        tenant_id = ((row_id - 1) % 128) + 1
        category_id = ((row_id - 1) % 32) + 1
        model_id = ((row_id - 1) % 4) + 1
        active = "t" if row_id % 17 != 0 else "f"
        labels = label_literal(labels_for(row_id))
        embedding = embedding_literal(row_id, dims)
        content = f"document_{row_id % 10000}"
        proc.stdin.write(
            f"{row_id}\t{tenant_id}\t{category_id}\t{model_id}\t{active}\t"
            f"{labels}\t{embedding}\t{content}\n"
        )
    proc.stdin.close()
    returncode = proc.wait()
    if returncode != 0:
        raise RuntimeError(f"COPY failed with exit code {returncode}")
    return time.perf_counter() - start


def setup_table(dsn: str, schema: str, rows: int, dims: int, verbose: bool = False) -> dict[str, Any]:
    exec_sql(
        dsn,
        f"""
        DROP SCHEMA IF EXISTS {schema} CASCADE;
        CREATE SCHEMA {schema};
        CREATE TABLE {schema}.items (
            id bigint PRIMARY KEY,
            tenant_id integer NOT NULL,
            category_id integer NOT NULL,
            model_id integer NOT NULL,
            active boolean NOT NULL,
            labels smallint[] NOT NULL,
            embedding vector({dims}) NOT NULL,
            content text NOT NULL
        ) USING heap;
        """,
        verbose=verbose,
    )
    load_seconds = copy_rows(dsn, schema, rows, dims)
    analyze_start = time.perf_counter()
    exec_sql(dsn, f"ANALYZE {schema}.items;", verbose=verbose)
    analyze_seconds = time.perf_counter() - analyze_start
    return {
        "load_seconds": round(load_seconds, 4),
        "analyze_seconds": round(analyze_seconds, 4),
    }


def query_specs(dims: int, count: int, top_k: int) -> list[dict[str, str]]:
    specs: list[dict[str, str]] = []
    for query_index in range(count):
        seed = 10_000_000 + (query_index + 1) * 9_973
        query_embedding = embedding_literal(seed, dims)
        tenant_id = ((seed - 1) % 128) + 1
        category_id = ((seed - 1) % 32) + 1
        specs.append({
            "name": "unfiltered_topk",
            "where": "",
            "label": "",
            "query_embedding": query_embedding,
            "top_k": str(top_k),
        })
        specs.append({
            "name": "category_filter",
            "where": f"WHERE category_id = {category_id}",
            "label": "",
            "query_embedding": query_embedding,
            "top_k": str(top_k),
        })
        specs.append({
            "name": "tenant_filter",
            "where": f"WHERE tenant_id = {tenant_id}",
            "label": "",
            "query_embedding": query_embedding,
            "top_k": str(top_k),
        })
        specs.append({
            "name": "label_category_filter",
            "where": f"WHERE labels && ARRAY[{1000 + category_id}]::smallint[]",
            "label": str(1000 + category_id),
            "query_embedding": query_embedding,
            "top_k": str(top_k),
        })
        specs.append({
            "name": "label_tenant_filter",
            "where": f"WHERE labels && ARRAY[{tenant_id}]::smallint[]",
            "label": str(tenant_id),
            "query_embedding": query_embedding,
            "top_k": str(top_k),
        })
    return specs


def exact_query(schema: str, spec: dict[str, str]) -> str:
    return f"""
    SELECT id
    FROM {schema}.items
    {spec['where']}
    ORDER BY embedding <=> {sql_literal(spec['query_embedding'])}::vector
    LIMIT {spec['top_k']}
    """


def variant_query(schema: str, spec: dict[str, str], variant: str, tuning: AnnTuning) -> str:
    if variant == "exact" or tuning.candidate_multiplier == 1:
        return exact_query(schema, spec)

    top_k = int(spec["top_k"])
    candidate_limit = top_k * tuning.candidate_multiplier
    distance_expr = f"embedding <=> {sql_literal(spec['query_embedding'])}::vector"
    return f"""
    WITH relaxed_results AS MATERIALIZED (
        SELECT id, {distance_expr} AS distance
        FROM {schema}.items
        {spec['where']}
        ORDER BY distance
        LIMIT {candidate_limit}
    )
    SELECT id
    FROM relaxed_results
    ORDER BY distance
    LIMIT {top_k}
    """


def query_ids(dsn: str, sql: str, setup_sql: str = "") -> list[int]:
    wrapped = f"""
    SET client_min_messages TO warning;
    SET jit = off;
    {setup_sql}
    SELECT coalesce(json_agg(id), '[]'::json)
    FROM (
        {sql}
    ) q;
    """
    ids = scalar_json(dsn, wrapped)
    return [int(value) for value in ids]


def explain_timing_ms(dsn: str, sql: str, setup_sql: str = "") -> float:
    wrapped = f"""
    SET client_min_messages TO warning;
    SET jit = off;
    {setup_sql}
    EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
    {sql}
    """
    plan = scalar_json(dsn, wrapped)
    return float(plan[0]["Execution Time"])


def recall_at_k(found: list[int], truth: list[int], k: int) -> float:
    if not truth:
        return 1.0 if not found else 0.0
    truth_set = set(truth[:k])
    return len([item for item in found[:k] if item in truth_set]) / len(truth_set)


def create_index(
    dsn: str,
    schema: str,
    variant: str,
    tuning: AnnTuning,
    verbose: bool = False,
) -> dict[str, Any]:
    exec_sql(
        dsn,
        f"""
        DROP INDEX IF EXISTS {schema}.items_embedding_hnsw;
        DROP INDEX IF EXISTS {schema}.items_embedding_diskann;
        DROP INDEX IF EXISTS {schema}.items_embedding_diskann_labels;
        """,
        verbose=verbose,
    )
    if variant == "exact":
        return {"index_seconds": 0.0}

    if variant == "hnsw":
        sql = f"""
        SET maintenance_work_mem = {sql_literal(tuning.maintenance_work_mem)};
        CREATE INDEX items_embedding_hnsw
        ON {schema}.items
        USING hnsw (embedding vector_cosine_ops)
        WITH (m = 16, ef_construction = 64);
        """
    elif variant == "diskann":
        sql = f"""
        CREATE INDEX items_embedding_diskann
        ON {schema}.items
        USING diskann (embedding vector_cosine_ops)
        WITH (
            num_neighbors = {tuning.diskann_num_neighbors},
            search_list_size = {tuning.diskann_build_search_list_size}
        );
        """
    elif variant == "diskann_labels":
        sql = f"""
        CREATE INDEX items_embedding_diskann_labels
        ON {schema}.items
        USING diskann (embedding vector_cosine_ops, labels)
        WITH (
            num_neighbors = {tuning.diskann_num_neighbors},
            search_list_size = {tuning.diskann_build_search_list_size}
        );
        """
    else:
        raise ValueError(f"unknown variant: {variant}")

    start = time.perf_counter()
    exec_sql(dsn, sql, verbose=verbose)
    index_seconds = time.perf_counter() - start
    exec_sql(dsn, f"ANALYZE {schema}.items;", verbose=verbose)
    return {"index_seconds": round(index_seconds, 4)}


def variant_setup_sql(variant: str, tuning: AnnTuning) -> str:
    if variant == "hnsw":
        return f"""
        SET hnsw.ef_search = {tuning.hnsw_ef_search};
        SET hnsw.iterative_scan = relaxed_order;
        SET hnsw.max_scan_tuples = {tuning.hnsw_max_scan_tuples};
        """
    if variant.startswith("diskann"):
        return f"""
        SET diskann.query_search_list_size = {tuning.diskann_query_search_list_size};
        SET diskann.query_rescore = {tuning.diskann_query_rescore};
        """
    if variant == "exact":
        return """
        SET enable_indexscan = off;
        SET enable_bitmapscan = off;
        """
    return ""


def benchmark_variant(
    dsn: str,
    schema: str,
    variant: str,
    specs: list[dict[str, str]],
    query_runs: int,
    top_k: int,
    tuning: AnnTuning,
) -> dict[str, Any]:
    setup_sql = variant_setup_sql(variant, tuning)
    per_query: dict[str, dict[str, Any]] = {
        name: {"runs_ms": [], "recall": []} for name in QUERY_NAMES
    }

    # Ground truth is exact search with indexes disabled.
    truth_cache: dict[int, list[int]] = {}
    for spec_index, spec in enumerate(specs):
        truth_ids = query_ids(dsn, exact_query(schema, spec), variant_setup_sql("exact", tuning))
        truth_cache[spec_index] = truth_ids

    for spec_index, spec in enumerate(specs):
        name = spec["name"]
        sql = variant_query(schema, spec, variant, tuning)
        ids = query_ids(dsn, sql, setup_sql)
        recall = round(recall_at_k(ids, truth_cache[spec_index], top_k), 4)
        for _ in range(query_runs):
            elapsed_ms = explain_timing_ms(dsn, sql, setup_sql)
            per_query[name]["runs_ms"].append(round(elapsed_ms, 3))
            per_query[name]["recall"].append(recall)

    for result in per_query.values():
        result["median_ms"] = round(statistics.median(result["runs_ms"]), 3)
        result["min_ms"] = round(min(result["runs_ms"]), 3)
        result["max_ms"] = round(max(result["runs_ms"]), 3)
        result["median_recall"] = round(statistics.median(result["recall"]), 4)

    return per_query


def supports_variant(extensions: dict[str, str | None], variant: str) -> bool:
    if variant in {"exact", "hnsw"}:
        return "vector" in extensions
    if variant in {"diskann", "diskann_labels"}:
        return "vectorscale" in extensions
    return False


def print_summary(report: dict[str, Any]) -> None:
    print("\nVector benchmark summary")
    print("========================")
    print(f"Rows: {report['config']['rows']}, dims: {report['config']['dims']}")

    for target, target_result in report["targets"].items():
        print(f"\n[{target}] extensions={target_result['extensions']}")
        for variant, variant_result in target_result["variants"].items():
            print(
                f"  {variant}: index={variant_result['index'].get('index_seconds', 0.0):.3f}s"
            )
            for name in QUERY_NAMES:
                result = variant_result["queries"][name]
                print(
                    f"    {name}: median={result['median_ms']:.3f} ms "
                    f"recall={result['median_recall']:.3f}"
                )
        if target_result.get("skipped_variants"):
            print("  skipped: " + ", ".join(target_result["skipped_variants"]))


def main() -> int:
    args = parse_args()
    schema = "bench_vector"
    tuning = AnnTuning(
        maintenance_work_mem=args.maintenance_work_mem,
        hnsw_ef_search=args.hnsw_ef_search,
        hnsw_max_scan_tuples=args.hnsw_max_scan_tuples,
        diskann_num_neighbors=args.diskann_num_neighbors,
        diskann_build_search_list_size=args.diskann_build_search_list_size,
        diskann_query_search_list_size=args.diskann_query_search_list_size,
        diskann_query_rescore=args.diskann_query_rescore,
        candidate_multiplier=args.candidate_multiplier,
    )
    specs = query_specs(args.dims, args.queries, args.top_k)
    report: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "config": {
            "rows": args.rows,
            "dims": args.dims,
            "queries": args.queries,
            "query_runs": args.query_runs,
            "top_k": args.top_k,
            "variants": args._variants,
            "ann_tuning": tuning.__dict__,
        },
        "targets": {},
    }

    for target in args._targets:
        print(f"\n--- Vector benchmark: {target.label} ---", file=sys.stderr)
        try:
            extensions = create_extensions(target.dsn, verbose=args.verbose)
            setup = setup_table(target.dsn, schema, args.rows, args.dims, verbose=args.verbose)
            target_result: dict[str, Any] = {
                "extensions": extensions,
                "setup": setup,
                "variants": {},
                "skipped_variants": [],
            }
            for variant in args._variants:
                if not supports_variant(extensions, variant):
                    target_result["skipped_variants"].append(variant)
                    continue
                print(f"  variant {variant}", file=sys.stderr)
                index_result = create_index(
                    target.dsn,
                    schema,
                    variant,
                    tuning,
                    verbose=args.verbose,
                )
                query_result = benchmark_variant(
                    target.dsn, schema, variant, specs, args.query_runs, args.top_k, tuning
                )
                target_result["variants"][variant] = {
                    "index": index_result,
                    "queries": query_result,
                }
            report["targets"][target.label] = target_result
        finally:
            if args.cleanup:
                try:
                    exec_sql(target.dsn, f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
                except Exception as exc:
                    print(f"cleanup failed for {target.label}: {exc}", file=sys.stderr)

    print_summary(report)
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(f"\nWrote JSON report to {output_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
