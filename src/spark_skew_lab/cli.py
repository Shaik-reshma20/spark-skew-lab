"""Command-line entry points."""
from __future__ import annotations

import argparse
import json

from pyspark.sql import SparkSession

from .benchmark import run_benchmark
from .generator import generate_jsonl
from .pipeline import run_pipeline


def main() -> None:
    parser = argparse.ArgumentParser(prog="spark-skew-lab")
    sub = parser.add_subparsers(dest="command", required=True)

    gen = sub.add_parser("generate", help="Generate deterministic synthetic JSONL events")
    gen.add_argument("--rows", type=int, default=10_000)
    gen.add_argument("--seed", type=int, default=42)
    gen.add_argument("--hot-share", type=float, default=0.70)
    gen.add_argument("--output", default="data/generated/events.jsonl")

    run = sub.add_parser("run", help="Run validation, quarantine, aggregation and reconciliation")
    run.add_argument("--input", required=True)
    run.add_argument("--output-dir", default="output")

    bench = sub.add_parser("benchmark", help="Benchmark baseline and salted aggregation")
    bench.add_argument("--rows", type=int, default=100_000)
    bench.add_argument("--seed", type=int, default=42)
    bench.add_argument("--hot-share", type=float, default=0.70)
    bench.add_argument("--salt-buckets", type=int, default=16)
    bench.add_argument("--repetitions", type=int, default=3)
    bench.add_argument("--output", default="reports/benchmark.json")

    args = parser.parse_args()
    if args.command == "generate":
        count = generate_jsonl(args.output, args.rows, args.seed, args.hot_share)
        print(json.dumps({"output": args.output, "rows_written": count}, indent=2))
    elif args.command == "run":
        spark = (
            SparkSession.builder.appName("spark-skew-lab-pipeline")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "8")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        try:
            result = run_pipeline(args.input, args.output_dir, spark)
            print(json.dumps(result, indent=2))
        finally:
            spark.stop()
    elif args.command == "benchmark":
        result = run_benchmark(
            rows=args.rows,
            seed=args.seed,
            hot_share=args.hot_share,
            salt_buckets=args.salt_buckets,
            repetitions=args.repetitions,
            output_path=args.output,
        )
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
