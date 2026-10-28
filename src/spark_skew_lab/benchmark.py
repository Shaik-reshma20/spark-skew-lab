"""Repeatable baseline-versus-salted benchmark."""
from __future__ import annotations

import json
import platform
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path

import pyspark
from pyspark.sql import SparkSession

from .generator import make_events
from .pipeline import EVENT_SCHEMA, baseline_aggregate, salted_aggregate


def run_benchmark(
    rows: int = 100_000,
    seed: int = 42,
    hot_share: float = 0.70,
    salt_buckets: int = 16,
    repetitions: int = 3,
    output_path: str = "reports/benchmark.json",
) -> dict:
    if repetitions < 1:
        raise ValueError("repetitions must be >= 1")
    if rows < 0:
        raise ValueError("rows must be >= 0")

    spark = (
        SparkSession.builder
        .appName("spark-skew-lab-benchmark")
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        records = make_events(rows=rows, seed=seed, hot_share=hot_share)
        df = spark.createDataFrame(records, schema=EVENT_SCHEMA).cache()
        input_count = df.count()  # materialize once, outside measured time

        def measure(label, fn):
            samples = []
            output_count = None
            for _ in range(repetitions + 1):  # first is warm-up
                start = time.perf_counter()
                result = fn(df).cache()
                output_count = result.count()
                elapsed = time.perf_counter() - start
                result.unpersist()
                if len(samples) < repetitions:
                    samples.append(elapsed)
            return {
                "samples_seconds": [round(x, 4) for x in samples],
                "median_seconds": round(statistics.median(samples), 4),
                "output_groups": output_count,
            }

        baseline = measure("baseline", baseline_aggregate)
        salted = measure("salted", lambda data: salted_aggregate(data, salt_buckets))
        report = {
            "created_at": datetime.now(timezone.utc).isoformat(),
            "status": "COMPLETED",
            "methodology": {
                "input_rows": input_count,
                "seed": seed,
                "hot_share": hot_share,
                "salt_buckets": salt_buckets,
                "repetitions_after_warmup": repetitions,
                "warmup_runs_per_method": 1,
                "aqe_enabled": False,
                "shuffle_partitions": 8,
                "note": "Times are machine-specific; aggregation output is materialized with count().",
            },
            "environment": {
                "python": platform.python_version(),
                "platform": platform.platform(),
                "pyspark": pyspark.__version__,
                "spark_version": spark.version,
                "master": "local[*]",
            },
            "baseline": baseline,
            "salted": salted,
            "speedup_baseline_over_salted": (
                round(baseline["median_seconds"] / salted["median_seconds"], 4)
                if salted["median_seconds"] > 0 else None
            ),
            "correctness": {
                "output_group_counts_match": baseline["output_groups"] == salted["output_groups"],
                "note": "The end-to-end pipeline also performs per-customer count and amount reconciliation.",
            },
        }
        out = Path(output_path)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(report, indent=2), encoding="utf-8")
        return report
    finally:
        spark.stop()
