"""Spark ingestion, validation, aggregation, and run audit."""
from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, StringType, StructField, StructType,
)


EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("event_ts", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("source_file", StringType(), True),
    StructField("run_id", StringType(), True),
])

CUSTOMER_IDS = [f"customer_{i:05d}" for i in range(1, 101)] + ["customer_hot_001"]


def baseline_aggregate(df: DataFrame) -> DataFrame:
    return df.groupBy("customer_id").agg(
        F.count("*").cast("long").alias("event_count"),
        F.sum("amount").alias("total_amount"),
    )


def salted_aggregate(df: DataFrame, salt_buckets: int = 16) -> DataFrame:
    if salt_buckets < 2:
        raise ValueError("salt_buckets must be >= 2")
    salted = df.withColumn(
        "_salt", F.pmod(F.xxhash64("event_id"), F.lit(salt_buckets))
    )
    partial = salted.groupBy("customer_id", "_salt").agg(
        F.count("*").cast("long").alias("event_count"),
        F.sum("amount").alias("total_amount"),
    )
    return partial.groupBy("customer_id").agg(
        F.sum("event_count").cast("long").alias("event_count"),
        F.sum("total_amount").alias("total_amount"),
    )


def assert_no_unexpected_json_fields(input_path: str) -> None:
    """Reject unknown JSON keys while allowing malformed lines to reach quarantine.

    This lightweight line scan keeps the input contract strict without loading the whole
    source file into memory. Missing keys are handled by row-level validation.
    """
    allowed = set(EVENT_SCHEMA.fieldNames())
    with open(input_path, "r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue  # Spark will parse this row as null fields in permissive mode.
            if isinstance(record, dict):
                unexpected = set(record) - allowed
                if unexpected:
                    raise ValueError(
                        f"Unexpected schema field(s) on line {line_number}: "
                        + ", ".join(sorted(unexpected))
                    )


def read_events(spark: SparkSession, input_path: str) -> DataFrame:
    """Read JSON Lines with an explicit schema and strict unknown-field check."""
    assert_no_unexpected_json_fields(input_path)
    raw = spark.read.schema(EVENT_SCHEMA).option("mode", "PERMISSIVE").json(input_path)
    return raw.withColumn("_input_order", F.monotonically_increasing_id())


def validate_events(df: DataFrame, run_id: str) -> tuple[DataFrame, DataFrame]:
    """Return accepted events and rejected records with stable reasons.

    The first encountered duplicate event_id is retained. Input order is represented by
    monotonically_increasing_id after reading the single input file. For multi-file
    deterministic winner selection, use an explicit source offset or ingestion sequence.
    """
    prepared = (
        df.withColumn("event_ts_parsed", F.to_timestamp("event_ts"))
        .withColumn("amount_num", F.col("amount").cast("double"))
        .withColumn("processing_run_id", F.lit(run_id))
    )
    # Build a single reason string so every rejected row is traceable.
    prepared = prepared.withColumn(
        "reject_reason",
        F.when(F.col("event_id").isNull() | (F.trim("event_id") == ""), "missing_event_id")
        .when(F.col("customer_id").isNull() | (F.trim("customer_id") == ""), "missing_customer_id")
        .when(F.col("event_ts").isNull() | F.col("event_ts_parsed").isNull(), "malformed_event_ts")
        .when(F.col("amount_num").isNull() | F.isnan("amount_num"), "missing_or_malformed_amount")
        .when(F.col("amount_num") < 0, "negative_amount")
        .when(~F.col("customer_id").isin(CUSTOMER_IDS), "unknown_customer_id")
    )

    # Duplicate selection is stable for one JSONL input file using input-order IDs.
    from pyspark.sql.window import Window
    
    if "_input_order" not in prepared.columns:
        prepared = prepared.withColumn(
            "_input_order", F.monotonically_increasing_id()
        )

    window = Window.partitionBy("event_id").orderBy(F.col("_input_order").asc())

    prepared = prepared.withColumn(
        "_duplicate_rank",
        F.when(F.col("event_id").isNotNull(), F.row_number().over(window))
    )
    prepared = prepared.withColumn(
        "reject_reason",
        F.when(
            F.col("reject_reason").isNull() &
            (F.col("_duplicate_rank") > 1),
            "duplicate_event_id",
        ).otherwise(F.col("reject_reason"))
    )

    quarantine = prepared.filter(F.col("reject_reason").isNotNull()).select(
        "event_id", "customer_id", "event_ts", "amount", "source_file",
        F.col("processing_run_id").alias("run_id"), "reject_reason"
    )
    accepted = prepared.filter(F.col("reject_reason").isNull()).select(
        "event_id", "customer_id",
        F.col("event_ts_parsed").alias("event_ts"),
        F.col("amount_num").alias("amount"),
        "source_file",
        F.col("processing_run_id").alias("run_id"),
    )
    return accepted, quarantine


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def run_pipeline(input_path: str, output_dir: str, spark: SparkSession) -> dict:
    run_id = str(uuid.uuid4())
    started = datetime.now(timezone.utc)
    start_clock = time.perf_counter()
    root = Path(output_dir) / f"run_id={run_id}"
    root.mkdir(parents=True, exist_ok=False)
    metrics_path = root / "run_metrics.json"
    metrics = {
        "run_id": run_id,
        "started_at": started.isoformat(),
        "status": "RUNNING",
        "input_path": str(input_path),
    }
    _write_json(metrics_path, metrics)

    try:
        raw = read_events(spark, input_path).cache()
        input_count = raw.count()
        accepted, quarantine = validate_events(raw, run_id)
        accepted = accepted.cache()
        quarantine = quarantine.cache()
        accepted_count = accepted.count()
        rejected_count = quarantine.count()

        baseline = baseline_aggregate(accepted).cache()
        salted = salted_aggregate(accepted).cache()
        baseline_count = baseline.count()
        salted_count = salted.count()

        # Reconcile row counts and per-key aggregates.
        mismatch = (
            baseline.alias("b").join(salted.alias("s"), "customer_id", "full")
            .filter(
                F.coalesce(F.col("b.event_count"), F.lit(-1)) !=
                F.coalesce(F.col("s.event_count"), F.lit(-1))
            )
            .count()
        )
        amount_mismatch = (
            baseline.alias("b").join(salted.alias("s"), "customer_id", "full")
            .filter(
                F.abs(F.coalesce(F.col("b.total_amount"), F.lit(0.0)) -
                      F.coalesce(F.col("s.total_amount"), F.lit(0.0))) > 1e-7
            )
            .count()
        )
        if input_count != accepted_count + rejected_count:
            raise AssertionError("Input count does not reconcile to accepted + rejected")
        if mismatch or amount_mismatch or baseline_count != salted_count:
            raise AssertionError(
                f"Aggregation mismatch: count={mismatch}, amount={amount_mismatch}"
            )

        accepted.write.mode("overwrite").parquet(str(root / "accepted_events"))
        quarantine.write.mode("overwrite").parquet(str(root / "quarantine"))
        baseline.write.mode("overwrite").parquet(str(root / "baseline_aggregates"))
        salted.write.mode("overwrite").parquet(str(root / "salted_aggregates"))

        ended = datetime.now(timezone.utc)
        metrics.update({
            "status": "SUCCESS",
            "ended_at": ended.isoformat(),
            "duration_seconds": round(time.perf_counter() - start_clock, 4),
            "input_count": input_count,
            "accepted_count": accepted_count,
            "rejected_count": rejected_count,
            "baseline_output_count": baseline_count,
            "salted_output_count": salted_count,
            "count_reconciliation_passed": input_count == accepted_count + rejected_count,
            "aggregate_reconciliation_passed": True,
            "reject_reason_counts": {
                row["reject_reason"]: row["count"]
                for row in quarantine.groupBy("reject_reason").count().collect()
            },
        })
        _write_json(metrics_path, metrics)
        return metrics
    except Exception as exc:
        metrics.update({
            "status": "FAILED",
            "ended_at": datetime.now(timezone.utc).isoformat(),
            "duration_seconds": round(time.perf_counter() - start_clock, 4),
            "failure_reason": f"{type(exc).__name__}: {exc}",
        })
        _write_json(metrics_path, metrics)
        raise
