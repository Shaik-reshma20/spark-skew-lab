
"""Spark ingestion, validation, aggregation, publishing, and run audit."""

from __future__ import annotations

import hashlib
import json
import math
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)


EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("event_ts", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("source_file", StringType(), True),
    StructField("run_id", StringType(), True),
    StructField("_corrupt_record", StringType(), True),
])

CUSTOMER_IDS = [
    f"customer_{i:05d}" for i in range(1, 101)
] + ["customer_hot_001"]


def calculate_input_fingerprint(input_path: str) -> str:
    sha256 = hashlib.sha256()

    with open(input_path, "rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            sha256.update(chunk)

    return sha256.hexdigest()


def utc_now() -> str:
    """Return an ISO-8601 UTC timestamp."""
    return datetime.now(timezone.utc).isoformat()


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    """Write a JSON audit file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )


def baseline_aggregate(df: DataFrame) -> DataFrame:
    """Aggregate events by customer without salting."""
    return df.groupBy("customer_id").agg(
        F.count("*").cast("long").alias("event_count"),
        F.sum("amount").alias("total_amount"),
    )


def salted_aggregate(
    df: DataFrame,
    salt_buckets: int = 16,
) -> DataFrame:
    """Aggregate in two stages using deterministic event-ID salting."""
    if salt_buckets < 2:
        raise ValueError("salt_buckets must be >= 2")

    salted = df.withColumn(
        "_salt",
        F.pmod(
            F.xxhash64("event_id"),
            F.lit(salt_buckets),
        ),
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
    """Reject JSON objects containing unknown fields."""
    allowed = set(EVENT_SCHEMA.fieldNames()) - {"_corrupt_record"}

    with open(input_path, "r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                # Malformed lines are handled by Spark's corrupt-record column.
                continue

            if not isinstance(record, dict):
                # Non-object JSON values will be handled during validation.
                continue

            unexpected = set(record) - allowed

            if unexpected:
                raise ValueError(
                    f"Unexpected schema field(s) on line {line_number}: "
                    + ", ".join(sorted(unexpected))
                )


def read_events(
    spark: SparkSession,
    input_path: str,
) -> DataFrame:
    """Read JSON Lines using an explicit schema."""
    assert_no_unexpected_json_fields(input_path)

    raw = (
        spark.read
        .schema(EVENT_SCHEMA)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(input_path)
    )

    return raw.withColumn(
        "_input_order",
        F.monotonically_increasing_id(),
    )



def validate_events(
    df: DataFrame,
    run_id: str,
) -> tuple[DataFrame, DataFrame]:
    """
    Return accepted and quarantined events.

    Duplicate policy:
    The earliest encountered row for an event_id wins.
    If that row is invalid, a later duplicate is not substituted.
    Ordering is not a durable source-order guarantee.
    """
    if "_input_order" not in df.columns:
        df = df.withColumn(
            "_input_order",
            F.monotonically_increasing_id(),
        )

    prepared = (
        df
        .withColumn(
            "event_ts_parsed",
            F.to_timestamp("event_ts"),
        )
        .withColumn(
            "amount_num",
            F.col("amount").cast("double"),
        )
        .withColumn(
            "processing_run_id",
            F.lit(run_id),
        )
    )

    # Validate required fields and domain rules.
    prepared = prepared.withColumn(
        "reject_reason",
        F.when(
            F.col("_corrupt_record").isNotNull(),
            "malformed_json",
        )
        .when(
            F.col("event_id").isNull()
            | (F.trim("event_id") == ""),
            "missing_event_id",
        )
        .when(
            F.col("customer_id").isNull()
            | (F.trim("customer_id") == ""),
            "missing_customer_id",
        )
        .when(
            F.col("event_ts").isNull()
            | F.col("event_ts_parsed").isNull(),
            "malformed_event_ts",
        )
        .when(
            F.col("amount_num").isNull()
            | F.isnan("amount_num"),
            "missing_or_malformed_amount",
        )
        .when(
            (F.abs(F.col("amount_num")) == float("inf")),
            "non_finite_amount",
        )
        .when(
            F.col("amount_num") < 0,
            "negative_amount",
        )
        .when(
            ~F.col("customer_id").isin(CUSTOMER_IDS),
            "unknown_customer_id",
        ),
    )

    # Duplicate detection includes valid and invalid event rows.
    # First occurrence wins; the policy is intentionally explicit.
    window = Window.partitionBy("event_id").orderBy(
        F.col("_input_order").asc()
    )

    prepared = prepared.withColumn(
        "_duplicate_rank",
        F.when(
            F.col("event_id").isNotNull(),
            F.row_number().over(window),
        ),
    )

    prepared = prepared.withColumn(
        "reject_reason",
        F.when(
            F.col("reject_reason").isNull()
            & (F.col("_duplicate_rank") > 1),
            "duplicate_event_id",
        ).otherwise(F.col("reject_reason")),
    )

    quarantine = prepared.filter(
        F.col("reject_reason").isNotNull()
    ).select(
        "event_id",
        "customer_id",
        "event_ts",
        "amount",
        "source_file",
        F.col("processing_run_id").alias("run_id"),
        "reject_reason",
        F.col("_corrupt_record").alias("raw_error_record"),
    )

    accepted = prepared.filter(
        F.col("reject_reason").isNull()
    ).select(
        "event_id",
        "customer_id",
        F.col("event_ts_parsed").alias("event_ts"),
        F.col("amount_num").alias("amount"),
        "source_file",
        F.col("processing_run_id").alias("run_id"),
    )

    return accepted, quarantine


def reconcile_aggregates(
    baseline: DataFrame,
    salted: DataFrame,
    amount_tolerance: float = 1e-7,
) -> dict[str, int]:
    """Compare output group counts, event counts, and amounts."""
    b = baseline.alias("b")
    s = salted.alias("s")

    joined = b.join(s, "customer_id", "full")

    count_mismatches = joined.filter(
        F.coalesce(F.col("b.event_count"), F.lit(-1))
        != F.coalesce(F.col("s.event_count"), F.lit(-1))
    ).count()

    amount_mismatches = joined.filter(
        F.abs(
            F.coalesce(F.col("b.total_amount"), F.lit(0.0))
            - F.coalesce(F.col("s.total_amount"), F.lit(0.0))
        ) > amount_tolerance
    ).count()

    baseline_count = baseline.count()
    salted_count = salted.count()

    return {
        "count_mismatches": count_mismatches,
        "amount_mismatches": amount_mismatches,
        "baseline_output_count": baseline_count,
        "salted_output_count": salted_count,
    }


def run_pipeline(
    input_path: str,
    output_dir: str,
    spark: SparkSession,
    salt_buckets: int = 16,
    batch_id: str | None = None,
) -> dict[str, Any]:
    """
    Run the Spark pipeline and publish a unique run directory.

    Output layout:
      run_id=<uuid>/
        accepted/
        quarantine/
        baseline/
        salted/
        run_metrics.json

    This provides run isolation, not full business-level idempotency.
    """
    if salt_buckets < 2:
        raise ValueError("salt_buckets must be >= 2")

    run_id = str(uuid.uuid4())
    started = utc_now()
    start_clock = time.perf_counter()
    input_fingerprint = calculate_input_fingerprint(input_path)

    root = Path(output_dir) / f"run_id={run_id}"

    # Do not overwrite another run directory.
    root.mkdir(parents=True, exist_ok=False)

    metrics_path = root / "run_metrics.json"
    metrics: dict[str, Any] = {
        "run_id": run_id,
        "batch_id": batch_id,
        "started_at": started,
        "status": "RUNNING",
        "input_path": str(input_path),
        "input_fingerprint": input_fingerprint,
        "salt_buckets": salt_buckets,
    }

    _write_json(metrics_path, metrics)

    raw = None
    accepted = None
    quarantine = None
    baseline = None
    salted = None

    try:
        raw = read_events(spark, input_path).cache()
        input_count = raw.count()

        accepted, quarantine = validate_events(raw, run_id)

        accepted = accepted.cache()
        quarantine = quarantine.cache()

        accepted_count = accepted.count()
        rejected_count = quarantine.count()

        baseline = baseline_aggregate(accepted).cache()
        salted = salted_aggregate(
            accepted,
            salt_buckets=salt_buckets,
        ).cache()

        # Materialize aggregations.
        reconciliation = reconcile_aggregates(
            baseline,
            salted,
        )

        if input_count != accepted_count + rejected_count:
            raise AssertionError(
                "Input count does not reconcile to accepted + rejected"
            )

        if (
            reconciliation["count_mismatches"] != 0
            or reconciliation["amount_mismatches"] != 0
            or reconciliation["baseline_output_count"]
            != reconciliation["salted_output_count"]
        ):
            raise AssertionError(
                f"Aggregation mismatch: {reconciliation}"
            )

        # Publish each dataset under this unique run directory.
        # A failed write may leave partial run output; inspect before retry.
        accepted.write.mode("errorifexists").parquet(
            str(root / "accepted")
        )

        quarantine.write.mode("errorifexists").parquet(
            str(root / "quarantine")
        )

        baseline.write.mode("errorifexists").parquet(
            str(root / "baseline")
        )

        salted.write.mode("errorifexists").parquet(
            str(root / "salted")
        )

        reject_counts = {
            row["reject_reason"]: row["count"]
            for row in quarantine.groupBy(
                "reject_reason"
            ).count().collect()
        }

        metrics.update({
            "status": "SUCCESS",
            "ended_at": utc_now(),
            "duration_seconds": round(
                time.perf_counter() - start_clock,
                4,
            ),
            "input_count": input_count,
            "accepted_count": accepted_count,
            "rejected_count": rejected_count,
            "reject_reason_counts": reject_counts,
            "count_reconciliation_passed": True,
            "aggregate_reconciliation_passed": True,
            **reconciliation,
            "output_paths": {
                "accepted": str(root / "accepted"),
                "quarantine": str(root / "quarantine"),
                "baseline": str(root / "baseline"),
                "salted": str(root / "salted"),
            },
        })

        _write_json(metrics_path, metrics)
        return metrics

    except Exception as exc:
        metrics.update({
            "status": "FAILED",
            "ended_at": utc_now(),
            "duration_seconds": round(
                time.perf_counter() - start_clock,
                4,
            ),
            "failure_reason": f"{type(exc).__name__}: {exc}",
        })

        _write_json(metrics_path, metrics)
        raise

    finally:
        for df in (raw, accepted, quarantine, baseline, salted):
            if df is not None:
                try:
                    df.unpersist()
                except Exception:
                    pass
