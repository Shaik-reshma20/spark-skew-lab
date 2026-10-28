import json
from pathlib import Path

import pytest
from pyspark.sql import Row
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from spark_skew_lab.pipeline import (
    EVENT_SCHEMA, assert_no_unexpected_json_fields, baseline_aggregate,
    salted_aggregate, validate_events,
)


def _row(event_id="e1", customer_id="customer_00001",
         event_ts="2025-01-01T00:00:00Z", amount=12.5,
         source_file="test.jsonl", run_id="r1"):
    return {
        "event_id": event_id, "customer_id": customer_id, "event_ts": event_ts,
        "amount": amount, "source_file": source_file, "run_id": run_id,
    }


def test_happy_path_and_aggregates_reconcile(spark):
    df = spark.createDataFrame([_row("e1", amount=10.0), _row("e2", amount=5.0)], EVENT_SCHEMA)
    accepted, rejected = validate_events(df, "test-run")
    assert accepted.count() == 2
    assert rejected.count() == 0
    base = {r["customer_id"]: (r["event_count"], r["total_amount"]) for r in baseline_aggregate(accepted).collect()}
    salted = {r["customer_id"]: (r["event_count"], r["total_amount"]) for r in salted_aggregate(accepted, 4).collect()}
    assert base.keys() == salted.keys()
    for key in base:
        assert base[key][0] == salted[key][0]
        assert abs(base[key][1] - salted[key][1]) < 1e-7


def test_empty_input(spark):
    df = spark.createDataFrame([], EVENT_SCHEMA)
    accepted, rejected = validate_events(df, "empty-run")
    assert accepted.count() == 0
    assert rejected.count() == 0
    assert baseline_aggregate(accepted).count() == 0


def test_malformed_missing_negative_and_unknown_reference(spark):
    records = [
        _row("e1", event_ts="not-a-timestamp"),
        _row("e2", customer_id=None),
        _row("e3", amount=-1.0),
        _row("e4", customer_id="customer_unknown"),
        _row("e5", amount=None),
    ]
    df = spark.createDataFrame(records, EVENT_SCHEMA)
    accepted, rejected = validate_events(df, "bad-run")
    assert accepted.count() == 0
    reasons = {r["reject_reason"] for r in rejected.select("reject_reason").collect()}
    assert "malformed_event_ts" in reasons
    assert "missing_customer_id" in reasons
    assert "negative_amount" in reasons
    assert "unknown_customer_id" in reasons
    assert "missing_or_malformed_amount" in reasons


def test_duplicate_event_id_quarantines_later_copy(spark):
    df = spark.createDataFrame([_row("same", amount=2.0), _row("same", amount=9.0)], EVENT_SCHEMA)
    accepted, rejected = validate_events(df, "dup-run")
    assert accepted.count() == 1
    assert rejected.filter("reject_reason = 'duplicate_event_id'").count() == 1

def test_unexpected_schema_field_fails_fast(tmp_path):
    path = tmp_path / "unexpected.jsonl"
    path.write_text(
        '{"event_id":"e1","customer_id":"customer_00001",'
        '"event_ts":"2025-01-01T00:00:00Z","amount":1.0,'
        '"source_file":"x","run_id":"r1","unexpected_column":"oops"}\n',
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="Unexpected schema field"):
        assert_no_unexpected_json_fields(str(path))


def test_salt_requires_multiple_buckets(spark):
    df = spark.createDataFrame([_row()], EVENT_SCHEMA)
    accepted, _ = validate_events(df, "salt-test")
    with pytest.raises(ValueError):
        salted_aggregate(accepted, 1)
