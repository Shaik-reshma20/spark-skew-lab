"""Deterministic synthetic event generator."""
from __future__ import annotations

import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable


def make_events(
    rows: int = 10_000,
    seed: int = 42,
    hot_share: float = 0.70,
    source_file: str = "synthetic",
    run_id: str = "generator",
) -> list[dict]:
    """Create reproducible event records; hot_share is the approximate hot-key fraction."""
    if rows < 0:
        raise ValueError("rows must be >= 0")
    if not 0.0 <= hot_share <= 1.0:
        raise ValueError("hot_share must be between 0 and 1")

    rng = random.Random(seed)
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    result = []
    for i in range(rows):
        is_hot = rng.random() < hot_share
        customer_id = (
            "customer_hot_001"
            if is_hot
            else f"customer_{rng.randint(1, 100):05d}"
        )
        event_time = start + timedelta(seconds=rng.randint(0, max(rows, 1) * 10))
        result.append({
            "event_id": f"event_{i:012d}",
            "customer_id": customer_id,
            "event_ts": event_time.isoformat().replace("+00:00", "Z"),
            "amount": round(rng.uniform(0.0, 500.0), 2),
            "source_file": source_file,
            "run_id": run_id,
        })
    return result


def write_jsonl(records: Iterable[dict], path: str | Path) -> int:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for record in records:
            handle.write(json.dumps(record, sort_keys=True) + "\n")
            count += 1
    return count


def generate_jsonl(
    path: str | Path,
    rows: int = 10_000,
    seed: int = 42,
    hot_share: float = 0.70,
) -> int:
    return write_jsonl(
        make_events(rows=rows, seed=seed, hot_share=hot_share, source_file=str(path)),
        path,
    )
