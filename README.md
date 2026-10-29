# Spark Skew Lab

A local-first, reproducible PySpark data-engineering project for the YottaFlex 72-hour Spark skew challenge. It demonstrates synthetic data generation, validation and quarantine, duplicate handling, baseline vs. salted aggregation, reconciliation, run metrics, and automated tests.

> **Benchmark evidence:** This repository includes measured, machine-specific results in `reports/benchmark.json`. Results are preliminary and should not be interpreted as a production performance guarantee. Run the benchmark on your machine and commit the generated report with your environment details.

## Requirements

- Python 3.10–3.11 recommended
- Java 17 (or a Java version supported by your chosen PySpark release)
- Git
- A terminal / command prompt

PySpark launches a local JVM. If startup fails, check `java -version` and `JAVA_HOME`.

## Quick start

### macOS / Linux

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
python -m spark_skew_lab.cli generate --rows 10000 --output data/generated/events.jsonl
python -m spark_skew_lab.cli run --input data/generated/events.jsonl --output-dir output
python -m spark_skew_lab.cli benchmark --rows 100000 --output reports/benchmark.json --repetitions 3
pytest -q
```

### Windows PowerShell

```powershell
py -3.11 -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
python -m spark_skew_lab.cli generate --rows 10000 --output data/generated/events.jsonl
python -m spark_skew_lab.cli run --input data/generated/events.jsonl --output-dir output
python -m spark_skew_lab.cli benchmark --rows 100000 --output reports/benchmark.json --repetitions 3
pytest -q
```

If PowerShell blocks activation, use `Set-ExecutionPolicy -Scope Process Bypass` for that terminal session or run the commands from Command Prompt with `.venv\\Scripts\\activate.bat`.

## Project layout

```text
spark-skew-lab/
├── src/spark_skew_lab/
│   ├── __init__.py
│   ├── cli.py
│   ├── generator.py
│   ├── pipeline.py
│   └── benchmark.py
├── tests/
├── data/sample/events.jsonl
├── reports/
├── requirements.txt
├── pyproject.toml
└── README.md
```

## Architecture

1. **Generate** synthetic event records using a fixed random seed and a configurable hot-key share.
2. **Ingest** JSON Lines with an explicit Spark schema.
3. **Validate** required fields, timestamp/amount validity, customer references, and duplicate event IDs.
4. **Quarantine** rejected rows with source file, run ID, and reject reason.
5. **Aggregate** accepted events by customer using a baseline group-by and a two-stage salted group-by.
6. **Reconcile** counts and totals, write output, and record operational metrics.
7. **Benchmark** both algorithms on the same generated input and report timings. The output is machine-specific.

## Data dictionary

### `events.jsonl` (one JSON object per line)

| Column | Type | Rule |
|---|---|---|
| `event_id` | string | Required, stable event key; duplicate IDs are rejected after keeping the first deterministic input occurrence |
| `customer_id` | string | Required; must exist in the generated/reference customer set |
| `event_ts` | string | ISO-8601 timestamp; required and parseable |
| `amount` | number | Required, finite, nonnegative; converted to Spark `double` for this benchmark |
| `source_file` | string | Source traceability |
| `run_id` | string | Processing run identifier |

The generator creates customer IDs `customer_00001` through `customer_00100` plus `customer_hot_001`. The hot key is deliberately assigned a configurable share of records. `event_id` is deterministic for a given generated row index.

### Output tables

Each pipeline run publishes datasets under its unique run directory:

- `accepted/`: validated event records in Parquet.
- `quarantine/`: rejected records with a `reject_reason`, in Parquet.
- `baseline/`: baseline customer aggregates in Parquet.
- `salted/`: salted customer aggregates in Parquet.
- `run_metrics.json`: run ID, timestamps, status, input/accepted/rejected/output counts, duration, reconciliation results, output paths, and failure details if available.

## CLI usage

```bash
# Generate deterministic input (same parameters => same logical records)
python -m spark_skew_lab.cli generate --rows 100000 --seed 42 --hot-share 0.70 --output data/generated/events.jsonl

# Run ingestion, validation, quarantine, aggregates, and reconciliation
python -m spark_skew_lab.cli run --input data/generated/events.jsonl --output-dir output

# Run benchmark (uses synthetic input generated in memory)
python -m spark_skew_lab.cli benchmark --rows 100000 --seed 42 --hot-share 0.70 --salt-buckets 16 --repetitions 3 --output reports/benchmark.json
```

Run `python -m spark_skew_lab.cli --help` for command options.

## Benchmark methodology and limitations

- Both algorithms use the same valid input DataFrame and Spark session.
- A warm-up run is performed; each measured output is materialized with `count()` before timing stops.
- The report includes median runtime and individual repetitions. Runtime includes aggregation and output materialization, not input generation.
- Spark configuration and local hardware influence results. Run more repetitions and compare medians for stronger evidence.
- For simple `count`/`sum` aggregation, Spark's partial aggregation and Adaptive Query Execution (AQE) may already reduce skew impact. Salting adds a stage and can be slower. A negative result is valid evidence.
- For the benchmark only, Spark's AQE is disabled to keep the comparison easier to interpret. This is not a claim that disabling AQE is optimal in production.
- `event_id`-based hashing distributes a hot key's events across salt buckets only if IDs are stable and reasonably distributed.
- This starter uses a local filesystem and is intended for a single writer. It does not provide transactional commits for concurrent writers or object stores.
- `amount` uses `double` to keep the starter approachable. For financial-grade exact arithmetic, use `DecimalType` and define rounding rules.

## Idempotency, retries, and backfills

- Event IDs are deterministic for generated records.
- Duplicate IDs within a batch are handled deterministically: the first occurrence in input order is accepted and later occurrences are quarantined.
- Each run writes to a run-specific output directory. Re-running creates a new run directory rather than appending to an existing dataset.
- The pipeline checks reconciliation before marking the run successful.
- If a run fails, inspect its metrics and remove or archive the incomplete run directory before retrying. For production, publish to a temporary path and atomically switch a manifest/pointer only after validation succeeds.
- Backfills should use the same generator seed or an immutable source snapshot, with a documented event-time window and a new `run_id`.

## Tests

Tests cover valid data, empty input, malformed/missing fields, duplicates, unexpected schema, invalid references, negative amounts, and baseline-vs-salted reconciliation. Some tests use local Spark and may take a little longer on the first JVM startup.

### Test results

- **Local test suite:** 9 tests passed in the validated development environment.
- **GitHub Actions:** The `tests` workflow completed successfully for commit `467b009` (workflow run #7).
- **Test command:** `python -m pytest -q`

Tests cover data generation, validation, quarantine, duplicate handling, and baseline-versus-salted reconciliation.

> **Execution note:** The automated test suite passed 9 tests in the validated development environment. A Windows end-to-end run also succeeded for 1,000 generated records: 1,000 accepted, 0 rejected, 95 aggregate groups from both methods, and successful reconciliation. Hadoop-related warnings appeared but did not prevent this run. This is a small clean-input validation, not proof of every production or edge-case scenario.

### Benchmark status

**Benchmark results (2026-10-28, local WSL2 environment)**

- Input: 100,000 records; seed 42; hot share 0.70.
- Salt buckets: 16; shuffle partitions: 8; AQE disabled.
- Warm-up: 1 run per method; measured repetitions: 3.
- Baseline timings: 1.5805, 0.4671, 0.5866 seconds; median: 0.5866 seconds.
- Salted timings: 1.4060, 0.5368, 0.3958 seconds; median: 0.5368 seconds.
- Baseline/salted median ratio: 1.0928x; salted median was 8.49% lower in this run.
- Output group counts: 101 for both methods; group-count check passed.
- Environment: Python 3.11.18, PySpark/Spark 3.5.5, WSL2 Linux, `local[*]`.

**Limitations:** Preliminary machine-specific result based on three measured repetitions. Timings varied between runs. Matching group counts alone does not prove aggregate values match. Benchmark timings cover aggregation and `count()` materialization, not the full pipeline. See `reports/benchmark.json`.

## GitHub submission

1. Create a **public** repository named `spark-skew-lab`.
2. From this project directory:

   ```bash
   git init
   git add .
   git commit -m "Build reproducible Spark skew lab"
   git branch -M main
   git remote add origin https://github.com/YOUR_USERNAME/spark-skew-lab.git
   git push -u origin main
   ```

3. Run all commands from a clean environment and commit the actual `reports/benchmark.json` output only after you have run it.
4. Do not commit `.venv/`, Spark temporary files, credentials, or fabricated performance results.
