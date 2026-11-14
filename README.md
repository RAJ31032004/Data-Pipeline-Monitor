# Data Pipeline Monitoring & SLA Management

A reproducible, local-first data engineering project that ingests multiple CSV datasets, validates records, quarantines invalid rows, publishes valid rows idempotently, records operational metrics, and evaluates dataset arrival/completion SLAs.

## Architecture

```text
CSV producers
    |
    v
Ingestion (run_id, source_file, timestamps)
    |
    v
Schema + business validation -----> rejected_records (quarantine)
    |
    v
Idempotent publish (SQLite target_records)
    |
    +----> pipeline_runs / dataset_loads
                    |
                    v
             SLA monitor + Streamlit dashboard
```

## Stack

- Python 3.10+
- CSV + Python standard library for ingestion and validation
- SQLite for durable local metadata and target storage
- Pandas + Plotly + Streamlit for the monitoring UI
- Pytest for automated tests

The core pipeline intentionally uses the standard library and SQLite to keep setup simple and reproducible. Pandas is used by the dashboard.

## Quick start

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
# macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt

# Generate deterministic sample data
python scripts/generate_data.py --out-dir data/raw --scale 1 --seed 42

# Run ingestion, validation and publishing
python -m src.pipeline run

# Evaluate SLA status for a reproducible demo date
python -m src.pipeline monitor --as-of 2026-11-14T12:00:00+00:00

# Run tests
pytest -q

# Start dashboard
streamlit run dashboard/app.py
```

For a larger volume, use `--scale 200` to generate approximately 100,000 orders (plus customers/products):
`python scripts/generate_data.py --out-dir data/raw --scale 200 --seed 42`

## Data model and grain

| Entity/table | Grain / key | Purpose |
|---|---|---|
| customers CSV | one row per `customer_id` | Customer master |
| products CSV | one row per `product_id` | Product master |
| orders CSV | one row per `order_id` | Order event |
| `target_records` | `(dataset_name, record_key)` | Published record payload |
| `pipeline_runs` | one row per pipeline invocation; `run_id` | Run status and aggregate metrics |
| `dataset_loads` | one row per dataset per run | Arrival/completion and SLA evidence |
| `rejected_records` | one row per rejected source row | Raw record, source row and reason |

`orders.customer_id` references `customers.customer_id`; `orders.product_id` references `products.product_id`. The configured dataset primary key is unique in the target. Customer email is checked for valid format; configured unique columns are documented but not currently enforced across separate batches.

## Data dictionary

- `run_id`: UUID for one pipeline invocation.
- `source_file`: source CSV path used for lineage.
- `arrival_at`: producer arrival timestamp supplied to the run (defaults to run start).
- `completed_at`: time the dataset processing finished.
- `freshness_minutes`: evaluation time minus latest successful completion time.
- `status`: `ON_TIME`, `LATE`, `MISSING`, or `FAILED`.
- `rejection_reason`: deterministic validation reason.
- `payload_json`: original validated source values stored as JSON.

All operational timestamps are ISO-8601. Naive timestamps are interpreted as UTC. The sample config uses UTC wall-clock SLA times for deterministic local demonstrations.

## Validation and behavior

- Exact source schema is checked before reading each dataset. Unexpected/missing columns fail the run with a recorded failure reason.
- Required fields, configured primitive types, positive quantities/prices, email shape and datetime parsing are validated.
- Orders are checked against already-published customer and product keys. Datasets are processed in config order.
- Invalid rows are written to `rejected_records`, including source path, row number, raw JSON and reason.
- Duplicate primary keys within a batch are quarantined. Keys already published by a previous run are skipped and counted as `duplicate_rows`.
- Empty CSV batches with headers are valid.
- Target primary key `(dataset_name, record_key)` makes publishing idempotent for the same business key.
- Each pipeline invocation gets a `run_id`; aggregate metrics and failures are stored in `pipeline_runs`.

## Retry, restart and backfill

Retry by rerunning the same command. Already-published keys are skipped, while new valid keys can be published. Rejected rows are recorded per run so a retry remains auditable. A failed run is marked `FAILED`; rerun after correcting the source/config. For backfills, provide the historical `--arrival-at` timestamp and retain source files; target keys remain deduplicated. This starter uses append-only target records and does not implement updates/SCD2.

## SLA logic

For each dataset, the configured `expected_arrival` plus `sla_minutes` defines the deadline. The monitor compares both arrival and completion timestamps with that deadline. No load for the evaluation date is `MISSING`; arrival or completion after deadline is `LATE`; otherwise it is `ON_TIME`. Freshness is the age in minutes since completion. The default arrival timestamp is pipeline start; pass `--arrival-at` to simulate source arrival independently.

## Reconciliation

The database is the source of operational truth. Useful checks:

```sql
-- Run-level reconciliation
SELECT run_id, input_rows, output_rows, rejected_rows, duplicate_rows,
       input_rows - output_rows - rejected_rows - duplicate_rows AS unexplained
FROM pipeline_runs;

-- Published records by dataset
SELECT dataset_name, COUNT(*) AS published_rows
FROM target_records GROUP BY dataset_name;

-- Quarantine breakdown
SELECT dataset_name, rejection_reason, COUNT(*) AS rejected
FROM rejected_records GROUP BY dataset_name, rejection_reason;

-- Latest dataset load status
SELECT dataset_name, status, arrival_at, completed_at, input_rows, output_rows, rejected_rows
FROM dataset_loads ORDER BY completed_at DESC;
```

For a first run, the row accounting equation reconciles exactly for each dataset after aggregating input/output/reject/duplicate counts. Pipeline-level metrics aggregate all datasets.

## Tests

`pytest -q` covers successful ingestion, empty input, unexpected schema, missing required values, invalid foreign keys, duplicate/rerun idempotency, and late SLA detection.

## Performance measurement and optimization

Measure locally with:
```bash
python scripts/generate_data.py --scale 200 --seed 42
time python -m src.pipeline run
```
This creates 100,000 order rows plus reference datasets. A reasonable next optimization is batch inserts with `executemany` and bounded transaction chunks. For larger production volumes, move to partitioned Parquet/object storage and a warehouse/lakehouse engine; keep the run/reject/SLA metadata model.

## Trade-offs and limitations

- Local SQLite is chosen for zero-service setup; it is not a concurrent multi-writer production warehouse.
- This is a batch CSV vertical slice, not a streaming platform.
- SLA schedule times are UTC in this sample; production deployments should store each dataset's timezone explicitly.
- The generator creates deterministic invalid records; it does not include hidden expected-answer labels in input files.
- Alerting is surfaced in the dashboard/CLI output; email/Slack delivery is a clear extension point.
- `unique` config metadata is reserved for future cross-row/cross-batch uniqueness rules; the current implementation enforces primary keys and email syntax.

## Suggested next steps

Add schema versioning, dataset-specific tables, cross-batch unique constraints, alert delivery integration, scheduler, and SCD Type 2 history if mutable dimensions are introduced.
