# Fintech Data Platform

An end-to-end IDEA1 implementation: real-time fintech fraud detection and analytics using Kafka, PySpark Structured Streaming, Apache Iceberg, MinIO, Airflow, and Streamlit.

## Architecture

```text
Synthetic Producer
  -> Kafka raw_transactions
  -> PySpark validator
  -> Kafka clean_transactions + dead_letter_queue
  -> Bronze Iceberg table on MinIO
  -> Airflow Silver/Gold batch ETL
  -> Streamlit Gold analytics dashboard
```

Fraud is treated as an enrichment signal, not a data-quality failure. Valid high-value transactions are stored in the lakehouse with `is_fraud_suspect=true`; malformed records are sent to the DLQ.

## Project Structure

```text
fintech-data-platform/
|-- airflow/dags/                 # Airflow orchestration
|-- docker/                       # Runtime images
|-- docs/                         # Planning, architecture, and walkthrough docs
|-- requirements/                 # Runtime and dev dependency groups
|-- src/fintech_platform/         # Python package
|   |-- analytics/                # Streamlit app and dashboard data loading
|   |-- lakehouse/                # Bronze ingestion and Silver/Gold ETL
|   |-- config.py                 # Shared environment-driven config
|   |-- producer.py               # Synthetic transaction producer
|   |-- schemas.py                # Shared Spark schemas
|   `-- streaming.py              # Kafka validation and routing job
|-- tests/                        # Unit tests
|-- docker-compose.yml
`-- pyproject.toml
```

## Run Locally

Start Kafka, MinIO, Spark streaming, Bronze ingestion, Airflow, and the dashboard:

```bash
docker compose up --build -d
```

### Local Credentials

- MinIO: `http://localhost:9001` (`minioadmin` / `minioadmin`)
- Airflow: `http://localhost:8080` (`airflow` / `airflow`)
- Dashboard: `http://localhost:8501`

### What Starts

- `zookeeper`: coordinates Kafka.
- `kafka`: stores raw, clean, and DLQ transaction topics.
- `kafka-setup`: creates `raw_transactions`, `clean_transactions`, and `dead_letter_queue`.
- `minio`: local S3-compatible object storage for the lakehouse.
- `minio-setup`: creates the `warehouse` and `checkpoints` buckets.
- `spark-streaming-job`: validates raw Kafka events and writes valid events to `clean_transactions`.
- `lakehouse-bronze`: consumes `clean_transactions` and writes Bronze Iceberg files to MinIO.
- `airflow-standalone`: runs the orchestration UI and the Silver/Gold ETL DAG.
- `analytics-dashboard`: serves the Streamlit dashboard from the Gold layer.

### How Data Moves

1. The producer creates synthetic card transactions.
2. Kafka receives those events in `raw_transactions`.
3. Spark validates each micro-batch with schema and business-rule checks.
4. Valid records move to `clean_transactions`.
5. Invalid or malformed records move to `dead_letter_queue`.
6. Bronze ingestion reads clean Kafka records and writes Iceberg data files to MinIO.
7. Airflow runs the Silver/Gold ETL:
   - Silver deduplicates and normalizes transactions.
   - Gold aggregates merchant-category metrics.
8. The dashboard reads Gold files from MinIO and visualizes volume, counts, and fraud suspects.

High-value transactions are not discarded. They are stored with `is_fraud_suspect=true` so they remain available for analytics.

## Test The Pipeline

### 1. Confirm Services Are Running

```bash
docker compose ps
```

Expected result: Kafka and MinIO should be healthy, and Spark, Bronze, Airflow, and Dashboard should be up.

### 2. Send Test Transactions

For a continuous producer:

```bash
python -m pip install -r requirements/producer.txt
PYTHONPATH=src python -m fintech_platform.producer
```

For a finite smoke-test batch:

```bash
PYTHONPATH=src python -c "import json, time; from confluent_kafka import Producer; from fintech_platform.producer import generate_transaction; p=Producer({'bootstrap.servers':'localhost:9092'}); [p.produce('raw_transactions', json.dumps(generate_transaction()).encode('utf-8')) or p.poll(0) or time.sleep(0.02) for _ in range(50)]; p.flush(); print('sent 50 transactions')"
```

### 3. Check Streaming Validation

```bash
docker compose logs --tail=120 spark-streaming-job
```

Look for a line similar to:

```text
Microbatch 1 routed: 50 clean, 0 DLQ
```

You can also inspect clean-topic offsets:

```bash
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka:29092 --topic clean_transactions
```

### 4. Confirm Bronze Files In MinIO

Open MinIO at `http://localhost:9001`, then browse:

```text
warehouse/bronze/transactions/
```

You should see Iceberg `data/` and `metadata/` files.

### 5. Run Silver/Gold ETL

In Airflow, open `http://localhost:8080`, unpause `silver_gold_etl`, and trigger it.

CLI option:

```bash
docker exec airflow-standalone airflow dags unpause silver_gold_etl
docker exec airflow-standalone airflow dags trigger silver_gold_etl
docker exec airflow-standalone airflow dags list-runs -d silver_gold_etl
```

Expected result: the DAG run reaches `success`.

### 6. Confirm Gold Files

In MinIO, browse:

```text
warehouse/gold/merchant_metrics/
```

The dashboard at `http://localhost:8501` should switch from demo fallback to live Gold data when Gold files are available.

### 7. Stop The Stack

```bash
docker compose down
```

To remove local MinIO data as well:

```bash
docker compose down -v
```

Run the dashboard outside Docker, if you prefer local Streamlit development:

```bash
python -m pip install -r requirements/analytics.txt
PYTHONPATH=src streamlit run src/fintech_platform/analytics/app.py
```

## Development

```bash
python -m pip install -r requirements/dev.txt
pytest
```

The Dockerized Spark jobs use MinIO by default. Override `S3_ENDPOINT`, `WAREHOUSE_PATH`, `AWS_ACCESS_KEY_ID`, and `AWS_SECRET_ACCESS_KEY` to point the same jobs at another S3-compatible target.
