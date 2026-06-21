# Local Setup Guide

This guide runs the IDEA1 fintech fraud detection platform locally with Docker Compose.

## Credentials

| Service | URL | Username | Password |
| --- | --- | --- | --- |
| MinIO | `http://localhost:9001` | `fintechadmin` | `fintechadmin123` |
| Airflow | `http://localhost:8080` | `fintechadmin` | `fintechadmin123` |
| Dashboard | `http://localhost:8501` | none | none |

These are local development credentials only. Change them before using this outside your machine.

## Start Everything

```bash
docker compose up --build -d
```

This starts:

- `zookeeper`: Kafka coordination.
- `kafka`: message broker for raw, clean, and DLQ events.
- `kafka-setup`: creates Kafka topics.
- `minio`: local S3-compatible lakehouse storage.
- `minio-setup`: creates `warehouse` and `checkpoints` buckets.
- `spark-streaming-job`: validates raw transactions and writes clean/DLQ events.
- `lakehouse-bronze`: writes clean Kafka events to the Bronze Iceberg table.
- `airflow-standalone`: creates the fixed admin user, starts the scheduler, and serves Airflow.
- `analytics-dashboard`: serves Streamlit analytics from the Gold layer.

## Data Flow

```text
Producer
  -> Kafka raw_transactions
  -> Spark streaming validation
  -> Kafka clean_transactions / dead_letter_queue
  -> Bronze Iceberg table in MinIO
  -> Airflow Silver/Gold ETL
  -> Streamlit dashboard
```

Fraud suspects are not dropped. Valid high-value transactions are stored with `is_fraud_suspect=true`. Only malformed records go to `dead_letter_queue`.

## Smoke Test

Check services:

```bash
docker compose ps
```

Send 50 test transactions:

```bash
PYTHONPATH=src python -c "import json, time; from confluent_kafka import Producer; from fintech_platform.producer import generate_transaction; p=Producer({'bootstrap.servers':'localhost:9092'}); [p.produce('raw_transactions', json.dumps(generate_transaction()).encode('utf-8')) or p.poll(0) or time.sleep(0.02) for _ in range(50)]; p.flush(); print('sent 50 transactions')"
```

Check streaming validation:

```bash
docker compose logs --tail=120 spark-streaming-job
```

Look for:

```text
Microbatch ... routed: ... clean, ... DLQ
```

Check clean-topic offsets:

```bash
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka:29092 --topic clean_transactions
```

## Lakehouse Checks

Open MinIO and browse:

```text
warehouse/bronze/transactions/
```

After running the Airflow ETL, browse:

```text
warehouse/silver/transactions/
warehouse/gold/merchant_metrics/
```

## Run The Airflow ETL

UI path:

1. Open `http://localhost:8080`.
2. Log in with `fintechadmin` / `fintechadmin123`.
3. Unpause `silver_gold_etl`.
4. Trigger the DAG.
5. Wait for `success`.

CLI path:

```bash
docker exec airflow-standalone airflow dags unpause silver_gold_etl
docker exec airflow-standalone airflow dags trigger silver_gold_etl
docker exec airflow-standalone airflow dags list-runs -d silver_gold_etl
```

## Dashboard

Open:

```text
http://localhost:8501
```

The dashboard falls back to demo data when Gold files do not exist yet. Once the ETL writes Gold files, it reads live lakehouse data from MinIO.

## Stop And Clean

Stop containers:

```bash
docker compose down
```

Stop containers and remove local lakehouse/checkpoint volumes:

```bash
docker compose down -v --remove-orphans
```

Prune unused Docker resources:

```bash
docker system prune -af --volumes
```

