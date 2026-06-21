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

Local services:

- MinIO: `http://localhost:9001` (`fintechadmin` / `fintechadmin123`)
- Airflow: `http://localhost:8080` (`fintechadmin` / `fintechadmin123`)
- Dashboard: `http://localhost:8501`

See [docs/setup.md](docs/setup.md) for the full startup guide, credentials, smoke test, data movement explanation, and cleanup commands.

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
