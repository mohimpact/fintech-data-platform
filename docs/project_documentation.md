# Fintech Data Platform: Technical Walkthrough

## Overview

This repository implements IDEA1 from the roadmap: a real-time fintech fraud detection and analytics platform. It uses a Kappa-style streaming path for validation and fraud enrichment, then lands clean records in an Iceberg lakehouse for batch Gold metrics.

## Runtime Flow

1. `fintech_platform.producer` generates synthetic card transactions and writes JSON events to Kafka topic `raw_transactions`.
2. `fintech_platform.streaming` reads `raw_transactions`, validates schema and business rules, flags high-value fraud suspects, then routes:
   - valid records to `clean_transactions`
   - malformed records to `dead_letter_queue`
3. `fintech_platform.lakehouse.bronze` streams `clean_transactions` into `local.bronze.transactions` on MinIO using Apache Iceberg.
4. Airflow DAG `silver_gold_etl` runs `fintech_platform.lakehouse.silver_gold`:
   - Bronze to Silver: deduplicate by `transaction_id`, normalize event date
   - Silver to Gold: aggregate merchant-category metrics for volume, count, average transaction size, and fraud suspects
5. `fintech_platform.analytics.app` reads Gold table Parquet files from MinIO and falls back to deterministic demo data when the lakehouse is empty.

## Key Design Choices

- Shared topics, fraud threshold, currencies, categories, and lakehouse settings live in `src/fintech_platform/config.py`.
- Shared Spark schemas live in `src/fintech_platform/schemas.py` so streaming and Bronze ingestion cannot drift silently.
- Fraud detection is a curated attribute on valid data, not a reason to discard the transaction. DLQ is reserved for malformed or invalid events.
- Docker uses one Spark app image for both the streaming validator and Bronze ingestion.
- Airflow calls packaged Python modules with `python -m`, avoiding path-specific scripts.

## Useful Commands

```bash
docker compose up --build -d
PYTHONPATH=src python -m fintech_platform.producer
PYTHONPATH=src streamlit run src/fintech_platform/analytics/app.py
pytest
```

## Next Improvements

- Add integration tests using Testcontainers or a lightweight Kafka-compatible broker.
- Add schema contracts with a registry-compatible format such as JSON Schema or Avro.
- Add a DLQ inspection dashboard or replay workflow.
- Replace threshold-based fraud logic with a scored rules engine or model inference service.
