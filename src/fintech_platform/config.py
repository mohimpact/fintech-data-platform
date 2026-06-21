"""Shared configuration for the fintech data platform."""

from __future__ import annotations

import os
from dataclasses import dataclass


RAW_TRANSACTIONS_TOPIC = os.getenv("RAW_TRANSACTIONS_TOPIC", "raw_transactions")
CLEAN_TRANSACTIONS_TOPIC = os.getenv("CLEAN_TRANSACTIONS_TOPIC", "clean_transactions")
DEAD_LETTER_TOPIC = os.getenv("DEAD_LETTER_TOPIC", "dead_letter_queue")

VALID_CURRENCIES = ("USD", "EUR", "GBP", "JPY")
VALID_MERCHANT_CATEGORIES = (
    "Retail",
    "Food",
    "Travel",
    "Electronics",
    "Entertainment",
    "Healthcare",
    "Utilities",
)
FRAUD_AMOUNT_THRESHOLD = float(os.getenv("FRAUD_AMOUNT_THRESHOLD", "5000"))


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


@dataclass(frozen=True)
class LakehouseConfig:
    access_key: str = os.getenv("AWS_ACCESS_KEY_ID", "fintechadmin")
    secret_key: str = os.getenv("AWS_SECRET_ACCESS_KEY", "fintechadmin123")
    endpoint: str = os.getenv("S3_ENDPOINT", "http://minio:9000")
    warehouse_path: str = os.getenv("WAREHOUSE_PATH", "s3a://warehouse/")
    catalog_name: str = os.getenv("ICEBERG_CATALOG", "local")
