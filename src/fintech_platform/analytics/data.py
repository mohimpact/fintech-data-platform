"""Data access helpers for the Streamlit dashboard."""

from __future__ import annotations

import os
import random
from datetime import datetime, timedelta

import pandas as pd


MERCHANT_CATEGORIES = ["Retail", "Food", "Travel", "Electronics", "Entertainment", "Healthcare", "Utilities"]


def generate_demo_data() -> pd.DataFrame:
    """Generate deterministic Gold-layer data for local demos and tests."""
    random.seed(42)
    records = []
    base_date = datetime.today() - timedelta(days=29)

    for offset in range(30):
        date = (base_date + timedelta(days=offset)).strftime("%Y-%m-%d")
        for category in MERCHANT_CATEGORIES:
            total_volume = random.uniform(10_000, 500_000)
            transaction_count = random.randint(50, 2_000)
            fraud_count = random.randint(0, max(1, int(transaction_count * 0.05)))
            records.append(
                {
                    "date": date,
                    "merchant_category": category,
                    "total_volume": round(total_volume, 2),
                    "transaction_count": transaction_count,
                    "average_transaction_size": round(total_volume / transaction_count, 2),
                    "fraud_count": fraud_count,
                }
            )

    return pd.DataFrame(records)


def load_gold_data_from_minio() -> pd.DataFrame:
    """Read the Gold merchant metrics table data files from MinIO."""
    import pyarrow.parquet as pq
    import s3fs

    endpoint = os.getenv("S3_ENDPOINT", "http://127.0.0.1:9000")
    filesystem = s3fs.S3FileSystem(
        key=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        secret=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        client_kwargs={"endpoint_url": endpoint},
    )

    gold_path = os.getenv("GOLD_DATA_PATH", "warehouse/local/gold/merchant_metrics/data")
    if not filesystem.exists(gold_path):
        return pd.DataFrame()

    dataset = pq.ParquetDataset(f"s3://{gold_path}", filesystem=filesystem)
    return dataset.read().to_pandas()


def load_dashboard_data(use_demo: bool = False) -> tuple[pd.DataFrame, bool]:
    """Return dashboard data and whether it came from the live Gold table."""
    if use_demo:
        return generate_demo_data(), False

    try:
        live_data = load_gold_data_from_minio()
    except Exception:
        live_data = pd.DataFrame()

    if live_data.empty:
        return generate_demo_data(), False

    return live_data, True
