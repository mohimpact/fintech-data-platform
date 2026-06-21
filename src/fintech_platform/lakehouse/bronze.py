"""Stream clean Kafka transactions into the Bronze Iceberg table."""

from __future__ import annotations

import logging
import os

from pyspark.sql import functions as F

from fintech_platform.config import CLEAN_TRANSACTIONS_TOPIC, KafkaConfig, LakehouseConfig
from fintech_platform.lakehouse.spark import build_iceberg_spark_session
from fintech_platform.schemas import CLEAN_TRANSACTION_SCHEMA


CHECKPOINT = os.getenv("BRONZE_CHECKPOINT", "/app/checkpoints/bronze_ingestion")
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
log = logging.getLogger("fintech_platform.lakehouse.bronze")


def main() -> None:
    kafka = KafkaConfig()
    lakehouse = LakehouseConfig()
    spark = build_iceberg_spark_session("fintech-lakehouse-bronze", lakehouse, extra_packages=[KAFKA_PACKAGE])

    catalog = lakehouse.catalog_name
    table_name = f"{catalog}.bronze.transactions"

    log.info("Creating Bronze namespace and table if needed")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.bronze")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            transaction_id STRING,
            user_id INT,
            amount DOUBLE,
            currency STRING,
            timestamp STRING,
            merchant_name STRING,
            merchant_category STRING,
            location STRING,
            is_fraud_suspect BOOLEAN,
            fraud_reason STRING,
            kafka_ingest_time TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (currency)
        """
    )

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka.bootstrap_servers)
        .option("subscribe", CLEAN_TRANSACTIONS_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_stream = raw_stream.select(
        F.col("timestamp").alias("kafka_ingest_time"),
        F.from_json(F.col("value").cast("string"), CLEAN_TRANSACTION_SCHEMA).alias("payload"),
    ).select("payload.*", "kafka_ingest_time")

    log.info("Writing clean transactions to %s", table_name)
    query = (
        parsed_stream.writeStream.format("iceberg")
        .outputMode("append")
        .trigger(processingTime="1 minute")
        .option("checkpointLocation", CHECKPOINT)
        .toTable(table_name)
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
