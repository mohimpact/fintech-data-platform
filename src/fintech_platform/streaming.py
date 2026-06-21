"""Validate raw Kafka transactions and route clean records to downstream topics."""

from __future__ import annotations

import logging
import os

import great_expectations as gx
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from fintech_platform.config import (
    CLEAN_TRANSACTIONS_TOPIC,
    DEAD_LETTER_TOPIC,
    FRAUD_AMOUNT_THRESHOLD,
    KafkaConfig,
    RAW_TRANSACTIONS_TOPIC,
    VALID_CURRENCIES,
    VALID_MERCHANT_CATEGORIES,
)
from fintech_platform.schemas import TRANSACTION_COLUMNS, TRANSACTION_SCHEMA


CHECKPOINT_BASE = os.getenv("CHECKPOINT_BASE", "/app/checkpoints")
SPARK_MASTER = os.getenv("SPARK_MASTER_URL", "local[*]")

SPARK_PACKAGES = ",".join(
    [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
    ]
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
log = logging.getLogger("fintech_platform.streaming")


def build_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.appName("fintech-fraud-detection-streaming")
        .master(SPARK_MASTER)
        .config("spark.jars.packages", SPARK_PACKAGES)
        .config("spark.sql.shuffle.partitions", "3")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_kafka_stream(spark: SparkSession, kafka: KafkaConfig) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka.bootstrap_servers)
        .option("subscribe", RAW_TRANSACTIONS_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


def to_kafka_payload(df: DataFrame, value_columns: list[str]) -> DataFrame:
    return df.select(
        F.col("transaction_id").cast("string").alias("key"),
        F.to_json(F.struct(*[F.col(column) for column in value_columns])).alias("value"),
    )


def run_great_expectations_validation(df: DataFrame) -> None:
    """Profile a micro-batch with Great Expectations for observability."""
    if df.isEmpty():
        return

    try:
        context = gx.get_context(mode="ephemeral")
        datasource = context.sources.add_spark("spark_microbatch")
        data_asset = datasource.add_dataframe_asset(name="transactions")
        batch_request = data_asset.build_batch_request(dataframe=df)
        suite_name = "transaction_microbatch_suite"
        context.add_expectation_suite(suite_name)

        validator = context.get_validator(
            batch_request=batch_request,
            expectation_suite_name=suite_name,
        )
        validator.expect_column_values_to_not_be_null("transaction_id")
        validator.expect_column_values_to_not_be_null("user_id")
        validator.expect_column_values_to_not_be_null("amount")
        validator.expect_column_values_to_be_between("amount", min_value=0.01)
        validator.expect_column_values_to_be_in_set("currency", list(VALID_CURRENCIES))
        validator.expect_column_values_to_be_in_set("merchant_category", list(VALID_MERCHANT_CATEGORIES))
        validator.expect_column_value_lengths_to_equal("location", 2)

        results = validator.validate()
        log.info("GX validation success=%s stats=%s", results.success, results.statistics)
    except Exception as exc:
        log.warning("Great Expectations profiling failed: %s", exc)


def process_microbatch(raw_df: DataFrame, epoch_id: int) -> None:
    parsed = (
        raw_df.select(
            F.col("timestamp").alias("kafka_ingest_time"),
            F.from_json(F.col("value").cast("string"), TRANSACTION_SCHEMA).alias("payload"),
        )
        .select("kafka_ingest_time", "payload.*")
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    total_count = parsed.count()
    if total_count == 0:
        parsed.unpersist()
        return

    log.info("Processing microbatch %s with %s records", epoch_id, total_count)
    run_great_expectations_validation(parsed)

    required_columns = [
        "transaction_id",
        "user_id",
        "amount",
        "currency",
        "timestamp",
        "merchant_name",
        "merchant_category",
        "location",
    ]
    null_checks = [
        F.when(F.col(column).isNull(), F.lit(f"MISSING:{column}")).otherwise(F.lit(None))
        for column in required_columns
    ]
    rule_checks = [
        F.when(F.col("amount") <= 0, F.lit("RULE:amount_must_be_positive")).otherwise(F.lit(None)),
        F.when(~F.col("currency").isin(list(VALID_CURRENCIES)), F.lit("RULE:invalid_currency")).otherwise(F.lit(None)),
        F.when(
            ~F.col("merchant_category").isin(list(VALID_MERCHANT_CATEGORIES)),
            F.lit("RULE:invalid_merchant_category"),
        ).otherwise(F.lit(None)),
        F.when(F.length(F.col("location")) != 2, F.lit("RULE:location_must_be_2char_iso")).otherwise(F.lit(None)),
    ]

    enriched = (
        parsed.withColumn("_issues", F.array_compact(F.array(*(null_checks + rule_checks))))
        .withColumn("_is_valid", F.size("_issues") == 0)
        .withColumn("is_fraud_suspect", F.col("amount") > FRAUD_AMOUNT_THRESHOLD)
        .withColumn(
            "fraud_reason",
            F.when(F.col("is_fraud_suspect"), F.lit("amount_exceeds_threshold")).otherwise(F.lit(None)),
        )
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    clean_df = enriched.filter(F.col("_is_valid"))
    dlq_df = enriched.filter(~F.col("_is_valid")).withColumn("dlq_reason", F.col("_issues"))

    clean_count = clean_df.count()
    dlq_count = dlq_df.count()

    if clean_count:
        (
            to_kafka_payload(clean_df, TRANSACTION_COLUMNS + ["is_fraud_suspect", "fraud_reason"])
            .write.format("kafka")
            .option("kafka.bootstrap.servers", KafkaConfig().bootstrap_servers)
            .option("topic", CLEAN_TRANSACTIONS_TOPIC)
            .save()
        )

    if dlq_count:
        (
            to_kafka_payload(dlq_df, TRANSACTION_COLUMNS + ["dlq_reason"])
            .write.format("kafka")
            .option("kafka.bootstrap.servers", KafkaConfig().bootstrap_servers)
            .option("topic", DEAD_LETTER_TOPIC)
            .save()
        )

    log.info("Microbatch %s routed: %s clean, %s DLQ", epoch_id, clean_count, dlq_count)
    enriched.unpersist()
    parsed.unpersist()


def main() -> None:
    kafka = KafkaConfig()
    log.info("Starting streaming validator: %s -> %s/DLQ", RAW_TRANSACTIONS_TOPIC, CLEAN_TRANSACTIONS_TOPIC)
    spark = build_spark_session()
    raw_stream = read_kafka_stream(spark, kafka)
    query = (
        raw_stream.writeStream.foreachBatch(process_microbatch)
        .outputMode("update")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/streaming_validator")
        .trigger(processingTime="15 seconds")
        .start()
    )
    log.info("Streaming query running: id=%s", query.id)
    query.awaitTermination()


if __name__ == "__main__":
    main()
