"""
Phase 2: PySpark Structured Streaming Application
====================================================
Reads raw transactions from Kafka, runs Great Expectations for batch quality profiling,
applies 12-point data quality checks (schema validation + business rules) row-by-row,
then routes to:

  ✅  clean_transactions  — valid, non-suspect records
  ❌  dead_letter_queue   — invalid schema OR high-value fraud suspects
"""

import os
import json
import logging

import great_expectations as gx
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType,
)

# ─────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────
KAFKA_SERVERS    = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SPARK_MASTER     = os.getenv("SPARK_MASTER_URL",        "local[*]")
CHECKPOINT_BASE  = "/app/checkpoints"

RAW_TOPIC        = "raw_transactions"
CLEAN_TOPIC      = "clean_transactions"
DLQ_TOPIC        = "dead_letter_queue"

SPARK_PACKAGES = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
])

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")
log = logging.getLogger("FinTechStreaming")

TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id",    StringType(),  True),
    StructField("user_id",           IntegerType(), True),
    StructField("amount",            DoubleType(),  True),
    StructField("currency",          StringType(),  True),
    StructField("timestamp",         StringType(),  True),
    StructField("merchant_name",     StringType(),  True),
    StructField("merchant_category", StringType(),  True),
    StructField("location",          StringType(),  True),
])

VALID_CURRENCIES  = ["USD", "EUR", "GBP", "JPY"]
VALID_CATEGORIES  = ["Retail", "Food", "Travel", "Electronics", "Entertainment", "Healthcare", "Utilities"]
FRAUD_THRESHOLD   = 5_000.0


def build_spark_session() -> SparkSession:
    log.info("Building SparkSession (master=%s)…", SPARK_MASTER)
    spark = (
        SparkSession.builder
        .appName("FinTech-FraudDetection-Streaming")
        .master(SPARK_MASTER)
        .config("spark.jars.packages",               SPARK_PACKAGES)
        .config("spark.sql.shuffle.partitions",       "3")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.ui.showConsoleProgress",       "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_kafka_stream(spark: SparkSession):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe",               RAW_TOPIC)
        .option("startingOffsets",         "latest")
        .option("failOnDataLoss",          "false")
        .load()
    )

def _to_kafka_payload(df, extra_cols: list = None):
    user_cols = [c for c in df.columns if not c.startswith("_") and c != "kafka_ingest_time"]
    if extra_cols:
        user_cols += extra_cols

    struct_fields = [F.col(c) for c in user_cols if c in df.columns]

    return df.select(
        F.col("transaction_id").cast("string").alias("key"),
        F.to_json(F.struct(*struct_fields)).alias("value"),
    )

def run_great_expectations_validation(df):
    """
    Runs Great Expectations validations on a single micro-batch DataFrame.
    """
    if df.isEmpty():
        return
        
    try:
        # Create an ephemeral data context
        context = gx.get_context(mode="ephemeral")
        
        # Add a Spark datasource
        datasource = context.sources.add_spark("spark_microbatch")
        
        # Add DataFrame asset
        data_asset = datasource.add_dataframe_asset(name="microbatch_asset")
        batch_request = data_asset.build_batch_request(dataframe=df)
        
        # Create an expectation suite
        suite_name = "transaction_microbatch_suite"
        suite = context.add_expectation_suite(suite_name)
        
        # Get validator
        validator = context.get_validator(
            batch_request=batch_request,
            expectation_suite_name=suite_name
        )
        
        # Add Expectations (a subset of the 12-point checks for batch profiling)
        validator.expect_column_values_to_not_be_null("transaction_id")
        validator.expect_column_values_to_not_be_null("user_id")
        validator.expect_column_values_to_not_be_null("amount")
        validator.expect_column_values_to_be_between("amount", min_value=0.01)
        validator.expect_column_values_to_be_in_set("currency", VALID_CURRENCIES)
        validator.expect_column_values_to_be_in_set("merchant_category", VALID_CATEGORIES)
        validator.expect_column_value_lengths_to_equal("location", 2)
        
        results = validator.validate()
        
        success = results.success
        metrics = results.statistics
        log.info("📊 GX Validation complete - Success: %s | Evaluated expectations: %s | Success percent: %s%%", 
                 success, metrics.get("evaluated_expectations"), metrics.get("success_percent"))
                 
    except Exception as e:
        log.error("Failed to run Great Expectations on microbatch: %s", e)

def process_microbatch(raw_df, epoch_id):
    """
    Called for each micro-batch.
    1. Parse JSON
    2. Run Great Expectations profiling
    3. Apply row-level validation (PySpark rules)
    4. Write valid records to clean topic
    5. Write invalid records to DLQ topic
    """
    # ── 1. Deserialise JSON ──────────────────────────────────
    parsed = (
        raw_df
        .select(
            F.col("timestamp").alias("kafka_ingest_time"),
            F.from_json(F.col("value").cast("string"), TRANSACTION_SCHEMA).alias("d"),
        )
        .select("kafka_ingest_time", "d.*")
    )
    
    # Caching the parsed DataFrame since we'll use it for GX and multiple Kafka writes
    parsed.cache()
    count = parsed.count()
    if count == 0:
        parsed.unpersist()
        return
        
    log.info("📦 Processing microbatch %s with %s records...", epoch_id, count)

    # ── 2. Great Expectations Profiling ──────────────────────
    run_great_expectations_validation(parsed)

    # ── 3. Row-Level Quality Checks & Routing (PySpark) ──────
    null_checks = [
        F.when(F.col(c).isNull(), F.lit(f"MISSING:{c}")).otherwise(F.lit(None))
        for c in ["transaction_id", "user_id", "amount", "currency",
                  "timestamp", "merchant_name", "merchant_category", "location"]
    ]

    rule_checks = [
        F.when(F.col("amount") <= 0, F.lit("RULE:amount_must_be_positive")).otherwise(F.lit(None)),
        F.when(~F.col("currency").isin(VALID_CURRENCIES), F.lit(f"RULE:invalid_currency")).otherwise(F.lit(None)),
        F.when(~F.col("merchant_category").isin(VALID_CATEGORIES), F.lit(f"RULE:invalid_merchant_category")).otherwise(F.lit(None)),
        F.when(F.length(F.col("location")) != 2, F.lit("RULE:location_must_be_2char_iso")).otherwise(F.lit(None)),
    ]

    all_checks = null_checks + rule_checks

    enriched = (
        parsed
        .withColumn("_issues", F.array_compact(F.array(*all_checks)))
        .withColumn("_issue_count", F.size("_issues"))
        .withColumn("_is_valid", F.col("_issue_count") == 0)
        .withColumn("_suspect_fraud", F.col("amount") > FRAUD_THRESHOLD)
    )

    clean_df = enriched.filter(F.col("_is_valid") & ~F.col("_suspect_fraud"))
    dlq_df   = enriched.filter(~F.col("_is_valid") | F.col("_suspect_fraud"))
    
    dlq_enriched = dlq_df.withColumn(
        "dlq_reason",
        F.when(~F.col("_is_valid"), F.col("_issues")).otherwise(F.array(F.lit("FRAUD:amount_exceeds_threshold")))
    )

    clean_payload = _to_kafka_payload(clean_df)
    dlq_payload = _to_kafka_payload(dlq_enriched, extra_cols=["dlq_reason"])

    # ── 4. Write to clean_transactions ───────────────────────
    if clean_payload.count() > 0:
        (
            clean_payload.write
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_SERVERS)
            .option("topic", CLEAN_TOPIC)
            .save()
        )
        
    # ── 5. Write to dead_letter_queue ────────────────────────
    if dlq_payload.count() > 0:
        (
            dlq_payload.write
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_SERVERS)
            .option("topic", DLQ_TOPIC)
            .save()
        )
        
    # Log summary
    valid_count = clean_df.count()
    dlq_count = dlq_df.count()
    log.info("✅ Microbatch %s routing complete: %s clean | %s DLQ", epoch_id, valid_count, dlq_count)
    
    parsed.unpersist()


def main():
    log.info("=" * 60)
    log.info("  FinTech Fraud Detection — Streaming Job v0.3.0")
    log.info("  Kafka  : %s", KAFKA_SERVERS)
    log.info("  Topics : %s → %s | %s", RAW_TOPIC, CLEAN_TOPIC, DLQ_TOPIC)
    log.info("=" * 60)

    spark = build_spark_session()
    raw_stream = read_kafka_stream(spark)

    # Use foreachBatch to process each micro-batch dataframe
    query = (
        raw_stream.writeStream
        .foreachBatch(process_microbatch)
        .outputMode("update")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/main_query")
        .trigger(processingTime="15 seconds")
        .start()
    )

    log.info("✅ Streaming query running: id=%s", query.id)
    query.awaitTermination()

if __name__ == "__main__":
    main()
