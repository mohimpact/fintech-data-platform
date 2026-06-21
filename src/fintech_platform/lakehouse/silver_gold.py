"""Batch Bronze-to-Silver-to-Gold transformations."""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from fintech_platform.config import LakehouseConfig
from fintech_platform.lakehouse.spark import build_iceberg_spark_session


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
log = logging.getLogger("fintech_platform.lakehouse.silver_gold")


def process_silver(spark: SparkSession, catalog: str) -> bool:
    """Create a deduplicated Silver table from Bronze transactions."""
    bronze_table = f"{catalog}.bronze.transactions"
    silver_table = f"{catalog}.silver.transactions"

    try:
        bronze_df = spark.table(bronze_table)
    except Exception as exc:
        log.warning("Skipping Silver: could not read %s: %s", bronze_table, exc)
        return False

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.silver")
    silver_df = (
        bronze_df.dropDuplicates(["transaction_id"])
        .withColumn("event_time", F.to_timestamp("timestamp"))
        .withColumn("date", F.to_date("event_time"))
        .filter(F.col("date").isNotNull())
    )

    log.info("Writing Silver table %s", silver_table)
    (
        silver_df.write.format("iceberg")
        .mode("overwrite")
        .option("overwrite-mode", "dynamic")
        .partitionBy("date")
        .saveAsTable(silver_table)
    )
    return True


def process_gold(spark: SparkSession, catalog: str) -> bool:
    """Create Gold merchant-category metrics for dashboard consumption."""
    silver_table = f"{catalog}.silver.transactions"
    gold_table = f"{catalog}.gold.merchant_metrics"

    try:
        silver_df = spark.table(silver_table)
    except Exception as exc:
        log.warning("Skipping Gold: could not read %s: %s", silver_table, exc)
        return False

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.gold")
    gold_df = silver_df.groupBy("date", "merchant_category").agg(
        F.sum("amount").alias("total_volume"),
        F.count("transaction_id").alias("transaction_count"),
        F.avg("amount").alias("average_transaction_size"),
        F.sum(F.when(F.col("is_fraud_suspect"), 1).otherwise(0)).alias("fraud_count"),
    )

    log.info("Writing Gold table %s", gold_table)
    (
        gold_df.write.format("iceberg")
        .mode("overwrite")
        .option("overwrite-mode", "dynamic")
        .partitionBy("date")
        .saveAsTable(gold_table)
    )
    return True


def main() -> None:
    lakehouse = LakehouseConfig()
    spark = build_iceberg_spark_session("fintech-lakehouse-silver-gold", lakehouse)
    if process_silver(spark, lakehouse.catalog_name):
        process_gold(spark, lakehouse.catalog_name)
    spark.stop()


if __name__ == "__main__":
    main()
