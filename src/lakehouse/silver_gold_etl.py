"""
Phase 3: Lakehouse - Silver & Gold ETL
====================================================
Batch job that runs periodically to:
  1. Read from Bronze (local.bronze.transactions)
  2. Clean/cast and write to Silver (local.silver.transactions)
  3. Aggregate metrics and write to Gold (local.gold.merchant_metrics)
"""

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")
log = logging.getLogger("FinTechLakehouse-Batch")

# Credentials and Endpoint for S3/MinIO
AWS_ACCESS_KEY        = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_KEY        = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
S3_ENDPOINT           = os.getenv("S3_ENDPOINT", "http://minio:9000")
WAREHOUSE_PATH        = os.getenv("WAREHOUSE_PATH", "s3a://warehouse/")

SPARK_PACKAGES = ",".join([
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
])

def build_spark_session() -> SparkSession:
    log.info("Building SparkSession for Lakehouse Batch...")
    builder = (
        SparkSession.builder
        .appName("FinTech-Lakehouse-Batch")
        .master("local[*]")
        .config("spark.jars.packages", SPARK_PACKAGES)
        
        # Iceberg configurations
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", WAREHOUSE_PATH)
        
        # S3 / MinIO configurations
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.path.style.access", "true" if S3_ENDPOINT else "false")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false" if "localhost" in S3_ENDPOINT else "true")
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
    )
    
    if S3_ENDPOINT:
        builder = builder.config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    return spark

def process_silver(spark: SparkSession):
    log.info("Processing Bronze -> Silver...")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.silver")
    
    # Read from Bronze
    try:
        bronze_df = spark.table("local.bronze.transactions")
    except Exception as e:
        log.warning("Could not read bronze table. Is it created yet? %s", e)
        return
    
    # Deduplicate and clean
    silver_df = (
        bronze_df
        .dropDuplicates(["transaction_id"])
        # Extract date from timestamp for partitioning
        .withColumn("date", F.to_date(F.col("timestamp")))
    )
    
    log.info("Writing to Silver table local.silver.transactions...")
    # Overwrite partitions dynamically so we can run this batch job repeatedly
    (
        silver_df.write
        .format("iceberg")
        .mode("overwrite")
        .option("overwrite-mode", "dynamic")
        .partitionBy("date")
        .saveAsTable("local.silver.transactions")
    )
    log.info("✅ Silver processing complete.")

def process_gold(spark: SparkSession):
    log.info("Processing Silver -> Gold...")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.gold")
    
    try:
        silver_df = spark.table("local.silver.transactions")
    except Exception as e:
        log.warning("Could not read silver table. %s", e)
        return
        
    # Aggregate: daily transaction volume, count, avg, and fraud count per merchant category
    gold_df = (
        silver_df
        .groupBy("date", "merchant_category")
        .agg(
            F.sum("amount").alias("total_volume"),
            F.count("transaction_id").alias("transaction_count"),
            F.avg("amount").alias("average_transaction_size"),
            F.sum(F.when(F.col("amount") > 5000, 1).otherwise(0)).alias("fraud_count")
        )
    )
    
    log.info("Writing to Gold table local.gold.merchant_metrics...")
    (
        gold_df.write
        .format("iceberg")
        .mode("overwrite")
        .option("overwrite-mode", "dynamic")
        .partitionBy("date")
        .saveAsTable("local.gold.merchant_metrics")
    )
    log.info("✅ Gold processing complete.")

def main():
    spark = build_spark_session()
    process_silver(spark)
    process_gold(spark)

if __name__ == "__main__":
    main()
