"""
Phase 3: Lakehouse - Bronze Ingestion
====================================================
Continuously reads from the `clean_transactions` Kafka topic
and appends the JSON payloads directly into an Apache Iceberg table
(local.bronze.transactions).
"""

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")
log = logging.getLogger("FinTechLakehouse-Bronze")

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CLEAN_TOPIC   = "clean_transactions"
CHECKPOINT    = "/app/checkpoints/bronze_ingestion"

# Credentials and Endpoint for S3/MinIO
AWS_ACCESS_KEY        = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_KEY        = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
S3_ENDPOINT           = os.getenv("S3_ENDPOINT", "http://localhost:9000") # Use "" or "https://s3.amazonaws.com" for real AWS
WAREHOUSE_PATH        = os.getenv("WAREHOUSE_PATH", "s3a://warehouse/")

# Required JARs for Iceberg and S3
SPARK_PACKAGES = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
])

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

def build_spark_session() -> SparkSession:
    log.info("Building SparkSession for Iceberg Bronze Ingestion...")
    builder = (
        SparkSession.builder
        .appName("FinTech-Lakehouse-Bronze")
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

def main():
    spark = build_spark_session()
    
    log.info("Creating Bronze database/namespace if not exists...")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.bronze")
    
    # We will let Iceberg create the table automatically or we can define it:
    log.info("Creating Iceberg table local.bronze.transactions if not exists...")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.bronze.transactions (
            transaction_id STRING,
            user_id INT,
            amount DOUBLE,
            currency STRING,
            timestamp STRING,
            merchant_name STRING,
            merchant_category STRING,
            location STRING,
            kafka_ingest_time TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (currency)
    """)

    log.info("Subscribing to Kafka topic '%s'", CLEAN_TOPIC)
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", CLEAN_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed_stream = (
        raw_stream
        .select(
            F.col("timestamp").alias("kafka_ingest_time"),
            F.from_json(F.col("value").cast("string"), TRANSACTION_SCHEMA).alias("d")
        )
        .select("d.*", "kafka_ingest_time")
    )

    log.info("Starting stream to Iceberg local.bronze.transactions...")
    query = (
        parsed_stream.writeStream
        .format("iceberg")
        .outputMode("append")
        .trigger(processingTime="1 minute")
        .option("path", "local.bronze.transactions")
        .option("checkpointLocation", CHECKPOINT)
        .start()
    )

    query.awaitTermination()

if __name__ == "__main__":
    main()
