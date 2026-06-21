"""Spark session helpers for Iceberg over S3-compatible storage."""

from __future__ import annotations

from pyspark.sql import SparkSession

from fintech_platform.config import LakehouseConfig


ICEBERG_SPARK_PACKAGES = ",".join(
    [
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    ]
)


def build_iceberg_spark_session(
    app_name: str,
    config: LakehouseConfig | None = None,
    extra_packages: list[str] | None = None,
) -> SparkSession:
    lakehouse = config or LakehouseConfig()
    packages = ICEBERG_SPARK_PACKAGES
    if extra_packages:
        packages = ",".join([packages, *extra_packages])

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages", packages)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{lakehouse.catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{lakehouse.catalog_name}.type", "hadoop")
        .config(f"spark.sql.catalog.{lakehouse.catalog_name}.warehouse", lakehouse.warehouse_path)
        .config("spark.hadoop.fs.s3a.access.key", lakehouse.access_key)
        .config("spark.hadoop.fs.s3a.secret.key", lakehouse.secret_key)
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
        .config("spark.sql.shuffle.partitions", "3")
        .config("spark.ui.showConsoleProgress", "false")
    )

    if lakehouse.endpoint:
        ssl_enabled = "false" if lakehouse.endpoint.startswith("http://") else "true"
        builder = builder.config("spark.hadoop.fs.s3a.endpoint", lakehouse.endpoint).config(
            "spark.hadoop.fs.s3a.path.style.access", "true"
        ).config("spark.hadoop.fs.s3a.connection.ssl.enabled", ssl_enabled)
    else:
        builder = builder.config("spark.hadoop.fs.s3a.path.style.access", "false")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
