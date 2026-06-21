"""Schema definitions shared by Spark jobs."""

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


TRANSACTION_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("merchant_name", StringType(), True),
        StructField("merchant_category", StringType(), True),
        StructField("location", StringType(), True),
    ]
)

CLEAN_TRANSACTION_SCHEMA = StructType(
    TRANSACTION_SCHEMA.fields
    + [
        StructField("is_fraud_suspect", BooleanType(), True),
        StructField("fraud_reason", StringType(), True),
    ]
)

TRANSACTION_COLUMNS = [field.name for field in TRANSACTION_SCHEMA.fields]
CLEAN_TRANSACTION_COLUMNS = [field.name for field in CLEAN_TRANSACTION_SCHEMA.fields]
