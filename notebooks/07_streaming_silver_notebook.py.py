# Databricks notebook source
import sys

PROJECT_ROOT = "/Workspace/real-time-fraud-analytics-platform"

if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from pipelines.silver.silver_streaming_transformer import (
    StreamingSilverTransformer,
)

CATALOG = "fraud_platform"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

CHECKPOINT_PATH = (
    "/Volumes/fraud_platform/silver/"
    "micro_batches/checkpoints/streaming_transactions"
)

transformer = StreamingSilverTransformer(
    spark=spark,
    catalog=CATALOG,
    bronze_schema=BRONZE_SCHEMA,
    silver_schema=SILVER_SCHEMA,
    checkpoint_path=CHECKPOINT_PATH,
)

query = transformer.run()

print("Streaming Silver transformation started successfully")
print(f"Query ID: {query.id}")
print(f"Target table: {transformer.target_table}")
print(query.status)