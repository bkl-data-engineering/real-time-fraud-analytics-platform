# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

import sys
PROJECT_ROOT = "/Workspace/real-time-fraud-analytics-platform"
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

import importlib
import pipelines.bronze.streaming_bronze_loader as streaming_bronze_loader

importlib.reload(streaming_bronze_loader)
from pipelines.bronze.streaming_bronze_loader import streamingBronzeLoader

CATALOG = "fraud_platform"
SCHEMA = "bronze"

SOURCE_PATH = "/Volumes/fraud_platform/bronze/micro_batches/incoming"

CHECKPOINT_PATH = (
    "/Volumes/fraud_platform/bronze/micro_batches/checkpoints"
    "bronze_streaming_transactions"
)

SCHEMA_LOCATION = (
    "/Volumes/fraud_platform/bronze/micro_batches/checkpoints"
    "/schema/bronze_streaming_transactions"
)

TARGET_TABLE = f"{CATALOG}.{SCHEMA}.streaming_transactions"

loader = streamingBronzeLoader(
    spark=spark,
    catalog=CATALOG,
    schema=SCHEMA,
    source_path=SOURCE_PATH,
    checkpoint_path=CHECKPOINT_PATH,
    schema_location=SCHEMA_LOCATION
)
raw_stream_df = loader.read_stream()

bronze_stream_df = loader.add_metadata(raw_stream_df)
query = loader.write_stream(bronze_stream_df)

print("\nStreaming Bronze Loader Started Successfully")
print(f"Query ID: {query.id}")
print(f"Target Table: {TARGET_TABLE}")

print("\nCurrent Stream Status:")
print(query.status)