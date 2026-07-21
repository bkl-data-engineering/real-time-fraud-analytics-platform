# Databricks notebook source
import sys

PROJECT_ROOT = (
    "/Workspace/real-time-fraud-analytics-platform"
)

if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from pipelines.gold.streaming_gold_transformer import (
    StreamingGoldTransformer,
)


transformer = StreamingGoldTransformer(
    spark=spark,
    checkpoint_path=(
        "/Volumes/fraud_platform/gold/"
        "micro_batches/checkpoints/"
        "streaming_fraud_transactions_v1"
    ),
)

query = transformer.run()

query.awaitTermination()

print("Streaming Gold processing completed")
print(
    "Transaction table: "
    f"{transformer.transaction_gold_table}"
)
print(
    "Merchant table: "
    f"{transformer.merchant_gold_table}"
)