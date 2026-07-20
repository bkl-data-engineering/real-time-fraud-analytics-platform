import sys
import importlib

PROJECT_ROOT = "/Workspace/real-time-fraud-analytics-platform"

if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

import jobs.transaction_stream_generator as transaction_stream_generator

importlib.reload(transaction_stream_generator)

from jobs.transaction_stream_generator import TransactionStreamGenerator

generator = TransactionStreamGenerator(
    spark=spark,
    source_table="fraud_platform.silver.transactions",
    output_path="/Volumes/fraud_platform/bronze/micro_batches/incoming/",
    batch_size=1000
)

generator.run(
    num_batches=2,
    interval_seconds=15
)