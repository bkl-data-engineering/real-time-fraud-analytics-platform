# Databricks notebook source
import sys
from uuid import uuid4

PROJECT_ROOT = "/Workspace/real-time-fraud-analytics-platform"

if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from pipelines.bronze.bronze_loader import BronzeLoader

CATALOG = "fraud_platform"
BRONZE_SCHEMA = "bronze"
RAW_FILES_PATH = "/Volumes/fraud_platform/bronze/raw_files"

run_id = str(uuid4())

loader = BronzeLoader(
    spark=spark,
    catalog=CATALOG,
    schema=BRONZE_SCHEMA,
    run_id=run_id
)

files_to_load = [
    {
        "file_path": f"{RAW_FILES_PATH}/credit_card_transactions-ibm_v2.csv",
        "table_name": "transactions",
        "source_name": "credit_card_transactions-ibm_v2.csv"
    },
    {
        "file_path": f"{RAW_FILES_PATH}/sd254_cards.csv",
        "table_name": "cards",
        "source_name": "sd254_cards.csv"
    },
    {
        "file_path": f"{RAW_FILES_PATH}/sd254_users.csv",
        "table_name": "users",
        "source_name": "sd254_users.csv"
    },
    {
        "file_path": f"{RAW_FILES_PATH}//User0_credit_card_transactions.csv",
        "table_name": "user0_transactions",
        "source_name": "User0_credit_card_transactions.csv"
    },
]

for item in files_to_load:

    print(
        f"Loading {item['source_name']} "
        f"into {CATALOG}.{BRONZE_SCHEMA}.{item['table_name']}"
    )
    loader.load_csv_to_bronze(
        file_path = item["file_path"],
        table_name = item["table_name"],
        source_name = item["source_name"],
        load_type = "seed",
        mode = "overwrite",
        header = True,
        infer_schema = False
    )

print(f"\nBronze seed load completed successfully.")
print(f"run_id = {run_id}")   