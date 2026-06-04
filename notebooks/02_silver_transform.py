# Databricks notebook source
import sys

PROJECT_ROOT = "/Workspace/real-time-fraud-analytics-platform"

if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from pipelines.silver.silver_transformer import SilverTransformer


CATALOG = "fraud_platform"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

transformer = SilverTransformer(
    spark=spark,
    catalog=CATALOG,
    bronze_schema=BRONZE_SCHEMA,
    silver_schema=SILVER_SCHEMA,
)

tables_to_transform = [
    {
        "source_table_name": "transactions",
        "target_table_name": "transactions",
        "transform_type": "transactions",
    },
    {
        "source_table_name": "cards",
        "target_table_name": "cards",
        "transform_type": "cards",
    },
    {
        "source_table_name": "users",
        "target_table_name": "users",
        "transform_type": "users",
    },
]

for item in tables_to_transform:
    print(
        f"Transforming {CATALOG}.{BRONZE_SCHEMA}.{item['source_table_name']} "
        f"into {CATALOG}.{SILVER_SCHEMA}.{item['target_table_name']}"
    )

    transformer.transform_and_write_table(
        source_table_name=item["source_table_name"],
        target_table_name=item["target_table_name"],
        transform_type=item["transform_type"],
        mode="overwrite",
    )

print("\nSilver transformation completed successfully.")

# -----------------------------------------
# Build users_expanded
# -----------------------------------------

silver_users_df = spark.table(
    f"{CATALOG}.silver.users"
)

users_expanded_df = transformer.transform_users_expanded(
    silver_users_df
)

transformer.write_silver_table(
    users_expanded_df,
    "users_expanded"
)

print("Created silver.users_expanded")