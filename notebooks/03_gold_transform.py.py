# Databricks notebook source
import sys

PROJECT_ROOT = "/Workspace/real-time-fraud-analytics-platform"

if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from pipelines.gold.gold_transformer import GoldTransformer


CATALOG = "fraud_platform"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

transformer = GoldTransformer(
    spark=spark,
    catalog=CATALOG,
    silver_schema=SILVER_SCHEMA,
    gold_schema=GOLD_SCHEMA,
)

# -----------------------------------------
# Read Silver tables
# -----------------------------------------

transactions_df = transformer.read_silver_table("transactions")
cards_df = transformer.read_silver_table("cards")
users_expanded_df = transformer.read_silver_table("users_expanded")

# -----------------------------------------
# Build and write Gold tables
# -----------------------------------------

print("Building gold.customer_card_profile")

customer_card_profile_df = transformer.build_customer_card_profile(
    users_df=users_expanded_df,
    cards_df=cards_df,
)

transformer.write_gold_table(
    customer_card_profile_df,
    "customer_card_profile",
    mode="overwrite",
)


print("Building gold.fraud_transactions")

fraud_transactions_df = transformer.build_fraud_transactions(
    transactions_df=transactions_df,
    cards_df=cards_df,
    users_df=users_expanded_df,
)

transformer.write_gold_table(
    fraud_transactions_df,
    "fraud_transactions",
    mode="overwrite",
)


print("Building gold.fraud_by_state")

fraud_by_state_df = transformer.build_fraud_by_state(
    transactions_df=transactions_df,
)

transformer.write_gold_table(
    fraud_by_state_df,
    "fraud_by_state",
    mode="overwrite",
)


print("Building gold.fraud_by_age_group")

fraud_by_age_group_df = transformer.build_fraud_by_age_group(
    transactions_df=transactions_df,
    users_df=users_expanded_df,
)

transformer.write_gold_table(
    fraud_by_age_group_df,
    "fraud_by_age_group",
    mode="overwrite",
)


print("Building gold.fraud_by_income_band")

fraud_by_income_band_df = transformer.build_fraud_by_income_band(
    transactions_df=transactions_df,
    users_df=users_expanded_df,
)

transformer.write_gold_table(
    fraud_by_income_band_df,
    "fraud_by_income_band",
    mode="overwrite",
)


print("Building gold.merchant_risk_profile")

merchant_risk_profile_df = transformer.build_merchant_risk_profile(
    transactions_df=transactions_df,
)

transformer.write_gold_table(
    merchant_risk_profile_df,
    "merchant_risk_profile",
    mode="overwrite",
)

print("\nGold transformation completed successfully.")


# Validation Queries:
gold_tables = [
    "customer_card_profile",
    "fraud_transactions",
    "fraud_by_state",
    "fraud_by_age_group",
    "fraud_by_income_band",
    "merchant_risk_profile",
]

for table_name in gold_tables:
    full_table_name = f"{CATALOG}.{GOLD_SCHEMA}.{table_name}"

    print(f"\nValidating {full_table_name}")
    spark.sql(f"SELECT COUNT(*) AS row_count FROM {full_table_name}").show()
    spark.sql(f"SELECT * FROM {full_table_name} LIMIT 5").show(truncate=False)
