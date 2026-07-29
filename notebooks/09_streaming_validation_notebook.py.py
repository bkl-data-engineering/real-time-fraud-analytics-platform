# Databricks notebook source
from pyspark.sql.functions import col, count, sum as spark_sum


# ============================================================
# Configuration
# ============================================================

TRANSACTION_GOLD_TABLE = (
    "fraud_platform.gold.streaming_fraud_transactions"
)

MERCHANT_GOLD_TABLE = (
    "fraud_platform.gold.streaming_merchant_risk"
)


# ============================================================
# Validation Functions
# ============================================================

def validate_table_not_empty(table_name: str) -> int:
    """
    Validates that a table exists and contains records.

    Returns:
        Row count
    """

    record_count = spark.table(table_name).count()

    if record_count == 0:
        raise ValueError(
            f"{table_name} contains no records."
        )

    print(
        f"✓ {table_name}: "
        f"{record_count:,} records"
    )

    return record_count


def validate_duplicate_hashes() -> None:
    """
    Ensures transaction Gold is idempotent.
    """

    duplicate_count = (
        spark.table(TRANSACTION_GOLD_TABLE)
        .groupBy("_record_hash")
        .agg(count("*").alias("record_count"))
        .filter(col("record_count") > 1)
        .count()
    )

    if duplicate_count > 0:
        raise ValueError(
            f"Found {duplicate_count} duplicate "
            "_record_hash values."
        )

    print(
        "✓ No duplicate _record_hash values"
    )


def validate_merchant_reconciliation(
    transaction_count: int,
) -> None:
    """
    Confirms merchant aggregation reconciles
    back to transaction Gold.
    """

    merchant_transaction_total = (
        spark.table(MERCHANT_GOLD_TABLE)
        .agg(
            spark_sum("transaction_count").alias(
                "merchant_total"
            )
        )
        .first()["merchant_total"]
    )

    if merchant_transaction_total != transaction_count:
        raise ValueError(
            "Merchant totals do not reconcile "
            "with transaction Gold."
        )

    print(
        "✓ Merchant reconciliation passed"
    )


# ============================================================
# Execute Validations
# ============================================================

print("=" * 60)
print("STREAMING PIPELINE VALIDATION")
print("=" * 60)

transaction_count = validate_table_not_empty(
    TRANSACTION_GOLD_TABLE
)

merchant_count = validate_table_not_empty(
    MERCHANT_GOLD_TABLE
)

validate_duplicate_hashes()

validate_merchant_reconciliation(
    transaction_count
)

print()
print("=" * 60)
print("✓ STREAMING PIPELINE VALIDATION PASSED")
print("=" * 60)