from delta.tables import DeltaTable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    countDistinct,
    current_timestamp,
    lit,
    lower,
    max as spark_max,
    round,
    sum as spark_sum,
    trim,
    when,
)


class StreamingGoldTransformer:
    """
    Builds transaction-level and merchant-level Gold tables from
    Silver streaming transactions.

    Gold outputs:
    1. streaming_fraud_transactions
       Enriched transaction-level fraud analytics.

    2. streaming_merchant_risk
       Merchant-level fraud and transaction-risk metrics.

    Transaction Gold uses an idempotent Delta MERGE based on
    _record_hash.

    Merchant Gold is recalculated from the complete transaction Gold
    table after each processed micro-batch.
    """

    def __init__(
        self,
        spark: SparkSession,
        catalog: str = "fraud_platform",
        silver_schema: str = "silver",
        gold_schema: str = "gold",
        source_table: str = "streaming_transactions",
        users_table: str = "users_expanded",
        cards_table: str = "cards",
        transaction_gold_table: str = "streaming_fraud_transactions",
        merchant_gold_table: str = "streaming_merchant_risk",
        checkpoint_path: str = (
            "/Volumes/fraud_platform/gold/"
            "micro_batches/checkpoints/"
            "streaming_fraud_transactions_v1"
        ),
    ) -> None:
        self.spark = spark

        self.source_table = (
            f"{catalog}.{silver_schema}.{source_table}"
        )

        self.users_table = (
            f"{catalog}.{silver_schema}.{users_table}"
        )

        self.cards_table = (
            f"{catalog}.{silver_schema}.{cards_table}"
        )

        self.transaction_gold_table = (
            f"{catalog}.{gold_schema}.{transaction_gold_table}"
        )

        self.merchant_gold_table = (
            f"{catalog}.{gold_schema}.{merchant_gold_table}"
        )

        self.checkpoint_path = checkpoint_path

    def read_stream(self) -> DataFrame:
        """
        Reads the Silver transaction table as a streaming DataFrame.
        """
        return (
            self.spark.readStream
            .format("delta")
            .table(self.source_table)
        )

    def read_users(self) -> DataFrame:
        """
        Selects customer attributes required for Gold enrichment.
        """
        return (
            self.spark.table(self.users_table)
            .select(
                "user_id",
                "person_name",
                "customer_age",
                "gender",
                col("city").alias("customer_city"),
                col("state").alias("customer_state"),
                col("zipcode").alias("customer_zipcode"),
                "per_capita_income",
                "yearly_income",
                "total_debt",
                "credit_score",
                "num_credit_cards",
            )
            .dropDuplicates(["user_id"])
        )

    def read_cards(self) -> DataFrame:
        """
        Selects non-sensitive card attributes required for enrichment.

        card_number and cvv are intentionally excluded.
        """
        return (
            self.spark.table(self.cards_table)
            .select(
                "user_id",
                "card_index",
                "card_brand",
                "card_type",
                "expires",
                "has_chip",
                "cards_issued",
                "credit_limit",
                "acct_open_date_parsed",
                "year_pin_last_changed",
                "card_on_dark_web_bool",
            )
            .dropDuplicates(
                [
                    "user_id",
                    "card_index",
                ]
            )
        )

    def enrich_transactions(
        self,
        transactions_df: DataFrame,
    ) -> DataFrame:
        """
        Joins transactions with customer and card reference data.
        """
        transactions_df = transactions_df.alias("t")
        users_df = self.read_users().alias("u")
        cards_df = self.read_cards().alias("c")

        return (
            transactions_df
            .join(
                users_df,
                col("t.user_id") == col("u.user_id"),
                "left",
            )
            .join(
                cards_df,
                (
                    col("t.user_id")
                    == col("c.user_id")
                )
                & (
                    col("t.card_index")
                    == col("c.card_index")
                ),
                "left",
            )
            .select(
                col("t.*"),
                col("u.person_name"),
                col("u.customer_age"),
                col("u.gender"),
                col("u.customer_city"),
                col("u.customer_state"),
                col("u.customer_zipcode"),
                col("u.per_capita_income"),
                col("u.yearly_income"),
                col("u.total_debt"),
                col("u.credit_score"),
                col("u.num_credit_cards"),
                col("c.card_brand"),
                col("c.card_type"),
                col("c.expires"),
                col("c.has_chip"),
                col("c.cards_issued"),
                col("c.credit_limit"),
                col("c.acct_open_date_parsed"),
                col("c.year_pin_last_changed"),
                col("c.card_on_dark_web_bool"),
            )
        )

    def add_customer_segments(
        self,
        df: DataFrame,
    ) -> DataFrame:
        """
        Adds age and income segments.
        """
        return (
            df
            .withColumn(
                "age_group",
                when(
                    col("customer_age").isNull(),
                    "UNKNOWN",
                )
                .when(
                    col("customer_age") < 25,
                    "UNDER_25",
                )
                .when(
                    col("customer_age") < 40,
                    "25_TO_39",
                )
                .when(
                    col("customer_age") < 55,
                    "40_TO_54",
                )
                .when(
                    col("customer_age") < 65,
                    "55_TO_64",
                )
                .otherwise("65_PLUS"),
            )
            .withColumn(
                "income_band",
                when(
                    col("yearly_income").isNull(),
                    "UNKNOWN",
                )
                .when(
                    col("yearly_income") < 30_000,
                    "LOW",
                )
                .when(
                    col("yearly_income") < 75_000,
                    "MEDIUM",
                )
                .when(
                    col("yearly_income") < 150_000,
                    "HIGH",
                )
                .otherwise("VERY_HIGH"),
            )
        )

    def add_risk_signals(
        self,
        df: DataFrame,
    ) -> DataFrame:
        """
        Adds explainable fraud and transaction-risk signals.

        risk_signal_count represents the number of heuristic signals
        triggered. It is not intended to represent a calibrated model
        score.
        """
        normalized_fraud = lower(
            trim(col("is_fraud"))
        )

        return (
            df
            .withColumn(
                "is_fraud_bool",
                when(
                    normalized_fraud.isin(
                        "yes",
                        "true",
                        "1",
                        "fraud",
                    ),
                    True,
                ).otherwise(False),
            )
            .withColumn(
                "is_out_of_state",
                when(
                    col("merchant_state").isNull()
                    | col("customer_state").isNull(),
                    False,
                )
                .when(
                    lower(trim(col("merchant_state")))
                    != lower(trim(col("customer_state"))),
                    True,
                )
                .otherwise(False),
            )
            .withColumn(
                "is_high_amount",
                when(
                    col("amount") >= 500,
                    True,
                ).otherwise(False),
            )
            .withColumn(
                "transaction_to_credit_limit_ratio",
                when(
                    col("credit_limit").isNotNull()
                    & (col("credit_limit") > 0),
                    round(
                        col("amount")
                        / col("credit_limit"),
                        4,
                    ),
                ),
            )
            .withColumn(
                "is_high_credit_utilization_transaction",
                when(
                    col(
                        "transaction_to_credit_limit_ratio"
                    ) >= 0.50,
                    True,
                ).otherwise(False),
            )
            .withColumn(
                "has_transaction_error",
                when(
                    col("errors").isNotNull()
                    & (trim(col("errors")) != ""),
                    True,
                ).otherwise(False),
            )
            .withColumn(
                "risk_signal_count",
                (
                    col("is_out_of_state").cast("int")
                    + col("is_high_amount").cast("int")
                    + col(
                        "is_high_credit_utilization_transaction"
                    ).cast("int")
                    + col(
                        "has_transaction_error"
                    ).cast("int")
                    + when(
                        col("card_on_dark_web_bool") == True,
                        lit(1),
                    ).otherwise(lit(0))
                ),
            )
            .withColumn(
                "_gold_processed_at",
                current_timestamp(),
            )
        )

    def transform_transactions(
        self,
        transactions_df: DataFrame,
    ) -> DataFrame:
        """
        Executes transaction-level Gold transformations.
        """
        enriched_df = self.enrich_transactions(
            transactions_df
        )

        segmented_df = self.add_customer_segments(
            enriched_df
        )

        return self.add_risk_signals(
            segmented_df
        )

    def merge_transaction_gold(
        self,
        transformed_df: DataFrame,
    ) -> None:
        """
        Creates or merges records into the transaction Gold table.
        """
        transformed_df = (
            transformed_df
            .filter(col("_record_hash").isNotNull())
            .dropDuplicates(["_record_hash"])
        )

        if not self.spark.catalog.tableExists(
            self.transaction_gold_table
        ):
            (
                transformed_df.write
                .format("delta")
                .mode("overwrite")
                .saveAsTable(
                    self.transaction_gold_table
                )
            )

            return

        target_table = DeltaTable.forName(
            self.spark,
            self.transaction_gold_table,
        )

        (
            target_table.alias("target")
            .merge(
                transformed_df.alias("source"),
                (
                    "target._record_hash "
                    "= source._record_hash"
                ),
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    def build_merchant_risk(
        self,
    ) -> DataFrame:
        """
        Builds merchant-level risk analytics from the complete
        transaction Gold table.

        The grouping represents a merchant location using:
        - merchant name
        - merchant city
        - merchant state
        - MCC
        """
        transaction_gold_df = self.spark.table(
            self.transaction_gold_table
        )

        merchant_df = (
            transaction_gold_df
            .groupBy(
                "merchant_name",
                "merchant_city",
                "merchant_state",
                "mcc",
            )
            .agg(
                count("*").alias(
                    "transaction_count"
                ),
                countDistinct("user_id").alias(
                    "distinct_customer_count"
                ),
                countDistinct(
                    "user_id",
                    "card_index",
                ).alias(
                    "distinct_cards_used"
                ),
                spark_sum(
                    when(
                        col("is_fraud_bool") == True,
                        1,
                    ).otherwise(0)
                ).alias(
                    "fraud_transaction_count"
                ),
                round(
                    avg("amount"),
                    2,
                ).alias(
                    "average_transaction_amount"
                ),
                spark_max("amount").alias(
                    "maximum_transaction_amount"
                ),
                round(
                    spark_sum("amount"),
                    2,
                ).alias(
                    "total_transaction_amount"
                ),
                round(
                    avg("risk_signal_count"),
                    4,
                ).alias(
                    "average_risk_signal_count"
                ),
                spark_sum(
                    when(
                        col("risk_signal_count") >= 2,
                        1,
                    ).otherwise(0)
                ).alias(
                    "high_risk_transaction_count"
                ),
                spark_sum(
                    when(
                        col("risk_signal_count") >= 1,
                        1,
                    ).otherwise(0)
                ).alias(
                    "flagged_transaction_count"
                ),
                spark_max(
                    "risk_signal_count"
                ).alias(
                    "maximum_risk_signal_count"
                ),
            )
            .withColumn(
                "fraud_rate_pct",
                round(
                    (
                        col(
                            "fraud_transaction_count"
                        )
                        / col("transaction_count")
                    ) * 100,
                    4,
                ),
            )
            .withColumn(
                "flagged_transaction_rate_pct",
                round(
                    (
                        col(
                            "flagged_transaction_count"
                        )
                        / col("transaction_count")
                    ) * 100,
                    4,
                ),
            )
            .withColumn(
                "_gold_processed_at",
                current_timestamp(),
            )
        )

        return merchant_df

    def write_merchant_risk(
        self,
    ) -> None:
        """
        Recalculates and overwrites the complete merchant Gold mart.

        Since the source transaction Gold table already contains the
        complete idempotent transaction history, a full overwrite keeps
        merchant metrics accurate and avoids complex incremental
        aggregation logic.
        """
        merchant_df = self.build_merchant_risk()

        (
            merchant_df.write
            .format("delta")
            .mode("overwrite")
            .option(
                "overwriteSchema",
                "true",
            )
            .saveAsTable(
                self.merchant_gold_table
            )
        )

    def process_batch(
        self,
        batch_df: DataFrame,
        batch_id: int,
    ) -> None:
        """
        Processes one Silver micro-batch.

        Steps:
        1. Enrich and derive transaction-level risk signals.
        2. MERGE transactions into transaction Gold.
        3. Rebuild merchant-level Gold analytics.
        """
        if batch_df.isEmpty():
            return

        transformed_df = self.transform_transactions(
            batch_df
        )

        self.merge_transaction_gold(
            transformed_df
        )

        self.write_merchant_risk()

        print(
            f"Completed Gold processing for batch {batch_id}"
        )

    def write_stream(
        self,
        streaming_df: DataFrame,
    ):
        """
        Processes all currently available Silver records and stops.
        """
        return (
            streaming_df.writeStream
            .foreachBatch(self.process_batch)
            .option(
                "checkpointLocation",
                self.checkpoint_path,
            )
            .trigger(availableNow=True)
            .start()
        )

    def run(self):
        """
        Runs the Silver-to-Gold streaming pipeline.
        """
        silver_stream_df = self.read_stream()

        return self.write_stream(
            silver_stream_df
        )
