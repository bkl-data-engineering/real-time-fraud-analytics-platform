from functools import reduce
from typing import List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
    when,
)


class DQValidator:
    """
    Reusable Data Quality validation helper.

    Responsibilities:
    - Read curated Silver tables
    - Run reusable validation checks
    - Persist DQ results to a Gold audit table
    """

    def __init__(
        self,
        spark: SparkSession,
        catalog: str,
        silver_schema: str = "silver",
        gold_schema: str = "gold",
    ) -> None:
        self.spark = spark
        self.catalog = catalog
        self.silver_schema = silver_schema
        self.gold_schema = gold_schema

    def read_silver_table(self, table_name: str) -> DataFrame:
        full_table_name = f"{self.catalog}.{self.silver_schema}.{table_name}"
        return self.spark.read.table(full_table_name)

    def write_dq_results(
        self,
        df: DataFrame,
        table_name: str = "dq_summary",
        mode: str = "append",
    ) -> None:
        full_table_name = f"{self.catalog}.{self.gold_schema}.{table_name}"

        (
            df.write
            .format("delta")
            .mode(mode)
            .option("mergeSchema", "true")
            .saveAsTable(full_table_name)
        )

    def run_validation_check(
        self,
        df: DataFrame,
        table_name: str,
        check_name: str,
        failure_condition,
    ) -> DataFrame:
        total_records = df.count()
        failed_records = df.filter(failure_condition).count()
        passed_records = total_records - failed_records

        status = "PASS" if failed_records == 0 else "FAIL"

        return self.spark.createDataFrame(
            [
                (
                    table_name,
                    check_name,
                    total_records,
                    passed_records,
                    failed_records,
                    status,
                )
            ],
            [
                "table_name",
                "check_name",
                "total_records",
                "passed_records",
                "failed_records",
                "status",
            ],
        ).withColumn("run_timestamp", current_timestamp())

    def combine_results(self, results: List[DataFrame]) -> DataFrame:
        return reduce(DataFrame.unionByName, results)

    def validate_transactions(self) -> DataFrame:
        transactions_df = self.read_silver_table("transactions")

        results = [
            self.run_validation_check(
                df=transactions_df,
                table_name="transactions",
                check_name="amount_not_null",
                failure_condition=col("amount").isNull(),
            ),
            self.run_validation_check(
                df=transactions_df,
                table_name="transactions",
                check_name="user_id_not_null",
                failure_condition=col("user_id").isNull(),
            ),
            self.run_validation_check(
                df=transactions_df,
                table_name="transactions",
                check_name="card_index_not_null",
                failure_condition=col("card_index").isNull(),
            ),
            self.run_validation_check(
                df=transactions_df,
                table_name="transactions",
                check_name="transaction_date_not_null",
                failure_condition=col("transaction_date").isNull(),
            ),
            self.run_validation_check(
                df=transactions_df,
                table_name="transactions",
                check_name="is_fraud_not_null",
                failure_condition=col("is_fraud").isNull(),
            ),
        ]

        return self.combine_results(results)

    def validate_cards(self) -> DataFrame:
        cards_df = self.read_silver_table("cards")

        results = [
            self.run_validation_check(
                df=cards_df,
                table_name="cards",
                check_name="user_id_not_null",
                failure_condition=col("user_id").isNull(),
            ),
            self.run_validation_check(
                df=cards_df,
                table_name="cards",
                check_name="card_index_not_null",
                failure_condition=col("card_index").isNull(),
            ),
            self.run_validation_check(
                df=cards_df,
                table_name="cards",
                check_name="credit_limit_non_negative",
                failure_condition=col("credit_limit") < 0,
            ),
        ]

        return self.combine_results(results)

    def validate_users_expanded(self) -> DataFrame:
        users_df = self.read_silver_table("users_expanded")

        results = [
            self.run_validation_check(
                df=users_df,
                table_name="users_expanded",
                check_name="user_id_not_null",
                failure_condition=col("user_id").isNull(),
            ),
            self.run_validation_check(
                df=users_df,
                table_name="users_expanded",
                check_name="current_age_valid_range",
                failure_condition=(
                    col("current_age").isNull()
                    | (col("current_age") < 18)
                    | (col("current_age") > 120)
                ),
            ),
            self.run_validation_check(
                df=users_df,
                table_name="users_expanded",
                check_name="credit_score_valid_range",
                failure_condition=(
                    col("credit_score").isNull()
                    | (col("credit_score") < 300)
                    | (col("credit_score") > 850)
                ),
            ),
            self.run_validation_check(
                df=users_df,
                table_name="users_expanded",
                check_name="yearly_income_non_negative",
                failure_condition=col("yearly_income") < 0,
            ),
            self.run_validation_check(
                df=users_df,
                table_name="users_expanded",
                check_name="total_debt_non_negative",
                failure_condition=col("total_debt") < 0,
            ),
        ]

        return self.combine_results(results)
    
    def run_referrential_integrity_check(
        self,
        source_df: DataFrame,
        reference_df: DataFrame,
        table_name: str,
        check_name: str,
        join_columns: List[str],
    ) -> DataFrame:
        total_records = source_df.count()
        failed_records = (
            source_df.alias("src")
            .join(
                reference_df.alias("ref"),
                on=join_columns,
                how="left_anti",
            )
            .count()
        )
        passed_records = total_records - failed_records
        status = "PASS" if failed_records == 0 else "FAIL"
        return (
            self.spark.createDataFrame(
                [
                    (
                        table_name,
                        check_name,
                        total_records,
                        passed_records,
                        failed_records,
                        status
                    )
                ],
                [
                    "table_name",
                    "check_name",
                    "total_records",
                    "passed_records",
                    "failed_records",
                    "status",
                ]
            ).withColumn("run_timestamp", current_timestamp())
    )

    def validate_referential_integrity(self) -> DataFrame:
        transactions_df = self.read_silver_table("transactions")
        users_df = self.read_silver_table("users_expanded")
        cards_df = self.read_silver_table("cards")

        user_keys_df = users_df.select("user_Id").dropDuplicates()
        card_keys_df = cards_df.select("card_index", "user_id").dropDuplicates()

        results = [
            self.run_referrential_integrity_check(
                source_df=transactions_df.select("user_id"),
                reference_df = user_keys_df,
                table_name = "transactions",
                check_name="transactions_user_id_exists_in_users",
                join_columns=["user_id"]
            ),
            self.run_referrential_integrity_check(
                source_df=transactions_df.select("user_id", "card_index"),
                reference_df=card_keys_df,
                table_name="transactions",
                check_name="transactions_card_exists_in_cards",
                join_columns=["user_id", "card_index"]  
            ),
            self.run_referrential_integrity_check(
                source_df = cards_df.select("user_id"),
                reference_df = user_keys_df,
                table_name = "cards",
                check_name="cards_user_id_exists_in_users",
                join_columns=["user_id"]
            )
        ]

        return self.combine_results(results)
    

    def run_all_validations(self) -> DataFrame:
        results = [
            self.validate_transactions(),
            self.validate_cards(),
            self.validate_users_expanded(),
            self.validate_referential_integrity()
        ]

        return self.combine_results(results)
