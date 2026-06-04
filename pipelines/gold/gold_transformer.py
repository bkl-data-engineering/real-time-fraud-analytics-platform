from pyspark.sql import SparkSession, DataFrame

from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    sum as spark_sum,
    avg,
    when,
    lit,
)

class GoldTransformer:
    """
    Reusable Gold transformation helper.

    Responsibilities:
    - Read curated Silver tables
    - Build business-level Gold marts
    - Write Delta Gold tables
    """
    def __init__(
        self,
        spark: SparkSession,
        catalog: str,
        silver_schema: str = "silver",
        gold_schema: str = "gold"
    ) -> None:
        self.spark = spark
        self.catalog = catalog
        self.silver_schema = silver_schema
        self.gold_schema = gold_schema

    def read_silver_table(self, table_name: str) -> DataFrame:
        """
        Read a Silver table from the catalog.
        """
        full_table_name = f"{self.catalog}.{self.silver_schema}.{table_name}"
        return self.spark.read.table(full_table_name)
    
    def write_gold_table(
        self, 
        df: DataFrame, 
        table_name: str,
        mode: str = "overwrite"
        ) -> None:
        """
        Write a Gold table to the catalog.
        """
        full_table_name = f"{self.catalog}.{self.gold_schema}.{table_name}"
        (
            df
            .write
            .format("delta")
            .mode(mode)
            .option("overwriteSchema", "true")
            .saveAsTable(full_table_name)
        )
    
    def add_fraud_flag_int(self, transaction_df: DataFrame):
        return transaction_df.withColumn(
            "is_fraud_int",
            when(col("is_fraud") == True, lit(1)).otherwise(lit(0))
        )

    def build_customer_card_profile(
        self,
        users_df: DataFrame,
        cards_df: DataFrame
    ) ->  DataFrame:
        """
        Build a customer profile Gold table by joining users_expanded table and cards tables.
        """
        return (
            users_df.alias("u")
            .join(cards_df.alias("c"), on="user_id", how="left")
            .groupBy(
                col("u.user_id"),
                col("u.current_age"),
                col("u.birth_year"),
                col("u.gender"),
                col("u.address"),
                col("u.latitude"),
                col("u.longitude"),
                col("u.per_capita_income"),
                col("u.yearly_income"),
                col("u.total_debt"),
                col("u.credit_score"),
            )
            .agg(
                countDistinct(col("c.card_index")).alias("num_cards"),
                countDistinct(col("c.card_brand")).alias("num_card_brands"),
                countDistinct(col("c.card_type")).alias("num_card_types"),
            )
        )
    
    def build_fraud_transactions(
        self,
        transactions_df: DataFrame,
        cards_df: DataFrame,
        users_df: DataFrame,
    ) -> DataFrame:
        transactions_df = self.add_fraud_flag_int(transactions_df)

        return (
            transactions_df.alias("t")
            .join(
                cards_df.alias("c"),
                [
                    col("t.user_id") == col("c.user_id"),
                    col("t.card_index") == col("c.card_index"),
                ],
                "left"
            )
            .join(
                users_df.alias("u"),
                col("t.user_id") == col("u.user_id"),
                "left"
            )
            .select(
                col("t.user_id"),
                col("t.card_index"),
                col("t.transaction_date"),
                col("t.amount"),
                col("t.use_chip"),
                col("t.merchant_name"),
                col("t.merchant_city"),
                col("t.merchant_state"),
                col("t.zip"),
                col("t.mcc"),
                col("t.errors"),
                col("t.is_fraud"),
                col("t.is_fraud_int"),
                col("c.card_brand"),
                col("c.card_type"),
                col("c.has_chip"),
                col("c.credit_limit"),
                col("u.current_age"),
                col("u.gender"),
                col("u.per_capita_income"),
                col("u.yearly_income"),
                col("u.total_debt"),
                col("u.credit_score"),
            )
            .where(col("is_fraud") == True)
        )

    def build_fraud_by_state(
        self,
        transactions_df: DataFrame,
    ) -> DataFrame:
        transactions_df = self.add_fraud_flag_int(transactions_df)

        return (
            transactions_df
            .groupBy("merchant_state")
            .agg(
                count("*").alias("total_transactions"),
                spark_sum("is_fraud_int").alias("fraud_transactions"),
                spark_sum("amount").alias("total_transaction_amount"),
                spark_sum(
                    when(col("is_fraud") == True, col("amount")).otherwise(lit(0.0))
                ).alias("fraud_amount"),
                avg("is_fraud_int").alias("fraud_rate"),
            )
            .orderBy(col("fraud_transactions").desc())
        )

    def build_fraud_by_age_group(
        self,
        transactions_df: DataFrame,
        users_df: DataFrame,
    ) -> DataFrame:
        transactions_df = self.add_fraud_flag_int(transactions_df)

        enriched_df = (
            transactions_df.alias("t")
            .join(
                users_df.alias("u"),
                col("t.user_id") == col("u.user_id"),
                "left"
            )
            .withColumn(
                "age_group",
                when(col("u.current_age") < 25, "Under 25")
                .when(col("u.current_age").between(25, 34), "25-34")
                .when(col("u.current_age").between(35, 44), "35-44")
                .when(col("u.current_age").between(45, 54), "45-54")
                .when(col("u.current_age").between(55, 64), "55-64")
                .otherwise("65+")
            )
        )

        return (
            enriched_df
            .groupBy("age_group")
            .agg(
                count("*").alias("total_transactions"),
                spark_sum("is_fraud_int").alias("fraud_transactions"),
                spark_sum("amount").alias("total_transaction_amount"),
                spark_sum(
                    when(col("is_fraud") == True, col("amount")).otherwise(lit(0.0))
                ).alias("fraud_amount"),
                avg("is_fraud_int").alias("fraud_rate"),
            )
            .orderBy("age_group")
        )
    
    def build_fraud_by_income_band(
        self,
        transactions_df: DataFrame,
        users_df: DataFrame,
    ) -> DataFrame:
        transactions_df = self.add_fraud_flag_int(transactions_df)

        enriched_df = (
            transactions_df.alias("t")
            .join(
                users_df.alias("u"),
                col("t.user_id") == col("u.user_id"),
                "left"
            )
            .withColumn(
                "income_band",
                when(col("u.yearly_income") < 30000, "Under 30K")
                .when(col("u.yearly_income").between(30000, 59999), "30K-59K")
                .when(col("u.yearly_income").between(60000, 99999), "60K-99K")
                .when(col("u.yearly_income").between(100000, 149999), "100K-149K")
                .otherwise("150K+")
            )
        )

        return (
            enriched_df
            .groupBy("income_band")
            .agg(
                count("*").alias("total_transactions"),
                spark_sum("is_fraud_int").alias("fraud_transactions"),
                spark_sum("amount").alias("total_transaction_amount"),
                spark_sum(
                    when(col("is_fraud") == True, col("amount")).otherwise(lit(0.0))
                ).alias("fraud_amount"),
                avg("is_fraud_int").alias("fraud_rate"),
            )
            .orderBy("income_band")
        )

    def build_merchant_risk_profile(
        self,
        transactions_df: DataFrame,
    ) -> DataFrame:
        transactions_df = self.add_fraud_flag_int(transactions_df)

        return (
            transactions_df
            .groupBy(
                "merchant_name",
                "merchant_city",
                "merchant_state",
                "mcc",
            )
            .agg(
                count("*").alias("total_transactions"),
                spark_sum("is_fraud_int").alias("fraud_transactions"),
                spark_sum("amount").alias("total_transaction_amount"),
                spark_sum(
                    when(col("is_fraud") == True, col("amount")).otherwise(lit(0.0))
                ).alias("fraud_amount"),
                avg("is_fraud_int").alias("fraud_rate"),
                countDistinct("user_id").alias("unique_customers"),
            )
            .where(col("total_transactions") >= 10)
            .orderBy(
                col("fraud_rate").desc(),
                col("fraud_transactions").desc()
            )
        )