from pyspark.sql import SparkSession, DataFrame

from pyspark.sql.functions import (
    col,
    concat_ws,
    lpad,
    regexp_replace,
    to_date,
    to_timestamp,
    when,
    row_number,
    monotonically_increasing_id,
    year,
    current_date,
)

from pyspark.sql.window import Window


class SilverTransformer:
    """
    Reusable Silver transformation helper.

    Responsibilities:
    - Cast Bronze string fields into trusted Silver data types
    - Derive canonical date/timestamp fields
    - Preserve Bronze lineage metadata
    """

    def __init__(
        self,
        spark: SparkSession,
        catalog: str,
        bronze_schema: str = "bronze",
        silver_schema: str = "silver",
    ) -> None:
        self.spark = spark
        self.catalog = catalog
        self.bronze_schema = bronze_schema
        self.silver_schema = silver_schema

    def read_bronze_table(self, table_name: str) -> DataFrame:
        full_table_name = f"{self.catalog}.{self.bronze_schema}.{table_name}"
        return self.spark.table(full_table_name)

    def write_silver_table(
        self,
        df: DataFrame,
        table_name: str,
        mode: str = "overwrite",
    ) -> None:
        full_table_name = f"{self.catalog}.{self.silver_schema}.{table_name}"

        (
            df.write
            .format("delta")
            .mode(mode)
            .option("overwriteSchema", "true")
            .saveAsTable(full_table_name)
        )

    def clean_currency(self, column_name: str):
        return (
            regexp_replace(
                regexp_replace(col(column_name), "\\$", ""),
                ",",
                "",
            )
            .cast("decimal(18,2)")
        )

    def to_boolean(self, column_name: str):
        normalized_col = regexp_replace(
            regexp_replace(col(column_name), '"', ""),
            "'",
            "",
        )

        return (
            when(normalized_col.isin("Yes", "yes", "TRUE", "True", "true", "1"), True)
            .when(normalized_col.isin("No", "no", "FALSE", "False", "false", "0"), False)
            .otherwise(None)
        )

    def transform_transactions(self, df: DataFrame) -> DataFrame:
        return (
            df
            .withColumnRenamed("user", "user_id")
            .withColumnRenamed("card", "card_index")
            .withColumn("user_id", col("user_id").cast("int"))
            .withColumn("card_index", col("card_index").cast("int"))
            .withColumn("transaction_year", col("year").cast("int"))
            .withColumn("transaction_month", col("month").cast("int"))
            .withColumn("transaction_day", col("day").cast("int"))
            .withColumn(
                "transaction_date",
                to_date(
                    concat_ws(
                        "-",
                        col("year"),
                        lpad(col("month"), 2, "0"),
                        lpad(col("day"), 2, "0"),
                    ),
                    "yyyy-MM-dd",
                ),
            )
            .withColumn(
                "transaction_ts",
                to_timestamp(
                    concat_ws(
                        " ",
                        concat_ws(
                            "-",
                            col("year"),
                            lpad(col("month"), 2, "0"),
                            lpad(col("day"), 2, "0"),
                        ),
                        col("time"),
                    ),
                    "yyyy-MM-dd HH:mm",
                ),
            )
            .withColumn("amount_decimal", self.clean_currency("amount"))
            .withColumn("zip", col("zip").cast("string"))
            .withColumn("mcc", col("mcc").cast("string"))
            .withColumn("is_fraud_bool", self.to_boolean("is_fraud"))
            .select(
                "user_id",
                "card_index",
                "transaction_year",
                "transaction_month",
                "transaction_day",
                "transaction_date",
                "transaction_ts",
                "amount_decimal",
                "use_chip",
                "merchant_name",
                "merchant_city",
                "merchant_state",
                "zip",
                "mcc",
                "errors",
                "is_fraud_bool",
                "_run_id",
                "_source_file",
                "_ingestion_ts",
                "_load_type",
                "_record_hash",
            )
        )

    def transform_cards(self, df: DataFrame) -> DataFrame:
        return (
            df
            .withColumnRenamed("user", "user_id")
            .withColumn("user_id", col("user_id").cast("int"))
            .withColumn("card_index", col("card_index").cast("int"))
            .withColumn("cards_issued", col("cards_issued").cast("int"))
            .withColumn("credit_limit_decimal", self.clean_currency("credit_limit"))
            .withColumn(
                "acct_open_date_parsed",
                to_date(col("acct_open_date"), "MM/yyyy"),
            )
            .withColumn(
                "year_pin_last_changed",
                col("year_pin_last_changed").cast("int"),
            )
            .withColumn("has_chip_bool", self.to_boolean("has_chip"))
            .withColumn(
                "card_on_dark_web_bool",
                self.to_boolean("card_on_dark_web"),
            )
            .select(
                "user_id",
                "card_index",
                "card_brand",
                "card_type",
                "card_number",
                "expires",
                "cvv",
                "has_chip_bool",
                "cards_issued",
                "credit_limit_decimal",
                "acct_open_date_parsed",
                "year_pin_last_changed",
                "card_on_dark_web_bool",
                "_run_id",
                "_source_file",
                "_ingestion_ts",
                "_load_type",
                "_record_hash",
            )
        )

    def transform_users(self, df: DataFrame) -> DataFrame:
        return (
            df
            .withColumnRenamed("person", "person_name")
            .withColumn("current_age", col("current_age").cast("int"))
            .withColumn("retirement_age", col("retirement_age").cast("int"))
            .withColumn("birth_year", col("birth_year").cast("int"))
            .withColumn("birth_month", col("birth_month").cast("int"))
            .withColumn("latitude", col("latitude").cast("double"))
            .withColumn("longitude", col("longitude").cast("double"))
            .withColumn(
                "per_capita_income_zipcode_decimal",
                self.clean_currency("per_capita_income_zipcode"),
            )
            .withColumn(
                "yearly_income_person_decimal",
                self.clean_currency("yearly_income_person"),
            )
            .withColumn(
                "total_debt_decimal",
                self.clean_currency("total_debt"),
            )
            .withColumn("fico_score", col("fico_score").cast("int"))
            .withColumn("num_credit_cards", col("num_credit_cards").cast("int"))
            .select(
                "person_name",
                "current_age",
                "retirement_age",
                "birth_year",
                "birth_month",
                "gender",
                "address",
                "apartment",
                "city",
                "state",
                "zipcode",
                "latitude",
                "longitude",
                "per_capita_income_zipcode_decimal",
                "yearly_income_person_decimal",
                "total_debt_decimal",
                "fico_score",
                "num_credit_cards",
                "_run_id",
                "_source_file",
                "_ingestion_ts",
                "_load_type",
                "_record_hash",
            )
        )

    def transform_users_expanded(self, users_df: DataFrame) -> DataFrame:
        """
        Build Silver users_expanded table.

        The source users file has 2000 rows but no user_id column.
        cards and transactions both use user_id values from 0 to 1999.
        Therefore, we reconstruct user_id from the users file row sequence.
        """

        window_spec = Window.orderBy(monotonically_increasing_id())

        users_expanded_df = (
            users_df
            .withColumn("user_id", row_number().over(window_spec) - 1)
            .withColumn("user_id", col("user_id").cast("int"))
        )

        if "birth_year" in users_expanded_df.columns:
            users_expanded_df = users_expanded_df.withColumn(
                "customer_age",
                year(current_date()) - col("birth_year").cast("int")
            )

        return users_expanded_df

    def transform_and_write_table(
        self,
        source_table_name: str,
        target_table_name: str,
        transform_type: str,
        mode: str = "overwrite",
    ) -> None:
        bronze_df = self.read_bronze_table(source_table_name)

        if transform_type == "transactions":
            silver_df = self.transform_transactions(bronze_df)
        elif transform_type == "cards":
            silver_df = self.transform_cards(bronze_df)
        elif transform_type == "users":
            silver_df = self.transform_users(bronze_df)
        else:
            raise ValueError(f"Unsupported transform_type: {transform_type}")

        print(
            f"[SILVER_TRANSFORM] Writing {target_table_name} "
            f"with {silver_df.count()} records"
        )

        self.write_silver_table(
            df=silver_df,
            table_name=target_table_name,
            mode=mode,
        )