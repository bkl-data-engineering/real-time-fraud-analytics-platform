from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    date_format,
    dayofweek,
    hour,
    lower,
    to_date,
    to_timestamp,
    trim,
    when,
)
from pyspark.sql.types import DecimalType


class StreamingSilverTransformer:
    """
    Transforms Bronze streaming transaction records and writes them
    to a Silver Delta table.

    The transformer:
    - Reads from the Bronze streaming Delta table
    - Casts columns to appropriate data types
    - Parses transaction dates and timestamps
    - Standardizes string columns
    - Adds analytical attributes
    - Applies basic data-quality filters
    - Writes incrementally to the Silver table
    """

    def __init__(
        self,
        spark: SparkSession,
        catalog: str = "fraud_platform",
        bronze_schema: str = "bronze",
        silver_schema: str = "silver",
        source_table: str = "streaming_transactions",
        target_table: str = "streaming_transactions",
        checkpoint_path: str = (
            "/Volumes/fraud_platform/silver/"
            "micro_batches/checkpoints/streaming_transactions"
        ),
    ) -> None:
        self.spark = spark

        self.source_table = (
            f"{catalog}.{bronze_schema}.{source_table}"
        )

        self.target_table = (
            f"{catalog}.{silver_schema}.{target_table}"
        )

        self.checkpoint_path = checkpoint_path

    def read_stream(self) -> DataFrame:
        """
        Reads transaction records from the Bronze streaming Delta table.
        """
        return (
            self.spark.readStream
            .format("delta")
            .table(self.source_table)
        )

    def cast_columns(self, df: DataFrame) -> DataFrame:
        """
        Casts Bronze string columns into appropriate Silver data types.

        transaction_ts is stored in Bronze using ISO 8601 UTC format:

            2014-07-07T14:04:00.000Z
        """
        return (
            df
            .withColumn(
                "user_id",
                col("user_id").cast("int"),
            )
            .withColumn(
                "card_index",
                col("card_index").cast("int"),
            )
            .withColumn(
                "transaction_year",
                col("transaction_year").cast("int"),
            )
            .withColumn(
                "transaction_month",
                col("transaction_month").cast("int"),
            )
            .withColumn(
                "transaction_day",
                col("transaction_day").cast("int"),
            )
            .withColumn(
                "transaction_date",
                to_date(
                    col("transaction_date"),
                    "yyyy-MM-dd",
                ),
            )
            .withColumn(
                "transaction_timestamp",
                to_timestamp(
                    col("transaction_ts"),
                    "yyyy-MM-dd'T'HH:mm:ss.SSSX",
                ),
            )
            .withColumn(
                "amount",
                col("amount").cast(DecimalType(18, 2)),
            )
            .withColumn(
                "mcc",
                col("mcc").cast("string"),
            )
            .withColumn(
                "zip",
                col("zip").cast("string"),
            )
        )

    def standardize_strings(self, df: DataFrame) -> DataFrame:
        """
        Trims and standardizes selected string columns.
        """
        return (
            df
            .withColumn(
                "use_chip",
                trim(col("use_chip")),
            )
            .withColumn(
                "merchant_name",
                trim(col("merchant_name")),
            )
            .withColumn(
                "merchant_city",
                trim(col("merchant_city")),
            )
            .withColumn(
                "merchant_state",
                trim(col("merchant_state")),
            )
            .withColumn(
                "errors",
                trim(col("errors")),
            )
            .withColumn(
                "is_fraud",
                lower(trim(col("is_fraud"))),
            )
        )

    def add_derived_columns(self, df: DataFrame) -> DataFrame:
        """
        Adds columns useful for downstream fraud analytics.
        """
        return (
            df
            .withColumn(
                "transaction_hour",
                hour(col("transaction_timestamp")),
            )
            .withColumn(
                "transaction_day_of_week",
                date_format(
                    col("transaction_timestamp"),
                    "EEEE",
                ),
            )
            .withColumn(
                "is_weekend",
                when(
                    dayofweek(
                        col("transaction_timestamp")
                    ).isin(1, 7),
                    True,
                ).otherwise(False),
            )
            .withColumn(
                "is_online_transaction",
                when(
                    lower(trim(col("use_chip")))
                    == "online transaction",
                    True,
                ).otherwise(False),
            )
            .withColumn(
                "amount_band",
                when(
                    col("amount") < 25,
                    "LOW",
                )
                .when(
                    col("amount") < 100,
                    "MEDIUM",
                )
                .when(
                    col("amount") < 500,
                    "HIGH",
                )
                .otherwise("VERY_HIGH"),
            )
        )

    def apply_data_quality(self, df: DataFrame) -> DataFrame:
        """
        Removes records missing required transaction attributes.
        """
        return df.filter(
            col("user_id").isNotNull()
            & col("card_index").isNotNull()
            & col("amount").isNotNull()
            & col("merchant_name").isNotNull()
            & col("transaction_timestamp").isNotNull()
        )

    def select_output_columns(self, df: DataFrame) -> DataFrame:
        """
        Selects and orders columns written to the Silver table.

        The original transaction_ts is retained for traceability,
        alongside the parsed transaction_timestamp.
        """
        return df.select(
            "user_id",
            "card_index",
            "transaction_year",
            "transaction_month",
            "transaction_day",
            "transaction_date",
            "transaction_ts",
            "transaction_timestamp",
            "transaction_hour",
            "transaction_day_of_week",
            "is_weekend",
            "amount",
            "amount_band",
            "use_chip",
            "is_online_transaction",
            "merchant_name",
            "merchant_city",
            "merchant_state",
            "zip",
            "mcc",
            "errors",
            "is_fraud",
            "_run_id",
            "_source_file",
            "_ingestion_ts",
            "_load_type",
            "_record_hash",
            "_stream_batch_id",
            "_stream_file_name",
            "_rescued_data",
            "_source_file_name",
            "_source_file_size",
            "_source_file_modification_time",
        )

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Executes all Silver transformation steps.
        """
        transformed_df = self.cast_columns(df)
        transformed_df = self.standardize_strings(transformed_df)
        transformed_df = self.add_derived_columns(transformed_df)
        transformed_df = self.apply_data_quality(transformed_df)
        transformed_df = self.select_output_columns(transformed_df)

        return transformed_df

    def write_stream(self, df: DataFrame):
        """
        Writes transformed records to the Silver Delta table.

        availableNow processes all currently available Bronze records
        and then stops the query.
        """
        return (
            df.writeStream
            .format("delta")
            .outputMode("append")
            .option(
                "checkpointLocation",
                self.checkpoint_path,
            )
            .trigger(availableNow=True)
            .toTable(self.target_table)
        )

    def run(self):
        """
        Runs the complete Bronze-to-Silver streaming transformation.
        """
        bronze_stream_df = self.read_stream()
        silver_stream_df = self.transform(bronze_stream_df)

        return self.write_stream(silver_stream_df)
