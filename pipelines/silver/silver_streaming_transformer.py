from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
    sha2,
    dayofweek,
    lpad,
    to_timestamp,
    to_date,
    concat_ws,
    year,
    month,
    hour,
    trim,
    when
)

from pyspark.sql.types import (
    StringType,
    DecimalType,
    IntegerType,
)

import sys
PROJECT_ROOT = "/Workspace/real-time-analytics-platform"
#/Workspace/real-time-fraud-analytics-platform/config/streaming_config.py
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

#import importlib
#import config.streaming_config as streaming_config
#importlib.reload(streaming_config)
from config.streaming_config import StreamingConfig

class StreamingSilverTransfer:
    """
    Transforms streaming bronze transactions data into a cleaned silver table.

    Responsibilities:
    - Read streaming bronze transaction table
    - cast columns to correct data type
    - create transaction timestamp/date fields
    - add fraud-analysis-friendly derived columns
    - apply lightweight data quality rules
    - write to silver delta table
    """

    TRANSACTION_COLUMN_TYPES = {
        "user": IntegerType(),
        "card": IntegerType(),
        "year": IntegerType(),
        "month": IntegerType(),
        "day": IntegerType(),
        "amount": DecimalType(18,2),
        "merchant_name": StringType(),
        "merchant_city": StringType(),
        "merchant_state": StringType(),
        "merchant_country": StringType(),
        "zip": StringType(),
        "mcc": StringType(),
        "errors": StringType(),
        "is_fraud": StringType()
    }

    def __init__(self, spark: SparkSession, config: StreamingConfig):
        self.spark = spark
        self.config = config
        
    def read_stream(self) -> DataFrame:
        return self.spark.readStream.table(
            self.config.bronze_streaming_table
        )

    def cast_columns(self, df: DataFrame) -> DataFrame:
        for column, data_type in self.TRANSACTION_COLUMN_TYPES.items():
            df = df.withColumn(column, col(column).cast(data_type))
        return df

    def clean_string_columns(self, df: DataFrame) -> DataFrame:
        string_columns = [
            "merchant_name",
            "merchant_city",
            "merchant_state",
            "zip",
            "mcc",
            "errors",
            "is_fraud"
        ]

        for column in string_columns:
            df = df.withColumn(column, trim(col(column)))
        return df
    
    def create_transaction_timestamp(self, df: DataFrame) -> DataFrame:
        transaction_date_string = concat_ws(
            "-",
            col("year").cast("string"),
            lpad(col("month").cast("string"), 2, "0"),
            lpad(col("day").cast("string"), 2, "0")
        )
    
        transaction_datetime_string = concat_ws(
            " ",
            transaction_date_string,
            col("time")
        )
    
        return (
            df.withColumn(
                "transaction_timestamp",
                to_timestamp(transaction_datetime_string, "yyyy-MM-dd HH:mm:ss")
            )
            .withColumn(
                "transaction_date",
                to_date(col("transaction_timestamp"))
            )
        )

    def add_derived_columns(self, df: DataFrame) -> DataFrame:
        return (
            df.withColumn("transaction_hour", hour(col("transaction_timestamp")))
            .withColumn("transaction_day_of_week", dayofweek(col("transaction_timestamp")))
            .withColumn(
                "is_weekend",
                when(col("transaction_day_of_week").isin(1, 7), True).otherwise(False)
            )
            .withColumn(
                "amount_band",
                when(col("amount") < 50, "LOW")
                .when((col("amount") >= 50) & (col("amount") < 200), "MEDIUM")
                .when((col("amount") >= 200) & (col("amount") < 1000), "HIGH")
                .otherwise("VERY_HIGH")
            )
            .withColumn(
                "is_online_transaction",
                when(col("merchant_city") == "ONLINE", True).otherwise(False)
            )
            .withColumn(
                "_silver_processed_ts",
                current_timestamp()
            )
        )

    def apply_data_quality(self, df: DataFrame) -> DataFrame:
        return df.filter(
            col("user").isNotNull()
            & col("card").isNotNull()
            & col("amount").isNotNull()
            & col("merchant_name").isNotNull()
            & col("transaction_timestamp").isNotNull()
        )

    def transform(self, df: DataFrame) -> DataFrame:
        df = self.cast_columns(df)
        df = self.clean_string_columns(df)
        df = self.create_transaction_timestamp(df)
        df = self.add_derived_columns(df)
        df = self.apply_data_quality(df)
        return df

    def write_stream(self, df: DataFrame):
        return (
            df.writeStream
            .format("delta")
            .option("checkpointLocation", self.config.silver_checkpoint_path)
            .outputMode("append")
            .trigger(availableNow=True)
            .toTable(self.config.silver_streaming_table)
        )

    def run(self):
        bronze_df = self.read_stream()
        silver_df = self.transform(bronze_df)
        return self.write_stream(silver_df)

    
