from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, current_timestamp, col

class streamingBronzeLoader():
    def __init__(
        self,
        spark: SparkSession,
        catalog: str,
        schema: str,
        source_path: str,
        checkpoint_path: str,
        schema_location: str
    ):
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
        self.source_path = source_path
        self.checkpoint_path = checkpoint_path
        self.schema_location = schema_location
    
    def read_stream(self) -> DataFrame:
        return (
            self.spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.schemaLocation", self.schema_location)
            .option("header", "true")
            .option("inferColumnTypes", "false")
            .load(self.source_path)
        )
        

    def add_metadata(self, df: DataFrame) -> DataFrame:
        return (
            df
            .withColumn("_ingestion_ts", current_timestamp())
            .withColumn("_source_file", col("_metadata")["file_path"])
            .withColumn("_source_file_name", col("_metadata")["file_name"])
            .withColumn("_source_file_size", col("_metadata")["file_size"])
            .withColumn("_source_file_modification_time", col("_metadata")["file_modification_time"])
            .withColumn("_load_type", lit("streaming_micro_batch"))
        )

    def write_stream(self, df: DataFrame):
        target_table = f"{self.catalog}.{self.schema}.streaming_transactions"

        return (
            df.writeStream
            .format("delta")
            .option("checkpointLocation", self.checkpoint_path)
            .trigger(availableNow=True)
            .outputMode("append")
            .toTable(target_table)
        )