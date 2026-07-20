import uuid
import time
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, lit


class TransactionStreamGenerator:

    def __init__(
        self,
        spark: SparkSession,
        source_table: str,
        output_path: str,
        batch_size: int = 1000
    ):
        self.spark = spark
        self.source_table = source_table
        self.output_path = output_path
        self.batch_size = batch_size

    def generate_batch(self):

        batch_id = str(uuid.uuid4())

        batch_df = (
            self.spark.table(self.source_table)
            .orderBy(rand())
            .limit(self.batch_size)
            .withColumn("_load_type", lit("stream"))
            .withColumn("_stream_batch_id", lit(batch_id))
            .withColumn(
                "_stream_file_name",
                lit(
                    f"transactions_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{batch_id}.csv"
                )
            )
        )

        print(f"batch_df count is: {batch_df.count()}")
        
        (    batch_df
            .write
            .format("csv")
            .option("header", "true")
            .mode("append")
            .save(self.output_path)
        )

        file_name = (
            f"{self.output_path}/"
            f"transactions_{datetime.now().strftime('%Y%m%d_%H%M%S')}_"
            f"{batch_id}.csv"
        )

        print(
            f"[{datetime.now()}] "
            f"Generated batch: {batch_id} "
            f"({self.batch_size} records)"
        )

    def run(
        self,
        num_batches: int = 10,
        interval_seconds: int = 30
    ):

        print(
            f"Starting stream simulation "
            f"({num_batches} batches)"
        )

        for batch_num in range(num_batches):

            print(
                f"Generating batch "
                f"{batch_num + 1}/{num_batches}"
            )

            self.generate_batch()

            if batch_num < num_batches - 1:
                time.sleep(interval_seconds)

        print("Stream simulation completed")