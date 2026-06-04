from typing import List, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, concat_ws, current_timestamp, sha2
import re

class BronzeLoader:
    """
    Reusable Bronze ingestion helper for seed and streaming-style loads.
    Adds common metadata columns and writes DataFrames to Delta tables.
    """
    def __init__(
        self,
        spark: SparkSession,
        catalog: str,
        schema: str = "bronze",
        run_id: Optional[str] = None
    ) -> None:
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
        self.run_id = run_id

    def clean_column_name(self, column_name: str) -> str:
        """
        Convert source column names into
        Delta-friendly snake_case names.
        """
        cleaned = column_name.strip().lower()
        cleaned = re.sub(
            r"[^a-zA-Z0-9_]",
            "_",
            cleaned
        )
        cleaned = re.sub(
            r"_+",
            "_",
            cleaned
        )
        cleaned = cleaned.strip("_")
        return cleaned
    
    def clean_column_names(self, df: DataFrame) -> DataFrame:
        """
        Standardize column names to be Delta-friendly and
        log any column renames for observability.
        """
        cleaned_df = df
        renamed_columns = []
        for original_column in df.columns:
            cleaned_column = self.clean_column_name(
                original_column
            )
            if original_column != cleaned_column:
                renamed_columns.append(
                    f"{original_column} -> {cleaned_column}"
                )
                cleaned_df = cleaned_df.withColumnRenamed(
                    original_column,
                    cleaned_column
                )
        if renamed_columns:
            print(
                f"[COLUMN_STANDARDIZATION] "
                f"{len(renamed_columns)} column(s) renamed"
            )
            for renamed_column in renamed_columns:
                print(f"  {renamed_column}")
        return cleaned_df

    def read_csv (
        self,
        file_path: str,
        header: bool = True,
        infer_schema: bool = False,
    ) -> DataFrame:
        return (
            self.spark.read
            .option("header", str(header).lower())
            .option("inferSchema", str(infer_schema).lower())
            .option("escape", "\"")
            .option("multiline", "true")
            .csv(file_path)
        )

    def add_metadata(
        self,
        df: DataFrame,
        source_name: str,
        load_type: str = "seed",
        ) -> DataFrame:
        data_columns: List[str] = [
            c for c in df.columns if not c.startswith("_")
        ]

        return (
            df
            .withColumn("_run_id", lit(self.run_id))
            .withColumn("_source_file", lit(source_name))
            .withColumn("_ingestion_ts", current_timestamp())
            .withColumn("_load_type", lit(load_type))
            .withColumn(
                "_record_hash",
                sha2(
                    concat_ws(
                        "||",
                        *[col(c).cast("string") for c in data_columns]
                    ),
                    256,
                ),
            )
        )

    def write_delta (
        self,
        df: DataFrame,
        table_name: str,
        mode: str = "overwrite",
        overwrite_schema: bool = True,
    ) -> None:
        full_table_name = f"{self.catalog}.{self.schema}.{table_name}"
        (   
            df
            .write
            .format("delta")
            .mode(mode)
            .option("overwriteSchema", overwrite_schema)
            .saveAsTable(full_table_name)
        )
    
    def load_csv_to_bronze(
        self,
        file_path: str,
        table_name: str,
        source_name: str,
        load_type: str = "seed",
        mode: str = "overwrite",
        header: bool = True,
        infer_schema: bool = False,
    ) -> None:

        raw_df = self.read_csv(
            file_path=file_path,
            header=header,
            infer_schema=infer_schema,
        )

        cleaned_df = self.clean_column_names(
            raw_df
        )

        bronze_df = self.add_metadata(
            df=cleaned_df,
            source_name=source_name,
            load_type=load_type,
        )

        print(
            f"[INGESTION] "
            f"Loaded {bronze_df.count()} records "
            f"from {source_name}"
        )

        self.write_delta(
            df=bronze_df,
            table_name=table_name,
            mode=mode,
        )