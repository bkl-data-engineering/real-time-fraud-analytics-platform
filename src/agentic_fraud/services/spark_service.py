from __future__ import annotations

from typing import Any, Mapping

from pyspark.sql import DataFrame, SparkSession


class SparkServiceError(RuntimeError):
    """Raised when a Spark operation cannot be completed."""


class SparkService:
    """
    Provides a small, reusable interface for Spark SQL operations.

    This service isolates Spark-specific behavior from the domain services
    and LangChain tools. Higher-level services should use this class instead
    of accessing SparkSession directly.
    """

    def __init__(self, spark: SparkSession | None = None) -> None:
        """
        Initialize the service with an existing Spark session.

        When no session is supplied, the active Spark session is used.
        If there is no active session, a new session is created.
        """
        self._spark = spark or self._resolve_spark_session()

    @property
    def spark(self) -> SparkSession:
        """Return the Spark session managed by this service."""
        return self._spark

    @staticmethod
    def _resolve_spark_session() -> SparkSession:
        """
        Return the active Spark session or create one when necessary.
        """
        active_session = SparkSession.getActiveSession()

        if active_session is not None:
            return active_session

        return SparkSession.builder.getOrCreate()

    def execute_query(
        self,
        query: str,
        parameters: Mapping[str, Any] | None = None,
    ) -> DataFrame:
        """
        Execute a Spark SQL query and return the resulting DataFrame.

        Parameters should be supplied separately rather than inserted into
        SQL strings with f-strings.

        Example:
            query = '''
                SELECT *
                FROM fraud_platform.gold.merchant_risk_profile
                WHERE merchant_state = :merchant_state
            '''

            dataframe = service.execute_query(
                query=query,
                parameters={"merchant_state": "MA"},
            )
        """
        normalized_query = query.strip()

        if not normalized_query:
            raise ValueError("SQL query cannot be empty.")

        try:
            if parameters:
                return self._spark.sql(
                    normalized_query,
                    args=dict(parameters),
                )

            return self._spark.sql(normalized_query)

        except Exception as exc:
            raise SparkServiceError(
                f"Spark SQL query failed: {exc}"
            ) from exc

    def read_table(self, table_name: str) -> DataFrame:
        """
        Read a registered Spark or Unity Catalog table.

        Example:
            dataframe = service.read_table(
                "fraud_platform.gold.merchant_risk_profile"
            )
        """
        normalized_table_name = table_name.strip()

        if not normalized_table_name:
            raise ValueError("Table name cannot be empty.")

        try:
            return self._spark.table(normalized_table_name)

        except Exception as exc:
            raise SparkServiceError(
                f"Unable to read table '{normalized_table_name}': {exc}"
            ) from exc

    def table_exists(self, table_name: str) -> bool:
        """
        Return True when the supplied table exists.
        """
        normalized_table_name = table_name.strip()

        if not normalized_table_name:
            raise ValueError("Table name cannot be empty.")

        try:
            return self._spark.catalog.tableExists(
                normalized_table_name
            )

        except Exception as exc:
            raise SparkServiceError(
                f"Unable to check table '{normalized_table_name}': {exc}"
            ) from exc

    def collect_as_dicts(
        self,
        dataframe: DataFrame,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """
        Collect a limited number of DataFrame rows as dictionaries.

        The limit protects the driver from accidentally collecting an
        unbounded distributed DataFrame.
        """
        if limit <= 0:
            raise ValueError("Collection limit must be greater than zero.")

        try:
            rows = dataframe.limit(limit).collect()
            return [row.asDict(recursive=True) for row in rows]

        except Exception as exc:
            raise SparkServiceError(
                f"Unable to collect Spark DataFrame rows: {exc}"
            ) from exc

    def execute_query_as_dicts(
        self,
        query: str,
        parameters: Mapping[str, Any] | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """
        Execute a SQL query and return a limited list of dictionaries.

        This is a convenience method for domain services that need small
        result sets rather than a distributed DataFrame.
        """
        dataframe = self.execute_query(
            query=query,
            parameters=parameters,
        )

        return self.collect_as_dicts(
            dataframe=dataframe,
            limit=limit,
        )