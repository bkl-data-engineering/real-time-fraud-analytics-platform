from __future__ import annotations

import datetime
from decimal import Decimal
from typing import Any

from agentic_fraud.models import MerchantLocator
from agentic_fraud.services.spark_service import SparkService


class MerchantNotFoundError(LookupError):
    """Raised when a merchant cannot be found in the Gold tables."""


class MerchantAmbiguousError(LookupError):
    """
    Raised when the supplied merchant locator matches multiple Gold records.

    Additional location or MCC information is required to uniquely identify
    the merchant.
    """


class MerchantService:
    """
    Provides deterministic merchant fraud analytics.

    This service owns merchant-specific SQL and uses SparkService for
    interaction with Spark. It does not contain LangChain or LLM logic.
    """

    DEFAULT_MERCHANT_PROFILE_TABLE = (
        "fraud_platform.gold.merchant_risk_profile"
    )

    DEFAULT_FRAUD_TRANSACTIONS_TABLE = (
        "fraud_platform.gold.fraud_transactions"
    )

    def __init__(
        self,
        spark_service: SparkService,
        merchant_profile_table: str | None = None,
        fraud_transactions_table: str | None = None,
    ) -> None:
        if spark_service is None:
            raise TypeError("spark_service must not be None.")

        self._spark_service = spark_service

        self._merchant_profile_table = (
            merchant_profile_table
            or self.DEFAULT_MERCHANT_PROFILE_TABLE
        )

        self._fraud_transactions_table = (
            fraud_transactions_table
            or self.DEFAULT_FRAUD_TRANSACTIONS_TABLE
        )

    def get_merchant_risk_profile(
        self,
        merchant: MerchantLocator,
    ) -> dict[str, Any]:
        """
        Return the aggregated Gold risk profile for one merchant.

        Merchant name is required by MerchantLocator. City, state, and MCC
        are applied only when they are supplied.

        Raises:
            MerchantNotFoundError:
                If no merchant matches the supplied locator.
            MerchantAmbiguousError:
                If multiple merchant records match the supplied locator.
        """
        merchant_filter, parameters = self._build_merchant_filter(
            merchant
        )

        query = f"""
            SELECT
                merchant_name,
                merchant_city,
                merchant_state,
                mcc,
                total_transactions,
                fraud_transactions,
                total_transaction_amount,
                fraud_amount,
                fraud_rate,
                unique_customers
            FROM {self._merchant_profile_table}
            WHERE {merchant_filter}
            LIMIT 2
        """

        results = self._spark_service.execute_query_as_dicts(
            query=query,
            parameters=parameters,
            limit=2,
        )

        if not results:
            raise MerchantNotFoundError(
                self._merchant_not_found_message(merchant)
            )

        if len(results) > 1:
            raise MerchantAmbiguousError(
                self._merchant_ambiguous_message(merchant)
            )

        return self._normalize_record(results[0])

    def compare_merchant_to_peers(
        self,
        merchant: MerchantLocator,
        peer_group_size: int = 10,
    ) -> dict[str, Any]:
        """
        Compare a merchant with peers sharing the same state and MCC.

        The merchant is first resolved against the Gold profile table. The
        resolved record supplies the authoritative state and MCC values used
        to define its peer group.

        The response contains:
            - the target merchant profile,
            - peer-group aggregate statistics,
            - relative fraud-rate metrics,
            - the highest-risk peer merchants.
        """
        if peer_group_size <= 0:
            raise ValueError(
                "Peer group size must be greater than zero."
            )

        target_profile = self.get_merchant_risk_profile(merchant)

        resolved_merchant = self._locator_from_profile(
            target_profile
        )

        peer_parameters = self._merchant_parameters(
            resolved_merchant
        )

        peer_summary_query = f"""
            SELECT
                COUNT(*) AS peer_count,
                AVG(fraud_rate) AS average_peer_fraud_rate,
                MIN(fraud_rate) AS minimum_peer_fraud_rate,
                MAX(fraud_rate) AS maximum_peer_fraud_rate,
                percentile_approx(
                    fraud_rate,
                    0.5
                ) AS median_peer_fraud_rate,
                AVG(
                    total_transactions
                ) AS average_peer_transactions,
                AVG(
                    fraud_transactions
                ) AS average_peer_fraud_transactions
            FROM {self._merchant_profile_table}
            WHERE merchant_state <=> :merchant_state
              AND CAST(mcc AS STRING) <=> :mcc
              AND NOT (
                    merchant_name <=> :merchant_name
                AND merchant_city <=> :merchant_city
                AND merchant_state <=> :merchant_state
                AND CAST(mcc AS STRING) <=> :mcc
              )
        """

        peer_summary_results = (
            self._spark_service.execute_query_as_dicts(
                query=peer_summary_query,
                parameters=peer_parameters,
                limit=1,
            )
        )

        peer_summary = (
            self._normalize_record(peer_summary_results[0])
            if peer_summary_results
            else {}
        )

        peer_merchants_query = f"""
            SELECT
                merchant_name,
                merchant_city,
                merchant_state,
                mcc,
                total_transactions,
                fraud_transactions,
                fraud_rate,
                fraud_amount,
                unique_customers
            FROM {self._merchant_profile_table}
            WHERE merchant_state <=> :merchant_state
              AND CAST(mcc AS STRING) <=> :mcc
              AND NOT (
                    merchant_name <=> :merchant_name
                AND merchant_city <=> :merchant_city
                AND merchant_state <=> :merchant_state
                AND CAST(mcc AS STRING) <=> :mcc
              )
            ORDER BY
                fraud_rate DESC,
                fraud_transactions DESC,
                total_transactions DESC
            LIMIT {int(peer_group_size)}
        """

        peer_merchants = (
            self._spark_service.execute_query_as_dicts(
                query=peer_merchants_query,
                parameters=peer_parameters,
                limit=peer_group_size,
            )
        )

        normalized_peers = [
            self._normalize_record(record)
            for record in peer_merchants
        ]

        target_fraud_rate = self._to_float(
            target_profile.get("fraud_rate")
        )

        average_peer_fraud_rate = self._to_float(
            peer_summary.get("average_peer_fraud_rate")
        )

        fraud_rate_difference = (
            target_fraud_rate - average_peer_fraud_rate
        )

        fraud_rate_ratio = (
            target_fraud_rate / average_peer_fraud_rate
            if average_peer_fraud_rate > 0
            else None
        )

        return {
            "merchant": target_profile,
            "peer_definition": {
                "merchant_state": (
                    resolved_merchant.merchant_state
                ),
                "mcc": resolved_merchant.mcc,
            },
            "peer_summary": peer_summary,
            "comparison": {
                "merchant_fraud_rate": target_fraud_rate,
                "average_peer_fraud_rate": (
                    average_peer_fraud_rate
                ),
                "fraud_rate_difference": fraud_rate_difference,
                "fraud_rate_ratio": fraud_rate_ratio,
            },
            "highest_risk_peers": normalized_peers,
        }

    def get_merchant_fraud_transactions(
        self,
        merchant: MerchantLocator,
        start_date: datetime.date | None = None,
        end_date: datetime.date | None = None,
        max_transactions: int = 20,
    ) -> list[dict[str, Any]]:
        """
        Return fraud-labeled transactions for a merchant.

        The merchant is first resolved against the Gold merchant profile. This
        ensures that omitted locator values, such as MCC, are populated from
        the authoritative profile before querying transaction-level data.

        Results are ordered by transaction date and amount descending.
        """
        self._validate_date_range(
            start_date=start_date,
            end_date=end_date,
        )

        if max_transactions <= 0:
            raise ValueError(
                "Maximum transaction count must be greater than zero."
            )

        resolved_merchant = self._resolve_merchant(merchant)

        merchant_filter, parameters = self._build_merchant_filter(
            resolved_merchant
        )

        date_filter, date_parameters = self._build_date_filter(
            start_date=start_date,
            end_date=end_date,
        )

        parameters.update(date_parameters)

        query = f"""
            SELECT
                user_id,
                card_index,
                transaction_date,
                amount,
                use_chip,
                merchant_name,
                merchant_city,
                merchant_state,
                zip,
                mcc,
                errors,
                is_fraud,
                is_fraud_int,
                card_brand,
                card_type,
                has_chip,
                credit_limit,
                current_age,
                gender,
                per_capita_income,
                yearly_income,
                total_debt,
                credit_score
            FROM {self._fraud_transactions_table}
            WHERE {merchant_filter}
              {date_filter}
            ORDER BY
                transaction_date DESC,
                amount DESC
            LIMIT {int(max_transactions)}
        """

        results = self._spark_service.execute_query_as_dicts(
            query=query,
            parameters=parameters,
            limit=max_transactions,
        )

        return [
            self._normalize_record(record)
            for record in results
        ]

    def get_merchant_fraud_pattern_breakdown(
        self,
        merchant: MerchantLocator,
        start_date: datetime.date | None = None,
        end_date: datetime.date | None = None,
    ) -> dict[str, Any]:
        """
        Summarize fraud patterns by transaction channel and error value.

        The merchant is first resolved against the Gold profile so all
        transaction queries use a complete and authoritative merchant locator.
        """
        self._validate_date_range(
            start_date=start_date,
            end_date=end_date,
        )

        resolved_merchant = self._resolve_merchant(merchant)

        merchant_filter, parameters = self._build_merchant_filter(
            resolved_merchant
        )

        date_filter, date_parameters = self._build_date_filter(
            start_date=start_date,
            end_date=end_date,
        )

        parameters.update(date_parameters)

        summary_query = f"""
            SELECT
                COUNT(*) AS fraud_transaction_count,
                COUNT(
                    DISTINCT user_id
                ) AS affected_customer_count,
                SUM(amount) AS total_fraud_amount,
                AVG(amount) AS average_fraud_amount,
                MIN(amount) AS minimum_fraud_amount,
                MAX(amount) AS maximum_fraud_amount,
                MIN(transaction_date) AS first_fraud_date,
                MAX(transaction_date) AS latest_fraud_date
            FROM {self._fraud_transactions_table}
            WHERE {merchant_filter}
              {date_filter}
        """

        summary_results = (
            self._spark_service.execute_query_as_dicts(
                query=summary_query,
                parameters=parameters,
                limit=1,
            )
        )

        channel_query = f"""
            SELECT
                COALESCE(
                    use_chip,
                    'Unknown'
                ) AS transaction_channel,
                COUNT(*) AS fraud_transaction_count,
                SUM(amount) AS fraud_amount,
                AVG(amount) AS average_fraud_amount
            FROM {self._fraud_transactions_table}
            WHERE {merchant_filter}
              {date_filter}
            GROUP BY
                COALESCE(use_chip, 'Unknown')
            ORDER BY
                fraud_transaction_count DESC
        """

        channel_results = (
            self._spark_service.execute_query_as_dicts(
                query=channel_query,
                parameters=parameters,
                limit=100,
            )
        )

        error_query = f"""
            SELECT
                COALESCE(
                    errors,
                    'No Error'
                ) AS error_category,
                COUNT(*) AS fraud_transaction_count,
                SUM(amount) AS fraud_amount
            FROM {self._fraud_transactions_table}
            WHERE {merchant_filter}
              {date_filter}
            GROUP BY
                COALESCE(errors, 'No Error')
            ORDER BY
                fraud_transaction_count DESC
        """

        error_results = (
            self._spark_service.execute_query_as_dicts(
                query=error_query,
                parameters=parameters,
                limit=100,
            )
        )

        summary = (
            self._normalize_record(summary_results[0])
            if summary_results
            else {}
        )

        return {
            "merchant": resolved_merchant.model_dump(),
            "date_range": {
                "start_date": start_date,
                "end_date": end_date,
            },
            "summary": summary,
            "channel_breakdown": [
                self._normalize_record(record)
                for record in channel_results
            ],
            "error_breakdown": [
                self._normalize_record(record)
                for record in error_results
            ],
        }

    def _resolve_merchant(
        self,
        merchant: MerchantLocator,
    ) -> MerchantLocator:
        """
        Resolve a partial merchant locator into a complete locator.

        The Gold merchant profile is treated as the authoritative source for
        city, state, and MCC values.
        """
        profile = self.get_merchant_risk_profile(merchant)

        return self._locator_from_profile(profile)

    @staticmethod
    def _locator_from_profile(
        profile: dict[str, Any],
    ) -> MerchantLocator:
        """
        Construct a complete MerchantLocator from a Gold profile record.

        MCC is normalized to a string because MerchantLocator uses a string
        representation while Spark may return MCC as an integer.
        """
        raw_mcc = profile.get("mcc")

        normalized_mcc = (
            str(raw_mcc).strip()
            if raw_mcc is not None
            else None
        )

        return MerchantLocator(
            merchant_name=str(profile["merchant_name"]).strip(),
            merchant_city=(
                str(profile["merchant_city"]).strip()
                if profile.get("merchant_city") is not None
                else None
            ),
            merchant_state=(
                str(profile["merchant_state"]).strip().upper()
                if profile.get("merchant_state") is not None
                else None
            ),
            mcc=normalized_mcc,
        )

    @staticmethod
    def _build_merchant_filter(
        merchant: MerchantLocator,
    ) -> tuple[str, dict[str, Any]]:
        """
        Build merchant predicates from the locator fields supplied.

        Merchant name is always included. City, state, and MCC are included
        only when they are not None.

        Returns:
            A SQL predicate string and its named parameter dictionary.
        """
        conditions = [
            "merchant_name <=> :merchant_name",
        ]

        parameters: dict[str, Any] = {
            "merchant_name": merchant.merchant_name,
        }

        if merchant.merchant_city is not None:
            conditions.append(
                "merchant_city <=> :merchant_city"
            )
            parameters["merchant_city"] = (
                merchant.merchant_city
            )

        if merchant.merchant_state is not None:
            conditions.append(
                "merchant_state <=> :merchant_state"
            )
            parameters["merchant_state"] = (
                merchant.merchant_state
            )

        if merchant.mcc is not None:
            conditions.append(
                "CAST(mcc AS STRING) <=> :mcc"
            )
            parameters["mcc"] = str(merchant.mcc)

        merchant_filter = "\n              AND ".join(
            conditions
        )

        return merchant_filter, parameters

    @staticmethod
    def _merchant_parameters(
        merchant: MerchantLocator,
    ) -> dict[str, Any]:
        """
        Build parameters from a fully resolved merchant locator.

        This method is used where merchant name, city, state, and MCC are all
        expected to be present, such as peer-group queries.
        """
        return {
            "merchant_name": merchant.merchant_name,
            "merchant_city": merchant.merchant_city,
            "merchant_state": merchant.merchant_state,
            "mcc": (
                str(merchant.mcc)
                if merchant.mcc is not None
                else None
            ),
        }

    @staticmethod
    def _build_date_filter(
        start_date: datetime.date | None,
        end_date: datetime.date | None,
    ) -> tuple[str, dict[str, Any]]:
        """
        Build an optional transaction-date SQL filter.

        Merchant parameters are intentionally not created here. Merchant and
        date filtering are separate responsibilities.
        """
        parameters: dict[str, Any] = {}
        conditions: list[str] = []

        if start_date is not None:
            conditions.append(
                "AND CAST(transaction_date AS DATE) "
                ">= :start_date"
            )
            parameters["start_date"] = start_date

        if end_date is not None:
            conditions.append(
                "AND CAST(transaction_date AS DATE) "
                "<= :end_date"
            )
            parameters["end_date"] = end_date

        return "\n              ".join(conditions), parameters

    @staticmethod
    def _validate_date_range(
        start_date: datetime.date | None,
        end_date: datetime.date | None,
    ) -> None:
        """Validate an optional transaction-date range."""
        if (
            start_date is not None
            and end_date is not None
            and start_date > end_date
        ):
            raise ValueError(
                "Start date cannot be after end date."
            )

    @staticmethod
    def _merchant_not_found_message(
        merchant: MerchantLocator,
    ) -> str:
        """Build a consistent merchant-not-found error message."""
        return (
            "Merchant was not found: "
            f"{merchant.merchant_name}, "
            f"{merchant.merchant_city}, "
            f"{merchant.merchant_state}, "
            f"MCC {merchant.mcc}."
        )

    @staticmethod
    def _merchant_ambiguous_message(
        merchant: MerchantLocator,
    ) -> str:
        """Build a consistent ambiguous-merchant error message."""
        return (
            "Multiple merchant records matched the supplied locator: "
            f"{merchant.merchant_name}, "
            f"{merchant.merchant_city}, "
            f"{merchant.merchant_state}, "
            f"MCC {merchant.mcc}. "
            "Provide additional city, state, or MCC information to "
            "uniquely identify the merchant."
        )

    @classmethod
    def _normalize_record(
        cls,
        record: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Convert Spark-specific values into plain Python values.

        Decimal values become floats. Date and datetime values are retained
        because Pydantic and JSON serializers can handle them.
        """
        return {
            key: cls._normalize_value(value)
            for key, value in record.items()
        }

    @staticmethod
    def _normalize_value(value: Any) -> Any:
        """Normalize one Spark result value."""
        if isinstance(value, Decimal):
            return float(value)

        return value

    @staticmethod
    def _to_float(value: Any) -> float:
        """Convert an optional numeric value to float."""
        if value is None:
            return 0.0

        return float(value)