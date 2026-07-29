from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from typing import Any, Callable

from langchain_core.tools import BaseTool, tool
from pydantic import BaseModel, Field

from agentic_fraud.models.merchant import MerchantLocator
from agentic_fraud.services.merchant_service import MerchantService


class MerchantToolInput(BaseModel):
    """Common merchant identification fields used by merchant tools."""

    merchant_name: str = Field(
        ...,
        min_length=1,
        description=(
            "Merchant name to investigate, for example Walmart. "
            "Use the merchant name supplied by the user."
        ),
    )
    merchant_city: str | None = Field(
        default=None,
        description=(
            "Merchant city when known. Leave null for merchants that do not "
            "have a physical location, such as ONLINE merchants."
        ),
    )
    merchant_state: str | None = Field(
        default=None,
        description=(
            "Two-character US state code when known, for example MA. "
            "Leave null for online merchants."
        ),
    )
    mcc: str | int | None = Field(
        default=None,
        description="Merchant category code when supplied or resolved.",
    )


class MerchantPeerComparisonInput(MerchantToolInput):
    """Input for comparing one merchant with similar merchants."""

    peer_group_size: int = Field(
        default=10,
        ge=3,
        le=50,
        description="Maximum number of comparable merchants to include.",
    )


class HighRiskTransactionsInput(MerchantToolInput):
    """Input for retrieving suspicious merchant transactions."""

    start_date: date | None = Field(
        default=None,
        description="Optional inclusive start date in YYYY-MM-DD format.",
    )
    end_date: date | None = Field(
        default=None,
        description="Optional inclusive end date in YYYY-MM-DD format.",
    )
    max_transactions: int = Field(
        default=20,
        ge=1,
        le=100,
        description="Maximum number of suspicious transactions to return.",
    )


class RiskSignalBreakdownInput(MerchantToolInput):
    """Input for summarizing fraud-related signals for a merchant."""

    start_date: date | None = Field(
        default=None,
        description="Optional inclusive start date in YYYY-MM-DD format.",
    )
    end_date: date | None = Field(
        default=None,
        description="Optional inclusive end date in YYYY-MM-DD format.",
    )


def _build_locator(
    merchant_name: str,
    merchant_city: str | None,
    merchant_state: str | None,
    mcc: str | int | None,
) -> MerchantLocator:
    """
    Construct the shared merchant locator passed to MerchantService.

    MerchantLocator stores MCC as a string, but tool callers may supply
    either an integer such as 5411 or a string such as "5411".
    """
    normalized_mcc = str(mcc).strip() if mcc is not None else None

    return MerchantLocator(
        merchant_name=merchant_name.strip(),
        merchant_city=merchant_city.strip() if merchant_city else None,
        merchant_state=(
            merchant_state.strip().upper()
            if merchant_state
            else None
        ),
        mcc=normalized_mcc,
    )

def _to_serializable(value: Any) -> Any:
    """
    Recursively convert service results into JSON-safe Python values.

    Spark rows, Pydantic models, dates, and decimals should not be returned
    directly to the LLM because their representations can be inconsistent.
    """
    if value is None:
        return None

    if isinstance(value, BaseModel):
        return _to_serializable(value.model_dump())

    if hasattr(value, "asDict"):
        return _to_serializable(value.asDict(recursive=True))

    if isinstance(value, dict):
        return {
            str(key): _to_serializable(item)
            for key, item in value.items()
        }

    if isinstance(value, (list, tuple, set)):
        return [_to_serializable(item) for item in value]

    if isinstance(value, (date, Decimal)):
        return str(value)

    return value


def _json_result(
    tool_name: str,
    data: Any,
    message: str | None = None,
) -> str:
    """
    Return a consistent tool response envelope.

    The agent can reliably distinguish successful results from handled errors.
    """
    payload = {
        "tool": tool_name,
        "status": "success",
        "message": message,
        "data": _to_serializable(data),
    }

    return json.dumps(payload, indent=2, default=str)


def _json_error(
    tool_name: str,
    error: Exception,
) -> str:
    """
    Return a handled tool error rather than exposing a Python stack trace
    to the language model.
    """
    payload = {
        "tool": tool_name,
        "status": "error",
        "error_type": type(error).__name__,
        "message": str(error),
        "data": None,
    }

    return json.dumps(payload, indent=2)


def create_merchant_tools(
    merchant_service: MerchantService,
) -> list[BaseTool]:
    """
    Create merchant investigation tools bound to one MerchantService.

    The language model receives only these tool interfaces. It never receives
    direct access to Spark, SQL, Delta tables, or the SparkSession.
    """

    @tool(
        "get_merchant_risk_profile",
        args_schema=MerchantToolInput,
    )
    def get_merchant_risk_profile(
        merchant_name: str,
        merchant_city: str | None = None,
        merchant_state: str | None = None,
        mcc: str | int | None = None,
    ) -> str:
        """
        Retrieve the authoritative fraud-risk profile for one merchant.

        Use this tool first when investigating a merchant. It returns aggregate
        merchant activity such as transaction count, fraud count, fraud rate,
        transaction amounts, fraud amounts, and unique customers. Do not use it
        to retrieve individual transactions.
        """
        tool_name = "get_merchant_risk_profile"

        try:
            locator = _build_locator(
                merchant_name=merchant_name,
                merchant_city=merchant_city,
                merchant_state=merchant_state,
                mcc=mcc,
            )

            result = merchant_service.get_merchant_risk_profile(locator)

            return _json_result(
                tool_name=tool_name,
                data=result,
                message="Merchant risk profile retrieved successfully.",
            )

        except Exception as exc:
            return _json_error(tool_name, exc)

    @tool(
        "compare_merchant_to_peers",
        args_schema=MerchantPeerComparisonInput,
    )
    def compare_merchant_to_peers(
        merchant_name: str,
        merchant_city: str | None = None,
        merchant_state: str | None = None,
        mcc: str | int | None = None,
        peer_group_size: int = 10,
    ) -> str:
        """
        Compare one merchant with similar merchants in the same peer group.

        Use this tool when the investigation requires context about whether the
        merchant's fraud rate or activity is unusual relative to comparable
        merchants. The comparison should use the merchant category code whenever
        it is available.
        """
        tool_name = "compare_merchant_to_peers"

        try:
            locator = _build_locator(
                merchant_name=merchant_name,
                merchant_city=merchant_city,
                merchant_state=merchant_state,
                mcc=mcc,
            )

            result = merchant_service.compare_merchant_to_peers(
                locator=locator,
                peer_group_size=peer_group_size,
            )

            return _json_result(
                tool_name=tool_name,
                data=result,
                message="Merchant peer comparison completed successfully.",
            )

        except Exception as exc:
            return _json_error(tool_name, exc)

    @tool(
        "get_high_risk_transactions",
        args_schema=HighRiskTransactionsInput,
    )
    def get_high_risk_transactions(
        merchant_name: str,
        merchant_city: str | None = None,
        merchant_state: str | None = None,
        mcc: str | int | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        max_transactions: int = 20,
    ) -> str:
        """
        Retrieve the highest-risk transactions associated with one merchant.

        Use this tool when transaction-level evidence is required. Results should
        be limited to the requested date range and ordered with the strongest
        fraud indicators first. Do not use it for merchant-level aggregate
        statistics.
        """
        tool_name = "get_high_risk_transactions"

        try:
            if start_date and end_date and start_date > end_date:
                raise ValueError(
                    "start_date cannot be later than end_date."
                )

            locator = _build_locator(
                merchant_name=merchant_name,
                merchant_city=merchant_city,
                merchant_state=merchant_state,
                mcc=mcc,
            )

            result = merchant_service.get_high_risk_transactions(
                locator=locator,
                start_date=start_date,
                end_date=end_date,
                max_transactions=max_transactions,
            )

            return _json_result(
                tool_name=tool_name,
                data=result,
                message="High-risk transactions retrieved successfully.",
            )

        except Exception as exc:
            return _json_error(tool_name, exc)

    @tool(
        "get_risk_signal_breakdown",
        args_schema=RiskSignalBreakdownInput,
    )
    def get_risk_signal_breakdown(
        merchant_name: str,
        merchant_city: str | None = None,
        merchant_state: str | None = None,
        mcc: str | int | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> str:
        """
        Retrieve fraud-pattern details for one merchant.

        Use this tool to analyze transaction-channel distribution, error
        patterns, affected customers, fraud amounts, and the observed fraud
        date range.
        """
        tool_name = "get_risk_signal_breakdown"

        try:
            if start_date and end_date and start_date > end_date:
                raise ValueError(
                    "start_date cannot be later than end_date."
                )

            locator = _build_locator(
                merchant_name=merchant_name,
                merchant_city=merchant_city,
                merchant_state=merchant_state,
                mcc=mcc,
            )

            result = (
                merchant_service
                .get_merchant_fraud_pattern_breakdown(
                    merchant=locator,
                    start_date=start_date,
                    end_date=end_date,
                )
            )

            return _json_result(
                tool_name=tool_name,
                data=result,
                message=(
                    "Merchant fraud-pattern breakdown "
                    "retrieved successfully."
                ),
            )

        except Exception as exc:
            return _json_error(tool_name, exc)

    return [
        get_merchant_risk_profile,
        compare_merchant_to_peers,
        get_high_risk_transactions,
        get_risk_signal_breakdown,
    ]