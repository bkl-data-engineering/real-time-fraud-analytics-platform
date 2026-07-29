from __future__ import annotations

from decimal import Decimal
from typing import Any

from pydantic import BaseModel, Field, field_validator

from agentic_fraud.models.common import (
    InvestigationStatus,
    RiskLevel,
)
from agentic_fraud.models.merchant import MerchantLocator


class EvidenceMetric(BaseModel):
    """A deterministic metric returned by an analytical tool."""

    metric_name: str = Field(
        ...,
        description="Human-readable name of the metric.",
    )

    metric_value: int | float | Decimal | str | None = Field(
        default=None,
        description="Observed value of the metric.",
    )

    comparison_value: int | float | Decimal | str | None = Field(
        default=None,
        description="Optional peer-group or historical comparison value.",
    )

    interpretation: str = Field(
        ...,
        description="Grounded explanation of why the metric matters.",
    )

    source_tool: str = Field(
        ...,
        description="Tool that produced this metric.",
    )


class RiskSignalFinding(BaseModel):
    """A risk-related finding supported by analytical evidence."""

    signal_name: str = Field(
        ...,
        description="Name of the identified fraud or risk pattern.",
    )

    severity: RiskLevel = Field(
        ...,
        description="Severity associated with the finding.",
    )

    evidence: str = Field(
        ...,
        description="Evidence supporting the finding.",
    )

    source_tool: str = Field(
        ...,
        description="Tool that produced the supporting evidence.",
    )


class TransactionEvidence(BaseModel):
    """Representative fraud transaction used as investigation evidence."""

    user_id: int | None = Field(
        default=None,
        description="Customer identifier associated with the transaction.",
    )

    card_index: int | None = Field(
        default=None,
        description="Card index associated with the transaction.",
    )

    transaction_date: str | None = Field(
        default=None,
        description="Date of the transaction.",
    )

    amount: Decimal | float | None = Field(
        default=None,
        description="Transaction amount.",
    )

    use_chip: str | None = Field(
        default=None,
        description="Transaction entry method.",
    )

    merchant_name: str | None = Field(
        default=None,
        description="Merchant name.",
    )

    merchant_city: str | None = Field(
        default=None,
        description="Merchant city.",
    )

    merchant_state: str | None = Field(
        default=None,
        description="Merchant state.",
    )

    mcc: str | None = Field(
        default=None,
        description="Merchant category code.",
    )

    card_brand: str | None = Field(
        default=None,
        description="Card brand associated with the transaction.",
    )

    card_type: str | None = Field(
        default=None,
        description="Card type associated with the transaction.",
    )

    errors: str | None = Field(
        default=None,
        description="Transaction-processing errors, when present.",
    )

    is_fraud: bool = Field(
        ...,
        description="Fraud label from the source dataset.",
    )


class PeerComparison(BaseModel):
    """Summary of the merchant's comparison with its peer group."""

    peer_group_description: str = Field(
        ...,
        description="Definition of the peer group used for comparison.",
    )

    merchant_fraud_rate: float | None = Field(
        default=None,
        ge=0,
        description="Fraud rate for the investigated merchant.",
    )

    peer_average_fraud_rate: float | None = Field(
        default=None,
        ge=0,
        description="Average fraud rate for the peer group.",
    )

    percentile_rank: float | None = Field(
        default=None,
        ge=0,
        le=100,
        description="Merchant percentile rank within the peer group.",
    )

    comparison_summary: str = Field(
        ...,
        description="Grounded explanation of the peer comparison.",
    )


class MerchantInvestigationReport(BaseModel):
    """
    Final structured output produced by the fraud investigation agent.

    Conclusions must be supported by deterministic analytical tool results.
    """

    investigation_status: InvestigationStatus = Field(
        ...,
        description="Whether the investigation completed successfully.",
    )

    merchant: MerchantLocator = Field(
        ...,
        description="Merchant location that was investigated.",
    )

    investigation_summary: str = Field(
        ...,
        description="Concise summary of the investigation.",
    )

    overall_risk_level: RiskLevel = Field(
        ...,
        description="Overall risk classification for the merchant.",
    )

    confidence_score: float = Field(
        ...,
        ge=0,
        le=1,
        description="Confidence in the findings, from 0.0 to 1.0.",
    )

    key_findings: list[RiskSignalFinding] = Field(
        default_factory=list,
        description="Primary findings supported by tool evidence.",
    )

    supporting_metrics: list[EvidenceMetric] = Field(
        default_factory=list,
        description="Metrics supporting the investigation findings.",
    )

    peer_comparison: PeerComparison | None = Field(
        default=None,
        description="Comparison against similar merchants.",
    )

    transaction_evidence: list[TransactionEvidence] = Field(
        default_factory=list,
        description="Representative fraud transactions.",
    )

    recommended_actions: list[str] = Field(
        default_factory=list,
        description="Recommended follow-up actions for an analyst.",
    )

    tools_used: list[str] = Field(
        default_factory=list,
        description="Analytical tools invoked during the investigation.",
    )

    limitations: list[str] = Field(
        default_factory=list,
        description="Data limitations or unresolved questions.",
    )

    investigation_metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Non-sensitive operational investigation metadata.",
    )

    @field_validator("tools_used")
    @classmethod
    def remove_duplicate_tools(cls, values: list[str]) -> list[str]:
        """
        Preserve tool invocation order while removing duplicate tool names.
        """

        return list(dict.fromkeys(values))