from __future__ import annotations

from datetime import date

from pydantic import BaseModel, Field, model_validator

from agentic_fraud.models.merchant import MerchantLocator


class MerchantInvestigationRequest(BaseModel):
    """Input supplied to the merchant investigation agent."""

    merchant: MerchantLocator = Field(
        ...,
        description="Merchant location to investigate.",
    )

    question: str = Field(
        ...,
        min_length=5,
        max_length=2_000,
        description="The analyst's investigation question.",
    )

    start_date: date | None = Field(
        default=None,
        description="Optional investigation start date.",
    )

    end_date: date | None = Field(
        default=None,
        description="Optional investigation end date.",
    )

    peer_group_size: int = Field(
        default=10,
        ge=1,
        le=50,
        description="Maximum number of peer merchants used for comparison.",
    )

    max_transactions: int = Field(
        default=20,
        ge=1,
        le=100,
        description="Maximum number of fraud transactions to retrieve.",
    )

    @model_validator(mode="after")
    def validate_date_range(self) -> "MerchantInvestigationRequest":
        if (
            self.start_date is not None
            and self.end_date is not None
            and self.start_date > self.end_date
        ):
            raise ValueError(
                "start_date cannot be later than end_date"
            )

        return self