from __future__ import annotations

from pydantic import BaseModel, Field, field_validator


class MerchantLocator(BaseModel):
    """
    Business attributes used to identify a merchant location.

    Merchant name alone may not uniquely identify a merchant. City, state,
    and merchant category code can be supplied to narrow the investigation.
    """

    merchant_name: str = Field(
        ...,
        min_length=1,
        description="Merchant name to investigate.",
    )

    merchant_city: str | None = Field(
        default=None,
        description="Optional merchant city used to narrow the investigation.",
    )

    merchant_state: str | None = Field(
        default=None,
        min_length=2,
        max_length=2,
        description="Optional two-character merchant state code.",
    )

    mcc: str | None = Field(
        default=None,
        description="Optional merchant category code.",
    )

    @field_validator("merchant_name")
    @classmethod
    def normalize_merchant_name(cls, value: str) -> str:
        cleaned_value = value.strip()

        if not cleaned_value:
            raise ValueError("merchant_name cannot be empty")

        return cleaned_value

    @field_validator("merchant_city")
    @classmethod
    def normalize_merchant_city(
        cls,
        value: str | None,
    ) -> str | None:
        if value is None:
            return None

        cleaned_value = value.strip()

        return cleaned_value or None

    @field_validator("merchant_state")
    @classmethod
    def normalize_merchant_state(
        cls,
        value: str | None,
    ) -> str | None:
        if value is None:
            return None

        cleaned_value = value.strip().upper()

        if len(cleaned_value) != 2:
            raise ValueError(
                "merchant_state must be a two-character state code"
            )

        return cleaned_value

    @field_validator("mcc")
    @classmethod
    def normalize_mcc(
        cls,
        value: str | None,
    ) -> str | None:
        if value is None:
            return None

        cleaned_value = value.strip()

        return cleaned_value or None