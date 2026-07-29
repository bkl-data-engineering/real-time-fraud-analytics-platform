"""
Composed tool layer for merchant fraud investigations.

This module groups the individual merchant analytics tools into a cohesive
investigation toolkit. It does not combine them into a single large tool.

Keeping the tools granular allows the agent to select only the analytical
capabilities required for a given investigation while giving the application
one stable interface through which to access and validate them.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from langchain_core.tools import BaseTool

from agentic_fraud.services.merchant_service import MerchantService
from agentic_fraud.tools.merchant_tools import create_merchant_tools


EXPECTED_TOOL_NAMES: frozenset[str] = frozenset(
    {
        "get_merchant_risk_profile",
        "compare_merchant_to_peers",
        "get_high_risk_transactions",
        "get_risk_signal_breakdown",
    }
)


@dataclass(frozen=True)
class InvestigationCapability:
    """
    Describes one analytical capability available to the investigation agent.

    This metadata is application documentation. LangChain still receives the
    actual BaseTool objects returned by MerchantInvestigationToolkit.get_tools().
    """

    tool_name: str
    investigation_stage: str
    purpose: str


class MerchantInvestigationToolkit:
    """
    Provides the complete set of tools used for merchant fraud investigations.

    Responsibilities:
        1. Create the merchant investigation tools.
        2. Validate that the expected tools are registered.
        3. Expose the tools through a stable interface.
        4. Provide investigation-strategy guidance for the agent prompt.

    The toolkit deliberately contains no LLM or agent logic.
    """

    _CAPABILITIES: tuple[InvestigationCapability, ...] = (
        InvestigationCapability(
            tool_name="get_merchant_risk_profile",
            investigation_stage="Merchant baseline",
            purpose=(
                "Retrieve the merchant's transaction volume, fraud activity, "
                "fraud rate, amounts, and other aggregate risk measures."
            ),
        ),
        InvestigationCapability(
            tool_name="compare_merchant_to_peers",
            investigation_stage="Peer assessment",
            purpose=(
                "Compare the merchant with similar merchants in its peer group "
                "to determine whether its fraud behavior is unusual."
            ),
        ),
        InvestigationCapability(
            tool_name="get_risk_signal_breakdown",
            investigation_stage="Risk explanation",
            purpose=(
                "Identify the transaction characteristics and fraud indicators "
                "that contribute to the merchant's observed risk."
            ),
        ),
        InvestigationCapability(
            tool_name="get_high_risk_transactions",
            investigation_stage="Evidence collection",
            purpose=(
                "Retrieve representative high-risk or fraudulent transactions "
                "that support the investigation findings."
            ),
        ),
    )

    def __init__(self, merchant_service: MerchantService) -> None:
        """
        Initialize and validate the investigation toolkit.

        Args:
            merchant_service:
                Validated domain service used by the individual merchant tools.

        Raises:
            TypeError:
                If merchant_service is missing.
            RuntimeError:
                If the tool factory returns missing, unexpected, or duplicate
                tool registrations.
        """
        if merchant_service is None:
            raise TypeError("merchant_service must not be None.")

        self._merchant_service = merchant_service

        created_tools = create_merchant_tools(
            merchant_service=self._merchant_service
        )

        self._tools: tuple[BaseTool, ...] = tuple(created_tools)

        self._validate_tools()

    def get_tools(self) -> list[BaseTool]:
        """
        Return a new list containing all registered investigation tools.

        Returning a new list prevents callers from mutating the toolkit's
        internally stored collection.

        Returns:
            LangChain tools suitable for passing to create_agent().
        """
        return list(self._tools)

    def get_tool_names(self) -> tuple[str, ...]:
        """
        Return registered tool names in their configured order.
        """
        return tuple(tool.name for tool in self._tools)

    def get_capabilities(self) -> tuple[InvestigationCapability, ...]:
        """
        Return descriptive metadata for the investigation capabilities.
        """
        return self._CAPABILITIES

    def get_tool(self, tool_name: str) -> BaseTool:
        """
        Return one investigation tool by name.

        Args:
            tool_name:
                Registered LangChain tool name.

        Raises:
            ValueError:
                If the requested name is blank.
            KeyError:
                If no tool is registered under that name.
        """
        normalized_name = tool_name.strip()

        if not normalized_name:
            raise ValueError("tool_name must not be blank.")

        for tool in self._tools:
            if tool.name == normalized_name:
                return tool

        available_names = ", ".join(self.get_tool_names())

        raise KeyError(
            f"Unknown investigation tool '{normalized_name}'. "
            f"Available tools: {available_names}."
        )

    def get_strategy_prompt(self) -> str:
        """
        Return investigation guidance to embed in the agent system prompt.

        This provides a preferred analytical sequence without hard-coding an
        execution workflow. The agent may omit steps that are not relevant to
        the user's question.
        """
        return """
MERCHANT INVESTIGATION STRATEGY

Use the available tools as a coordinated fraud investigation toolkit.

Preferred analytical sequence:

1. ESTABLISH THE MERCHANT BASELINE
   Use get_merchant_risk_profile to retrieve verified merchant-level metrics
   before making factual claims about transaction volume, fraud activity,
   fraud rate, or financial amounts.

2. ASSESS THE MERCHANT AGAINST PEERS
   Use compare_merchant_to_peers when the question asks whether the merchant
   is unusually risky, how it ranks, or whether its behavior differs from
   comparable merchants.

3. EXPLAIN THE OBSERVED RISK
   Use get_risk_signal_breakdown when the investigation requires an
   explanation of the transaction characteristics or risk signals associated
   with the merchant's fraud activity.

4. COLLECT SUPPORTING EVIDENCE
   Use get_high_risk_transactions when concrete transaction examples are
   necessary to support the findings. Do not present a transaction example
   that was not returned by this tool.

INVESTIGATION RULES

- Use only the supplied tools for merchant-specific facts.
- Never invent merchant metrics, peer statistics, fraud signals, or
  transactions.
- Do not generate SQL or attempt to access Spark or Delta tables directly.
- Do not assume that a merchant exists merely because the user names it.
- If a tool reports that the merchant was not found, state that clearly.
- Distinguish verified tool results from analytical interpretation.
- Use only the tools needed to answer the question.
- Do not call additional tools merely to make the response appear more
  comprehensive.
- When sufficient evidence is available, conclude with a concise,
  non-prescriptive recommendation for further review.
""".strip()

    def _validate_tools(self) -> None:
        """
        Validate the tool collection created by the merchant tool factory.
        """
        if not self._tools:
            raise RuntimeError(
                "The merchant investigation toolkit contains no tools."
            )

        invalid_tools = [
            tool
            for tool in self._tools
            if not isinstance(tool, BaseTool)
        ]

        if invalid_tools:
            invalid_types = ", ".join(
                type(tool).__name__ for tool in invalid_tools
            )

            raise RuntimeError(
                "The merchant tool factory returned objects that are not "
                f"LangChain BaseTool instances: {invalid_types}."
            )

        tool_names = [tool.name for tool in self._tools]
        unique_tool_names = set(tool_names)

        if len(tool_names) != len(unique_tool_names):
            duplicate_names = sorted(
                {
                    name
                    for name in tool_names
                    if tool_names.count(name) > 1
                }
            )

            raise RuntimeError(
                "Duplicate merchant investigation tool names were found: "
                f"{', '.join(duplicate_names)}."
            )

        missing_names = EXPECTED_TOOL_NAMES - unique_tool_names
        unexpected_names = unique_tool_names - EXPECTED_TOOL_NAMES

        validation_errors: list[str] = []

        if missing_names:
            validation_errors.append(
                "missing tools: " + ", ".join(sorted(missing_names))
            )

        if unexpected_names:
            validation_errors.append(
                "unexpected tools: " + ", ".join(sorted(unexpected_names))
            )

        if validation_errors:
            raise RuntimeError(
                "Invalid merchant investigation toolkit configuration — "
                + "; ".join(validation_errors)
                + "."
            )


def create_investigation_toolkit(
    merchant_service: MerchantService,
) -> MerchantInvestigationToolkit:
    """
    Factory for constructing the composed merchant investigation toolkit.

    Args:
        merchant_service:
            Domain service used by the merchant analytics tools.

    Returns:
        A validated MerchantInvestigationToolkit.
    """
    return MerchantInvestigationToolkit(
        merchant_service=merchant_service
    )