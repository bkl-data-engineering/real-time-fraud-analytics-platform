from enum import Enum


class RiskLevel(str, Enum):
    """Supported investigation risk classifications."""

    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"
    UNKNOWN = "UNKNOWN"


class InvestigationStatus(str, Enum):
    """Possible outcomes of an agent investigation."""

    COMPLETED = "COMPLETED"
    INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"
    FAILED = "FAILED"