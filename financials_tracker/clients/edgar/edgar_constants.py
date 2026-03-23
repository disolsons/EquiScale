from dataclasses import dataclass

@dataclass(frozen=True)
class EdgarConstants:
    """Constants specifically for SEC Edgar data processing."""
    STATEMENT_TYPE_INCOME: str = "income_statement"
    STATEMENT_TYPE_BALANCE_SHEET: str = "balance_sheet"
    STATEMENT_TYPE_CASH_FLOW: str = "cash_flow"