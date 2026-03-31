from dataclasses import dataclass
from src.domain.financial_report import FinancialReport
from src.domain.financial_metrics import FinancialMetrics

@dataclass
class FinancialDataset:
    """
    The ticker-level financial view, including statements and metrics.
    """
    ticker: str
    income_statement: FinancialReport | None = None
    balance_sheet: FinancialReport | None = None
    cash_flow: FinancialReport | None = None
    metrics: FinancialMetrics | None = None 
