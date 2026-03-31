from dataclasses import dataclass
import pandas as pd


@dataclass
class FinancialMetrics:
    """
    Collection of computed metric tables for one ticker.

    Each field is a metric family represented as a DataFrame where:
    - index = metric names
    - columns = fiscal periods
    """
    profitability: pd.DataFrame | None = None
    growth: pd.DataFrame | None = None
    cash_flow: pd.DataFrame | None = None
    balance_sheet: pd.DataFrame | None = None

    def as_dict(self) -> dict[str, pd.DataFrame | None]:
        return {
            "profitability": self.profitability,
            "growth": self.growth,
            "cash_flow": self.cash_flow,
            "balance_sheet": self.balance_sheet,
        }