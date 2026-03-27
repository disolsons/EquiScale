from dataclasses import dataclass
import pandas as pd


@dataclass
class FinancialDataset:
    income_statement: pd.DataFrame | None = None
    balance_sheet: pd.DataFrame | None = None
    cash_flow: pd.DataFrame | None = None