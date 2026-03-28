import pandas as pd

class MetricsCalculator:
    @staticmethod
    def divide(numerator: pd.Series | None, denominator: pd.Series | None) -> pd.Series | None:
        if numerator is None or denominator is None:
            return None
        return numerator / denominator.replace(0, pd.NA)

    @staticmethod
    def subtract(a: pd.Series | None, b: pd.Series | None) -> pd.Series | None:
        if a is None or b is None:
            return None
        return a - b

    @staticmethod
    def yoy_growth(series: pd.Series | None) -> pd.Series | None:
        if series is None or series.empty:
            return None
        return series.sort_index().pct_change()

    @staticmethod
    def cagr(series: pd.Series | None) -> float | None:
        if series is None or len(series) < 2:
            return None

        clean = series.dropna()
        if len(clean) < 2:
            return None

        start = clean.iloc[0]
        end = clean.iloc[-1]
        periods = len(clean) - 1

        if start <= 0 or periods <= 0:
            return None

        return (end / start) ** (1 / periods) - 1


    @staticmethod
    def average_with_previous_period(values: pd.Series | None) -> pd.Series | None:
        """
        For each period, compute the average of the current period and the prior period.

        Example:
        FY 2025 = (FY 2025 + FY 2024) / 2
        FY 2024 = (FY 2024 + FY 2023) / 2
        FY 2023 = NaN   # no earlier period available
        """
        if values is None or values.empty:
            return None

        sorted_values = values.sort_index()

        avg_values = (sorted_values + sorted_values.shift(1)) / 2
        return avg_values