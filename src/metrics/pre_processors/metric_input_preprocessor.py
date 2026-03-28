from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import pandas as pd


@dataclass
class StockSplitDetectionResult:
    """
    Result of a stock-split heuristic check for a per-share series.
    """
    detected: bool
    split_factor: float | None
    adjusted_series: pd.Series | None
    reason: str | None


class MetricInputPreprocessor:
    """
    Preprocesses mapped financial input series before metric calculation.

    Current responsibilities:
    - detect likely stock-split basis mismatches in per-share series
    - normalize basic/diluted EPS to a consistent share basis when possible

    This is intentionally narrow in v1 and only applies to per-share metrics.
    """

    COMMON_SPLIT_FACTORS = (2.0, 3.0, 4.0, 5.0, 10.0)

    def __init__(
        self,
        split_ratio_tolerance: float = 0.20,
        net_income_stability_threshold: float = 0.60,
    ):
        """
        Parameters
        ----------
        split_ratio_tolerance
            Allowed relative distance from a common split factor.
            Example: 0.20 means a detected ratio within 20% of 2, 3, 4, 5, or 10 can qualify.

        net_income_stability_threshold
            Maximum absolute net-income YoY change allowed when using net income
            as a sanity check for split mismatch. Large EPS changes paired with
            relatively stable net income are more suspicious.
        """
        self.split_ratio_tolerance = split_ratio_tolerance
        self.net_income_stability_threshold = net_income_stability_threshold

    def prepare_metric_input(
        self,
        concept_name: str,
        series: pd.Series | None,
        *,
        net_income_series: pd.Series | None = None,
        shares_series: pd.Series | None = None,
    ) -> pd.Series | None:
        """
        Preprocess a single mapped concept series before metric calculation.

        Only per-share concepts are currently normalized.
        """
        if series is None or series.empty:
            return series

        #FIXME: currently we are only allowing normalization for these metrics, but we should not hardcode it here - needs to be refactored.
        if concept_name not in {"basic_eps", "diluted_eps"}:
            return series

        result = self.normalize_per_share_series(
            per_share_series=series,
            net_income_series=net_income_series,
            shares_series=shares_series,
        )

        return result.adjusted_series if result.detected and result.adjusted_series is not None else series

    def normalize_per_share_series(
        self,
        *,
        per_share_series: pd.Series | None,
        net_income_series: pd.Series | None = None,
        shares_series: pd.Series | None = None,
    ) -> StockSplitDetectionResult:
        """
        Detect and normalize likely stock-split basis mismatch in a per-share series.

        Strategy:
        - inspect adjacent periods
        - try direct EPS-ratio detection against common split factors
        - check whether weighted-average shares confirm that factor
        - if shares do not confirm or are unavailable, try inferring a split factor
        from net income growth + EPS change
        - if detected, restate earlier periods to the newest share basis

        Returns a StockSplitDetectionResult with the adjusted series when detected.
        """
        if per_share_series is None or per_share_series.empty:
            return StockSplitDetectionResult(
                detected=False,
                split_factor=None,
                adjusted_series=per_share_series,
                reason="empty_per_share_series",
            )

        clean_per_share_series = per_share_series.dropna()
        if len(clean_per_share_series) < 2:
            return StockSplitDetectionResult(
                detected=False,
                split_factor=None,
                adjusted_series=per_share_series,
                reason="not_enough_periods",
            )

        clean_per_share_series = self._sort_period_series(clean_per_share_series)

        detected_factor = None
        detected_transition_idx = None

        for index in range(len(clean_per_share_series) - 1):
            earlier_period = clean_per_share_series.index[index]
            later_period = clean_per_share_series.index[index + 1]

            earlier_value = clean_per_share_series.iloc[index]
            later_value = clean_per_share_series.iloc[index + 1]

            if earlier_value is None or later_value is None:
                continue
            if earlier_value <= 0 or later_value <= 0:
                continue

            # Direct EPS ratio can reveal simple split patterns.
            direct_ratio = earlier_value / later_value
            direct_split_factor = self._nearest_common_split_factor(direct_ratio)
            print(f"direct_split_factor:", direct_split_factor)
            shares_confirm = False
            if direct_split_factor is not None:
                # If a stock split occurred, later weighted-average shares should
                # increase by roughly the split factor.
                shares_confirm = self._confirm_split_with_shares(
                    shares_series=shares_series,
                    earlier_period=earlier_period,
                    later_period=later_period,
                    split_factor=direct_split_factor,
                )
            print(f"shares_confirm:", shares_confirm)
            income_implied_factor = None
            # If shares do not confirm (or are unavailable), infer the split factor
            # from the relationship between net income growth and EPS change.
            if net_income_series is not None:
                income_implied_factor = self._infer_split_factor_from_income_and_eps_periods(
                    net_income_series=net_income_series,
                    earlier_period=earlier_period,
                    later_period=later_period,
                    earlier_eps=earlier_value,
                    later_eps=later_value,
                )
            print(f"income_implied_factor:",income_implied_factor)
            # Priority:
            # 1) If shares confirm the direct factor, trust it.
            # 2) Else, if income+EPS imply a clean factor, use that.
            chosen_factor = None
            if shares_confirm:
                chosen_factor = direct_split_factor
            elif income_implied_factor is not None:
                chosen_factor = income_implied_factor
            elif direct_split_factor is not None and shares_series is None:
                # Last-resort fallback for simple cases where direct EPS ratio looks
                # like a clean split and no share data is available.
                chosen_factor = direct_split_factor

            if chosen_factor is None:
                continue

            detected_factor = chosen_factor
            detected_transition_idx = index
            break

        if detected_factor is None or detected_transition_idx is None:
            return StockSplitDetectionResult(
                detected=False,
                split_factor=None,
                adjusted_series=per_share_series,
                reason="no_split_pattern_detected",
            )

        adjusted = clean_per_share_series.copy()

        # Restate all periods up to and including the earlier side of the detected
        # transition to the newer share basis.
        for index in range(detected_transition_idx + 1):
            adjusted.iloc[index] = adjusted.iloc[index] / detected_factor

        full_adjusted = per_share_series.copy()
        for period, value in adjusted.items():
            full_adjusted.loc[period] = value

        return StockSplitDetectionResult(
            detected=True,
            split_factor=detected_factor,
            adjusted_series=full_adjusted,
            reason=f"adjusted_earlier_periods_by_factor_{detected_factor}",
        )

    def _confirm_split_with_shares(
        self,
        *,
        shares_series: pd.Series | None,
        earlier_period: str,
        later_period: str,
        split_factor: float,
    ) -> bool:
        """
        Confirm a suspected split using weighted-average shares if available.
        """
        if shares_series is None or shares_series.empty:
            return False

        shares_series = self._sort_period_series(shares_series.dropna())
        if earlier_period not in shares_series.index or later_period not in shares_series.index:
            return False

        earlier_shares = shares_series.loc[earlier_period]
        later_shares = shares_series.loc[later_period]

        if earlier_shares is None or later_shares is None:
            return False
        if earlier_shares <= 0 or later_shares <= 0:
            return False
        
        # If a stock split occurred, later weighted-average shares should be higher than earlier shares by roughly the split factor.
        observed_ratio = later_shares / earlier_shares
        return self._is_close_to_factor(observed_ratio, split_factor)

    def _confirm_split_with_net_income(
        self,
        *,
        net_income_series: pd.Series | None,
        earlier_period: str,
        later_period: str,
    ) -> bool:
        """
        Use net income as a sanity check.

        If EPS appears to change by a clean split factor but net income does not
        change dramatically between the same periods, that supports a split-basis mismatch.
        """
        if net_income_series is None or net_income_series.empty:
            return False

        net_income_series = self._sort_period_series(net_income_series.dropna())
        if earlier_period not in net_income_series.index or later_period not in net_income_series.index:
            return False

        earlier_income = net_income_series.loc[earlier_period]
        later_income = net_income_series.loc[later_period]

        if earlier_income is None or later_income is None:
            return False
        if earlier_income == 0:
            return False
        
        # Measure how much net income changed between periods; 
        # a relatively stable income trend supports that the EPS jump is due to share-basis change rather than business performance.
        yoy_change = abs((later_income / earlier_income) - 1.0)
        return yoy_change <= self.net_income_stability_threshold

    def _infer_split_factor_from_income_and_eps_periods(
        self,
        *,
        net_income_series: pd.Series,
        earlier_period: str,
        later_period: str,
        earlier_eps: float,
        later_eps: float,
    ) -> float | None:
        """
            Infer a likely stock-split factor from the relationship between net income growth and EPS change across two periods.
        """

        clean_income = self._sort_period_series(net_income_series.dropna())

        if earlier_period not in clean_income.index or later_period not in clean_income.index:
            return None

        earlier_income = clean_income.loc[earlier_period]
        later_income = clean_income.loc[later_period]

        if earlier_income is None or later_income is None:
            return None
        if earlier_income <= 0 or later_income <= 0:
            return None
        if earlier_eps <= 0 or later_eps <= 0:
            return None

        income_ratio = later_income / earlier_income
        eps_ratio = later_eps / earlier_eps

        if eps_ratio == 0:
            return None

        implied_share_factor = income_ratio / eps_ratio
        return self._nearest_common_split_factor(implied_share_factor)

    def _nearest_common_split_factor(self, ratio: float) -> float | None:
        """
        Return the nearest common split factor if the observed ratio is close enough.
        """
        for factor in self.COMMON_SPLIT_FACTORS:
            if self._is_close_to_factor(ratio, factor):
                return factor
        return None

    def _is_close_to_factor(self, observed_ratio: float, factor: float) -> bool:
        """
        Check whether an observed ratio is close to a common split factor.
        """
        relative_error = abs(observed_ratio - factor) / factor
        return relative_error <= self.split_ratio_tolerance

    def _sort_period_series(self, series: pd.Series) -> pd.Series:
        """
        Sort period labels like 'FY 2024', 'FY 2025' in chronological order.

        Falls back to standard index sorting if parsing fails.
        """
        try:
            return series.sort_index(key=lambda idx: [self._extract_period_number(x) for x in idx])
        except Exception:
            return series.sort_index()

    def _extract_period_number(self, label: str) -> int:
        """
        Extract trailing numeric year/period from labels like 'FY 2025'.
        """
        digits = "".join(ch for ch in str(label) if ch.isdigit())
        return int(digits) if digits else 0