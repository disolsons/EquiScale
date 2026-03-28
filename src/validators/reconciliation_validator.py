from typing import Any

import pandas as pd

from src.validators.base_validator import BaseValidator
from src.validators.utils.validator_utils import get_period_columns, relative_difference, to_python_scalar


class StatementReconciliationValidator(BaseValidator):
    """Reconciliation validator for mapped financial statements."""

    def validate(
        self,
        mapped_df: pd.DataFrame | None,
        statement_type: str,
        tolerance: float = 1e-6,
    ) -> dict[str, Any]:
        """
            Run basic reconciliation checks on a mapped statement.

            Current checks:
            - income_statement:
                gross_profit ≈ revenue - cost_of_revenue
            - balance_sheet:
                total_assets ≈ total_liabilities + shareholder_equity

            Tolerance is applied as a relative tolerance where possible.
        """
        if mapped_df is None or mapped_df.empty:
            return {"statement_type": statement_type, "checks": {}}

        if statement_type == "income_statement":
            return {
                "statement_type": statement_type,
                "checks": {
                    "gross_profit_check": self._check_formula(
                        mapped_df=mapped_df,
                        left_concept="gross_profit",
                        right_a="revenue",
                        right_b="cost_of_revenue",
                        op="subtract",
                        tolerance=tolerance,
                    )
                },
            }

        if statement_type == "balance_sheet":
            return {
                "statement_type": statement_type,
                "checks": {
                    "balance_sheet_equation_check": self._check_balance_sheet_equation(
                        mapped_df=mapped_df,
                        tolerance=tolerance,
                    )
                },
            }

        return {"statement_type": statement_type, "checks": {}}

    def _check_balance_sheet_equation(
        self,
        mapped_df: pd.DataFrame,
        tolerance: float,
    ) -> dict[str, Any]:
        """
         Check the balance sheet equation through preferred or fallback formulas.
         Preferred: total_assets ≈ total_liabilities_and_equity
         Fallback: total_assets ≈ total_liabilities + shareholder_equity

         Returns the result of the first check that can be performed based on available concepts.
         If neither check can be performed, returns a failed result with missing concepts info.
        """
        available = set(mapped_df.index.tolist())

        if {"total_assets", "total_liabilities_and_equity"}.issubset(available):
            result = self._check_formula(
                mapped_df=mapped_df,
                left_concept="total_assets",
                right_a="total_liabilities_and_equity",
                right_b=None,
                op="identity",
                tolerance=tolerance,
            )
            result["method"] = "total_assets_vs_total_liabilities_and_equity"
            return result

        if {"total_assets", "total_liabilities", "shareholder_equity"}.issubset(available):
            result = self._check_formula(
                mapped_df=mapped_df,
                left_concept="total_assets",
                right_a="total_liabilities",
                right_b="shareholder_equity",
                op="add",
                tolerance=tolerance,
            )
            result["method"] = "total_assets_vs_total_liabilities_plus_shareholder_equity"
            return result

        missing_for_preferred = sorted(
            {"total_assets", "total_liabilities_and_equity"} - available
        )
        missing_for_fallback = sorted(
            {"total_assets", "total_liabilities", "shareholder_equity"} - available
        )

        return {
            "passed": False,
            "reason": "missing_required_concepts",
            "preferred_method_missing": missing_for_preferred,
            "fallback_method_missing": missing_for_fallback,
        }

    def _check_formula(
        self,
        mapped_df: pd.DataFrame,
        left_concept: str,
        right_a: str,
        right_b: str | None,
        op: str,
        tolerance: float,
    ) -> dict[str, Any]:
        """
            Compare target concept to computed expected concept under given op.
            Supported ops: 
                - identity (right_a is expected to be approximately equal to left_concept) 
                - subtract
                - add
        """
        period_cols = get_period_columns(mapped_df)

        required = {left_concept, right_a}
        if right_b is not None:
            required.add(right_b)

        if not required.issubset(set(mapped_df.index.tolist())):
            return {
                "passed": False,
                "reason": "missing_required_concepts",
                "missing_concepts": sorted(required - set(mapped_df.index.tolist())),
            }

        left = pd.to_numeric(mapped_df.loc[left_concept, period_cols], errors="coerce")
        a = pd.to_numeric(mapped_df.loc[right_a, period_cols], errors="coerce")

        if op == "identity":
            expected = a
        elif op == "subtract":
            b = pd.to_numeric(mapped_df.loc[right_b, period_cols], errors="coerce")
            expected = a - b
        elif op == "add":
            b = pd.to_numeric(mapped_df.loc[right_b, period_cols], errors="coerce")
            expected = a + b
        else:
            raise ValueError(f"Unsupported reconciliation op: {op}")

        comparison: dict[str, Any] = {}
        passed = True
        max_relative_diff = 0.0

        for period in period_cols:
            left_val = left.get(period)
            expected_val = expected.get(period)

            if pd.isna(left_val) or pd.isna(expected_val):
                comparison[period] = {
                    "left": to_python_scalar(left_val),
                    "expected": to_python_scalar(expected_val),
                    "difference": None,
                    "relative_difference": None,
                    "passed": None,
                }
                continue

            diff = float(left_val - expected_val)
            rel_diff = relative_difference(float(left_val), float(expected_val))
            this_passed = rel_diff <= tolerance

            if not this_passed:
                passed = False

            max_relative_diff = max(max_relative_diff, rel_diff)

            comparison[period] = {
                "left": float(left_val),
                "expected": float(expected_val),
                "difference": diff,
                "relative_difference": rel_diff,
                "passed": this_passed,
            }

        return {
            "passed": passed,
            "max_relative_difference": max_relative_diff,
            "by_period": comparison,
        }

