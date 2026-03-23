import math
from typing import Any

import pandas as pd

from financials_tracker.mappers.concept_map_helper import ConceptMapHelper


class StatementValidationEngine:
    """
    Evaluate the quality and completeness of mapped financial statements.

    It does not modify the mapped output. Only inspects and reports: 
    - Mapped concepts
    - Missing concepts
    - Raw row mappings for eaach normalized concept
    """
    
    def __init__(self, concept_map_helper: ConceptMapHelper):
        self.concept_map_helper = concept_map_helper

    def validate_statement(
        self,
        raw_df: pd.DataFrame | None,
        mapped_df: pd.DataFrame | None,
        statement_type: str,
        tolerance: float = 1e-6,
    ) -> dict[str, Any]:
        """
        Run the full validation suite for one statement.

        Returns:
        - coverage
        - mapping detail
        - reconciliation checks
        """
        return {
            "statement_type": statement_type,
            "coverage": self.validate_coverage(mapped_df, statement_type),
            "mapping_detail": self.validate_mapping_detail(raw_df, mapped_df, statement_type),
            "reconciliation": self.validate_reconciliation(mapped_df, statement_type, tolerance),
        }

    def validate_coverage(
        self,
        mapped_df: pd.DataFrame | None,
        statement_type: str,
    ) -> dict[str, Any]:
        """
        Check how many expected normalized concepts are present in a mapped statement.

        Returns a summary including:
        - expected concepts
        - mapped concepts
        - missing concepts
        - per-concept non-null period counts (Fiscal years or quarters)
        """
        expected_concepts = set(
            self.concept_map_helper.concept_map.get(statement_type, {}).keys()
        )

        if mapped_df is None or mapped_df.empty:
            return {
                "statement_type": statement_type,
                "expected_concepts_count": len(expected_concepts),
                "mapped_concepts_count": 0,
                "coverage_ratio": 0.0,
                "mapped_concepts": [],
                "missing_concepts": sorted(expected_concepts),
                "non_null_period_counts": {},
            }

        mapped_concepts = set(mapped_df.index.tolist())
        missing_concepts = expected_concepts - mapped_concepts

        period_cols = self._get_period_columns(mapped_df)
        non_null_period_counts: dict[str, int] = {}

        # Add the amount of non-null periods for each mapped concept. 
        for concept in mapped_concepts:
            if period_cols:
                values = pd.to_numeric(mapped_df.loc[concept, period_cols], errors="coerce")
                non_null_period_counts[concept] = int(values.notna().sum())
            else:
                non_null_period_counts[concept] = 0

        coverage_ratio = (
            len(mapped_concepts & expected_concepts) / len(expected_concepts)
            if expected_concepts
            else 0.0
        )

        return {
            "statement_type": statement_type,
            "expected_concepts_count": len(expected_concepts),
            "mapped_concepts_count": len(mapped_concepts & expected_concepts),
            "coverage_ratio": coverage_ratio,
            "mapped_concepts": sorted(mapped_concepts & expected_concepts),
            "missing_concepts": sorted(missing_concepts),
            "non_null_period_counts": non_null_period_counts,
        }
    
    def validate_reconciliation(
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
        - cash_flow:
            free_cash_flow ≈ operating_cash_flow - capital_expenditures

        Tolerance is applied as a relative tolerance where possible.
        """
        if mapped_df is None or mapped_df.empty:
            return {
                "statement_type": statement_type,
                "checks": {},
            }

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

        if statement_type == "cash_flow":
            return {
                "statement_type": statement_type,
                "checks": {
                    "free_cash_flow_check": self._check_formula(
                        mapped_df=mapped_df,
                        left_concept="free_cash_flow",
                        right_a="operating_cash_flow",
                        right_b="capital_expenditures",
                        op="subtract",
                        tolerance=tolerance,
                    )
                },
            }

        return {
            "statement_type": statement_type,
            "checks": {},
        }
    
    def validate_mapping_detail(
        self,
        raw_df: pd.DataFrame | None,
        mapped_df: pd.DataFrame | None,
        statement_type: str,
    ) -> dict[str, Any]:
        """
        Produce a per-concept report describing which raw row appears to have been selected.

        For historical statements:
        - raw concept tags are expected in raw_df.index

        For latest statements:
        - raw concept tags are expected in raw_df['concept']

        Returns a mapping report keyed by normalized concept.
        """
        if raw_df is None or raw_df.empty or mapped_df is None or mapped_df.empty:
            return {
                "statement_type": statement_type,
                "details": {},
            }

        details: dict[str, Any] = {}

        # Historical-style raw statement: concept tags in index
        if raw_df.index.name == "concept" or self._has_historical_raw_format(raw_df):
            details = self._validate_historical_mapping_detail(
                raw_df=raw_df,
                mapped_df=mapped_df,
                statement_type=statement_type,
            )
        # Latest-style raw statement: concept tags in a column
        elif "concept" in raw_df.columns:
            details = self._validate_latest_mapping_detail(
                raw_df=raw_df,
                mapped_df=mapped_df,
                statement_type=statement_type,
            )
        else:
            details = {}

        return {
            "statement_type": statement_type,
            "details": details,
        }

    def _validate_historical_mapping_detail(
        self,
        raw_df: pd.DataFrame,
        mapped_df: pd.DataFrame,
        statement_type: str,
    ) -> dict[str, Any]:
        details: dict[str, Any] = {}
        period_cols = self._get_period_columns(raw_df)

        for raw_tag in raw_df.index:
            mapped = self.concept_map_helper.get_concept_from_tag(raw_tag)
            if not mapped or mapped["statement"] != statement_type:
                continue

            normalized_concept = mapped["concept"]

            if normalized_concept not in mapped_df.index:
                continue

            if normalized_concept in details:
                continue

            values = pd.to_numeric(raw_df.loc[raw_tag, period_cols], errors="coerce") if period_cols else pd.Series(dtype=float)

            details[normalized_concept] = {
                "raw_tag": raw_tag,
                "label": raw_df.loc[raw_tag, "label"] if "label" in raw_df.columns else None,
                "is_total": self._safe_scalar(raw_df, raw_tag, "is_total"),
                "is_abstract": self._safe_scalar(raw_df, raw_tag, "is_abstract"),
                "depth": self._safe_scalar(raw_df, raw_tag, "depth"),
                "section": self._safe_scalar(raw_df, raw_tag, "section"),
                "confidence": self._safe_scalar(raw_df, raw_tag, "confidence"),
                "non_null_periods": int(values.notna().sum()) if not values.empty else 0,
            }

        return details

    def _validate_latest_mapping_detail(
        self,
        raw_df: pd.DataFrame,
        mapped_df: pd.DataFrame,
        statement_type: str,
    ) -> dict[str, Any]:
        details: dict[str, Any] = {}

        for _, row in raw_df.iterrows():
            raw_tag = self._normalize_raw_tag(row.get("concept"))
            mapped = self.concept_map_helper.get_concept_from_tag(raw_tag)

            if not mapped or mapped["statement"] != statement_type:
                continue

            normalized_concept = mapped["concept"]

            if normalized_concept not in mapped_df.index:
                continue

            if normalized_concept in details:
                continue

            details[normalized_concept] = {
                "raw_tag": raw_tag,
                "label": row.get("label"),
                "standard_concept": row.get("standard_concept"),
            }

        return details

    def _check_formula(
        self,
        mapped_df: pd.DataFrame,
        left_concept: str,
        right_a: str,
        right_b: str,
        op: str,
        tolerance: float,
    ) -> dict[str, Any]:
        """
        Compare:
            left_concept
        against:
            right_a (op) right_b

        Returns a per-period difference summary plus a passed/failed flag.
        """
        period_cols = self._get_period_columns(mapped_df)

        required = {left_concept, right_a, right_b}
        if not required.issubset(set(mapped_df.index.tolist())):
            return {
                "passed": False,
                "reason": "missing_required_concepts",
                "missing_concepts": sorted(required - set(mapped_df.index.tolist())),
            }

        left = pd.to_numeric(mapped_df.loc[left_concept, period_cols], errors="coerce")
        a = pd.to_numeric(mapped_df.loc[right_a, period_cols], errors="coerce")
        b = pd.to_numeric(mapped_df.loc[right_b, period_cols], errors="coerce")

        if op == "subtract":
            expected = a - b
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
                    "left": self._to_python_scalar(left_val),
                    "expected": self._to_python_scalar(expected_val),
                    "difference": None,
                    "relative_difference": None,
                    "passed": None,
                }
                continue

            diff = float(left_val - expected_val)
            rel_diff = self._relative_difference(float(left_val), float(expected_val))
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

    @staticmethod
    def _relative_difference(a: float, b: float) -> float:
        """
            Relative difference measures how far apart two values are compared to their scale.
            Formula: absolute(a - b) / max(absolute(a), absolute(b), 1e-12)
            This makes the comparison proportional rather than using raw absolute distance.
            A small floor (1e-12) is used to avoid division by zero.
        """
        denominator = max(abs(a), abs(b), 1e-12)
        return abs(a - b) / denominator

    @staticmethod
    def _get_period_columns(df: pd.DataFrame) -> list[str]:
        return [col for col in df.columns if str(col).startswith(("FY ", "Q"))]

    @staticmethod
    def _safe_scalar(df: pd.DataFrame, row_key: Any, col_name: str) -> Any:
        if col_name not in df.columns:
            return None
        return StatementValidationEngine._to_python_scalar(df.loc[row_key, col_name])

    @staticmethod
    def _to_python_scalar(value: Any) -> Any:
        """
        Normalize values into plain Python-friendly values
        E.g: 
         - Some reports may have numpy types that are not JSON serializable     "confidence": np.float64(0.95)
         - This converts it to a regular Python float:     "confidence": 0.95
        """
        if pd.isna(value):
            return None
        if hasattr(value, "item"):
            try:
                return value.item()
            except Exception:
                return value
        return value

    @staticmethod
    def _normalize_raw_tag(raw_tag: Any) -> Any:
        if not isinstance(raw_tag, str):
            return raw_tag

        if "_" in raw_tag:
            return raw_tag.split("_", 1)[1]

        if ":" in raw_tag:
            return raw_tag.split(":", 1)[1]

        return raw_tag

    @staticmethod
    def _has_historical_raw_format(df: pd.DataFrame) -> bool:
        return any(str(col).startswith(("FY ", "Q")) for col in df.columns)