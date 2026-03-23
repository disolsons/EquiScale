from typing import Any

import pandas as pd
from financials_tracker.mappers.concept_map_helper import ConceptMapHelper
from financials_tracker.validators.utils.validator_utils import get_period_columns


class StatementCoverageValidator:
    """
    Coverage validator for mapped statement concepts.
    """

    def __init__(self, concept_map_helper: ConceptMapHelper):
        self.concept_map_helper = concept_map_helper

    def validate(self, mapped_df: pd.DataFrame | None, statement_type: str) -> dict[str, Any]:
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

        period_cols = get_period_columns(mapped_df)
        non_null_period_counts: dict[str, int] = {}

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


