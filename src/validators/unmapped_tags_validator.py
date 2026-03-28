from typing import Any

import pandas as pd

from src.processing.utils.concept_map_helper import ConceptMapHelper
from src.validators.base_validator import BaseValidator
from src.validators.utils.validator_utils import get_period_columns, normalize_raw_tag, relative_difference, safe_scalar, to_python_scalar


class UnmappedTagsValidator(BaseValidator):
    """
    Validator to identify which tags in the raw statement where not mapped to any normalized concept.
    """
    def __init__(self, concept_map_helper: ConceptMapHelper):
        self.concept_map_helper = concept_map_helper
    
    def validate(
        self,
        raw_df: pd.DataFrame | None,
        statement_type: str,
        ignored_tags: set[str] | None = None,
    ) -> dict[str, Any]:
        """
        Report raw tags that were not mapped to any normalized concept for the given statement type.

        Supports both:
        - historical raw statements (raw tags in index)
        - latest raw statements (raw tags in 'concept' column)

        Allows for a list of tags to be ignored.
        """
        if raw_df is None or raw_df.empty:
            return {
                "statement_type": statement_type,
                "total_raw_rows": 0,
                "mapped_raw_tags": [],
                "ignored_raw_tags": [],
                "unmapped_raw_tags": [],
            }

        if ignored_tags is None:
            ignored_tags = set()

        if raw_df.index.name == "concept" or self._looks_like_historical_raw(raw_df):
            return self._validate_unmapped_tags_historical(
                raw_df=raw_df,
                statement_type=statement_type,
                ignored_tags=ignored_tags,
            )

        if "concept" in raw_df.columns:
            return self._validate_unmapped_tags_latest(
                raw_df=raw_df,
                statement_type=statement_type,
                ignored_tags=ignored_tags,
            )

        return {
            "statement_type": statement_type,
            "total_raw_rows": 0,
            "mapped_raw_tags": [],
            "ignored_raw_tags": [],
            "unmapped_raw_tags": [],
            "reason": "unrecognized_raw_statement_shape",
        }
    
    def _validate_unmapped_tags_historical(
        self,
        raw_df: pd.DataFrame,
        statement_type: str,
        ignored_tags: set[str],
    ) -> dict[str, Any]:
        
        period_cols = get_period_columns(raw_df)
        mapped_raw_tags = []
        ignored_raw_tags = []
        unmapped_raw_tags = []

        for raw_tag in raw_df.index:
            mapped = self.concept_map_helper.get_concept_from_tag(raw_tag)

            if mapped and mapped["statement"] == statement_type:
                mapped_raw_tags.append(raw_tag)
                continue

            if raw_tag in ignored_tags:
                ignored_raw_tags.append(raw_tag)
                continue

            values = (
                pd.to_numeric(raw_df.loc[raw_tag, period_cols], errors="coerce")
                if period_cols else pd.Series(dtype=float)
            )

            unmapped_raw_tags.append({
                "raw_tag": raw_tag,
                "label": raw_df.loc[raw_tag, "label"] if "label" in raw_df.columns else None,
                "is_total": safe_scalar(raw_df, raw_tag, "is_total"),
                "is_abstract": safe_scalar(raw_df, raw_tag, "is_abstract"),
                "depth": safe_scalar(raw_df, raw_tag, "depth"),
                "section": safe_scalar(raw_df, raw_tag, "section"),
                "confidence": safe_scalar(raw_df, raw_tag, "confidence"),
                "non_null_periods": int(values.notna().sum()) if not values.empty else 0,
            })

        return {
            "statement_type": statement_type,
            "total_raw_rows": int(len(raw_df.index)),
            "mapped_raw_tags": sorted(mapped_raw_tags),
            "ignored_raw_tags": sorted(ignored_raw_tags),
            "unmapped_raw_tags": unmapped_raw_tags,
        }
    
    def _validate_unmapped_tags_latest(
    self,
    raw_df: pd.DataFrame,
    statement_type: str,
    ignored_tags: set[str],
    ) -> dict[str, Any]:
        
        mapped_raw_tags = []
        ignored_raw_tags = []
        unmapped_raw_tags = []

        for _, row in raw_df.iterrows():
            raw_tag = normalize_raw_tag(row.get("concept"))

            mapped = self.concept_map_helper.get_concept_from_tag(raw_tag)

            if mapped and mapped["statement"] == statement_type:
                mapped_raw_tags.append(raw_tag)
                continue

            if raw_tag in ignored_tags:
                ignored_raw_tags.append(raw_tag)
                continue

            unmapped_raw_tags.append({
                "raw_tag": raw_tag,
                "label": row.get("label"),
                "standard_concept": row.get("standard_concept"),
            })

        return {
            "statement_type": statement_type,
            "total_raw_rows": int(len(raw_df)),
            "mapped_raw_tags": sorted(set(mapped_raw_tags)),
            "ignored_raw_tags": sorted(set(ignored_raw_tags)),
            "unmapped_raw_tags": unmapped_raw_tags,
        }