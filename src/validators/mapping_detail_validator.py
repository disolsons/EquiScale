from typing import Any

import pandas as pd
from src.processing.utils.concept_map_helper import ConceptMapHelper
from src.validators.base_validator import BaseValidator
from src.validators.utils.validator_utils import get_period_columns, has_historical_raw_format, normalize_raw_tag, safe_scalar, to_python_scalar

class StatementMappingDetailValidator(BaseValidator):
    """
    Mapping detail validator for mapped statement concepts.
    """

    def __init__(self, concept_map_helper: ConceptMapHelper):
        self.concept_map_helper = concept_map_helper

    def validate(
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

        if raw_df.index.name == "concept" or has_historical_raw_format(raw_df):
            details = self._validate_historical_mapping_detail(raw_df, mapped_df, statement_type)
        elif "concept" in raw_df.columns:
            details = self._validate_latest_mapping_detail(raw_df, mapped_df, statement_type)
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
        """Validate raw data provided in historical format """
        details: dict[str, Any] = {}
        period_cols = get_period_columns(raw_df)

        for raw_tag in raw_df.index:
            mapped = self.concept_map_helper.get_concept_from_tag(raw_tag)
            if not mapped or mapped["statement"] != statement_type:
                continue

            normalized_concept = mapped["concept"]
            if normalized_concept not in mapped_df.index:
                continue

            if normalized_concept in details:
                continue

            values = (
                pd.to_numeric(raw_df.loc[raw_tag, period_cols], errors="coerce")
                if period_cols
                else pd.Series(dtype=float)
            )

            details[normalized_concept] = {
                "raw_tag": raw_tag,
                "label": raw_df.loc[raw_tag, "label"] if "label" in raw_df.columns else None,
                "is_total": safe_scalar(df=raw_df, row_key=raw_tag, col_name="is_total"),
                "is_abstract": safe_scalar(df=raw_df, row_key=raw_tag, col_name="is_abstract"),
                "depth": safe_scalar(df=raw_df, row_key=raw_tag, col_name="depth"),
                "section": safe_scalar(df=raw_df, row_key=raw_tag, col_name="section"),
                "confidence": safe_scalar(df=raw_df, row_key=raw_tag, col_name="confidence"),
                "non_null_periods": int(values.notna().sum()) if not values.empty else 0,
            }

        return details

    def _validate_latest_mapping_detail(
        self,
        raw_df: pd.DataFrame,
        mapped_df: pd.DataFrame,
        statement_type: str,
    ) -> dict[str, Any]:
        """Validate raw data provided in latest format """
        details: dict[str, Any] = {}

        for _, row in raw_df.iterrows():
            raw_tag = normalize_raw_tag(row.get("concept"))
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


