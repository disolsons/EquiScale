import pandas as pd
from financials_tracker.mappers.concept_map_helper import ConceptMapHelper
from financials_tracker.mappers.mapper_constants import MapperConstants
from financials_tracker.mappers.concept_candidate_ranker import ConceptCandidateRanker
from financials_tracker.mappers.model.concept_candidate import ConceptCandidate

class StatementMapper:
    """
    Mapper class to map the raw data from the EdgarClient to a structured format.  
    - read each raw tag from the row index
    - look it up with get_concept_from_tag
    - ignore rows that are unmapped
    - ignore abstract rows
    - keep only the period columns
    - output a normalized DataFrame keyed by your internal concepts 
    """    

    def __init__(self, concept_map_helper: ConceptMapHelper):
        self.concept_map_helper = concept_map_helper
        self.candidate_ranker = ConceptCandidateRanker()

    
    def map_historical_statement(self, statement_type: str, df: pd.DataFrame):
        """
            Convert a raw historical statement DataFrame into a normalized one.

            Input:
                - row index: raw SEC/XBRL tags
                - columns: metadata + FY / Q columns

            Output:
                - row index: normalized concept names
                - columns: fiscal period columns only
        """
        if df is None or df.empty:
            return None

        auxiliar_df = df.copy()

        # Remove abstract rows if present -
        # is_abstract is a flag returned by edgartools for structural rows such as headers or grouping tags.
        if MapperConstants.IS_ABSTRACT_COL in auxiliar_df.columns:
            auxiliar_df = auxiliar_df[auxiliar_df[MapperConstants.IS_ABSTRACT_COL] == False]

        # Keep only fiscal period columns (FY for fiscal year, Q for quarter).
        fiscal_period_cols = [
            col for col in auxiliar_df.columns
            if str(col).startswith((MapperConstants.FISCAL_YEAR_PREFIX, MapperConstants.QUARTER_PREFIX))
        ]

        if not fiscal_period_cols:
            return None

        concept_candidates: dict[str, list[ConceptCandidate]] = {}

        for raw_tag in auxiliar_df.index:
            mapped = self.concept_map_helper.get_concept_from_tag(raw_tag)
            if not mapped:
                continue

            # Check the statement type matches the expected one for this mapping
            if mapped["statement"] != statement_type:
                continue

            normalized_concept = mapped[MapperConstants.CONCEPT_COL]
            row_series = auxiliar_df.loc[raw_tag]
            row_dict = row_series.to_dict()

            candidate = ConceptCandidate(
                raw_tag=raw_tag,
                label=row_dict.get("label"),
                is_abstract=row_dict.get(MapperConstants.IS_ABSTRACT_COL),
                is_total=row_dict.get("is_total"),
                depth=row_dict.get("depth"),
                non_null_periods=self._count_non_null_periods(row_dict, fiscal_period_cols),
                row_data={col: row_dict.get(col) for col in fiscal_period_cols},
            )

            concept_candidates.setdefault(normalized_concept, []).append(candidate)

        if not concept_candidates:
            return None

        mapped_rows = {}

        for normalized_concept, candidate_rows in concept_candidates.items():
            best_candidate = self.candidate_ranker.select_best_candidate(candidate_rows)
            if best_candidate is not None:
                mapped_rows[normalized_concept] = best_candidate.row_data

        if not mapped_rows:
            return None

        # Transpose so fiscal periods are columns and normalized concepts are rows.
        result = pd.DataFrame(mapped_rows).T
        result.index.name = MapperConstants.CONCEPT_COL

        return result
    
    def map_latest_statement(self, statement_type: str, df: pd.DataFrame):
        """
            Map the latest statement DataFrame into normalized concepts.

            Expected input shape:
            - one row per statement line
            - raw SEC concept in a column named 'concept'
            - optional columns like 'label', 'standard_concept', etc.

            Returns a DataFrame with one row per normalized concept.
        """
        if df is None or df.empty:
            return None

        if MapperConstants.CONCEPT_COL not in df.columns:
            return None

        auxiliar_df = df.copy()

        # Normalize raw concept strings so they match YAML tags, removing prefixes like "us-gaap_" or namespaces.
        auxiliar_df["normalized_raw_tag"] = auxiliar_df[MapperConstants.CONCEPT_COL].apply(self._normalize_raw_tag)

        concept_candidates: dict[str, list[ConceptCandidate]] = {}

        for _, row in auxiliar_df.iterrows():
            raw_tag = row["normalized_raw_tag"]
            mapped = self.concept_map_helper.get_concept_from_tag(raw_tag)

            if not mapped:
                continue

            if mapped["statement"] != statement_type:
                continue

            normalized_concept = mapped[MapperConstants.CONCEPT_COL]
            row_dict = row.to_dict()

            # For latest statements, there are no FY/Q columns like historical statements.
            # Use presence of the row itself as a minimal completeness signal.
            candidate = ConceptCandidate(
                raw_tag=raw_tag,
                label=row_dict.get("label"),
                is_abstract=row_dict.get(MapperConstants.IS_ABSTRACT_COL),
                is_total=row_dict.get("is_total"),
                depth=row_dict.get("depth"),
                non_null_periods=1,
                row_data=row_dict,
            )

            concept_candidates.setdefault(normalized_concept, []).append(candidate)

        if not concept_candidates:
            return None

        mapped_rows = {}

        for normalized_concept, candidate_rows in concept_candidates.items():
            best_candidate = self.candidate_ranker.select_best_candidate(candidate_rows)
            if best_candidate is not None:
                mapped_rows[normalized_concept] = best_candidate.row_data

        if not mapped_rows:
            return None

        result = pd.DataFrame.from_dict(mapped_rows, orient="index")
        result.index.name = MapperConstants.CONCEPT_COL

        return result
    
    def _normalize_raw_tag(self, raw_tag: str) -> str:
        """
        Normalize Edgar/SEC concept strings to the bare tag used in the concept map.

        Examples:
        - us-gaap_RevenueFromContractWithCustomerExcludingAssessedTax
          -> RevenueFromContractWithCustomerExcludingAssessedTax
        - RevenueFromContractWithCustomerExcludingAssessedTax
          -> RevenueFromContractWithCustomerExcludingAssessedTax
        """
        if not isinstance(raw_tag, str):
            return raw_tag

        if "_" in raw_tag:
            return raw_tag.split("_", 1)[1]

        if ":" in raw_tag:
            return raw_tag.split(":", 1)[1]

        return raw_tag
    
    def _count_non_null_periods(self, row_dict: dict, period_cols: list[str]) -> int:
        """
        Count how many fiscal period columns in a row contain non-null values.
        """
        count = 0

        for col in period_cols:
            if pd.notna(row_dict.get(col)):
                count += 1

        return count