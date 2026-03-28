import pandas as pd
from src.processing.utils.concept_map_helper import ConceptMapHelper
from src.processing.mappers.mapper_constants import MapperConstants
from src.processing.mappers.concept_candidate_ranker import ConceptCandidateRanker
from src.processing.mappers.model.concept_candidate import ConceptCandidate
from src.processing.mappers.model.concept_selection_metadata import ConceptSelectionMetadata
from src.processing.mappers.model.statement_mapping_result import StatementMappingResult

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

    def map_historical_statement(
        self,
        statement_type: str,
        df: pd.DataFrame,
    ) -> StatementMappingResult:
        """
            Convert a raw historical statement DataFrame into a normalized one.

            Returns both:
            - mapped_df: normalized concept-value table
            - selection_metadata: candidate-level metadata for all evaluated candidates
        """
        if df is None or df.empty:
            return StatementMappingResult(mapped_df=None, selection_metadata=[])

        auxiliar_df = df.copy()

        if MapperConstants.IS_ABSTRACT_COL in auxiliar_df.columns:
            auxiliar_df = auxiliar_df[auxiliar_df[MapperConstants.IS_ABSTRACT_COL] == False]

        fiscal_period_cols = [
            col for col in auxiliar_df.columns
            if str(col).startswith(
                (MapperConstants.FISCAL_YEAR_PREFIX, MapperConstants.QUARTER_PREFIX)
            )
        ]

        if not fiscal_period_cols:
            return StatementMappingResult(mapped_df=None, selection_metadata=[])

        concept_candidates: dict[str, list[ConceptCandidate]] = {}

        for raw_tag in auxiliar_df.index:
            mapped = self.concept_map_helper.get_concept_from_tag(raw_tag)
            if not mapped:
                continue

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
            return StatementMappingResult(mapped_df=None, selection_metadata=[])

        mapped_rows: dict[str, dict] = {}
        selection_metadata: list[ConceptSelectionMetadata] = []

        for normalized_concept, candidate_rows in concept_candidates.items():
            ranked_candidates = self.candidate_ranker.rank_candidates(normalized_concept, candidate_rows)
            if not ranked_candidates:
                continue

            candidate_count = len(ranked_candidates)

            for idx, (score, candidate) in enumerate(ranked_candidates, start=1):
                is_selected = idx == 1 and score > -999.0

                selection_metadata.append(
                    self._build_selection_metadata(
                        concept=normalized_concept,
                        candidate=candidate,
                        candidate_score=score,
                        is_selected=is_selected,
                        rank_order=idx,
                        candidate_count=candidate_count,
                    )
                )

            best_score, best_candidate = ranked_candidates[0]
            if best_score <= -999.0:
                continue

            mapped_rows[normalized_concept] = best_candidate.row_data

        if not mapped_rows:
            return StatementMappingResult(mapped_df=None, selection_metadata=selection_metadata)

        result_df = pd.DataFrame(mapped_rows).T
        result_df.index.name = MapperConstants.CONCEPT_COL

        return StatementMappingResult(
            mapped_df=result_df,
            selection_metadata=selection_metadata,
        )    

    def map_latest_statement(
        self,
        statement_type: str,
        df: pd.DataFrame,
    ) -> StatementMappingResult:
            """
                Map the latest-statement DataFrame into normalized concepts.

                Returns both:
                - mapped_df: normalized concept rows
                - selection_metadata: candidate-level metadata for all evaluated candidates
            """
            if df is None or df.empty:
                return StatementMappingResult(mapped_df=None, selection_metadata=[])

            if MapperConstants.CONCEPT_COL not in df.columns:
                return StatementMappingResult(mapped_df=None, selection_metadata=[])

            auxiliar_df = df.copy()
            auxiliar_df["normalized_raw_tag"] = auxiliar_df[MapperConstants.CONCEPT_COL].apply(
                self._normalize_raw_tag
            )

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
                return StatementMappingResult(mapped_df=None, selection_metadata=[])

            mapped_rows: dict[str, dict] = {}
            selection_metadata: list[ConceptSelectionMetadata] = []

            for normalized_concept, candidate_rows in concept_candidates.items():
                ranked_candidates = self.candidate_ranker.rank_candidates(normalized_concept, candidate_rows)
                if not ranked_candidates:
                    continue

                candidate_count = len(ranked_candidates)

                for idx, (score, candidate) in enumerate(ranked_candidates, start=1):
                    is_selected = idx == 1 and score > -999.0

                    selection_metadata.append(
                        self._build_selection_metadata(
                            concept=normalized_concept,
                            candidate=candidate,
                            candidate_score=score,
                            is_selected=is_selected,
                            rank_order=idx,
                            candidate_count=candidate_count,
                        )
                    )

                best_score, best_candidate = ranked_candidates[0]
                if best_score <= -999.0:
                    continue

                mapped_rows[normalized_concept] = best_candidate.row_data

            if not mapped_rows:
                return StatementMappingResult(mapped_df=None, selection_metadata=selection_metadata)

            result_df = pd.DataFrame.from_dict(mapped_rows, orient="index")
            result_df.index.name = MapperConstants.CONCEPT_COL

            return StatementMappingResult(
                mapped_df=result_df,
                selection_metadata=selection_metadata,
            )
    
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

    def _build_selection_metadata(
        self,
        concept: str,
        candidate: ConceptCandidate,
        candidate_score: float,
        is_selected: bool,
        rank_order: int,
        candidate_count: int,
    ) -> ConceptSelectionMetadata:
        """
            Build candidate-level mapping provenance metadata for one concept candidate.
        """
        return ConceptSelectionMetadata(
            concept=concept,
            raw_tag=candidate.raw_tag,
            label=candidate.label,
            is_abstract=candidate.is_abstract,
            is_total=candidate.is_total,
            depth=candidate.depth,
            non_null_periods=candidate.non_null_periods,
            candidate_score=candidate_score,
            is_selected=is_selected,
            rank_order=rank_order,
            candidate_count=candidate_count,
        )