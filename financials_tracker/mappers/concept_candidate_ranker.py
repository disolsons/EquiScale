from difflib import SequenceMatcher
import re

from financials_tracker.mappers.model.concept_candidate import ConceptCandidate
from financials_tracker.mappers.tag_normalization_utils import normalize_text

class ConceptCandidateRanker:
    """
    Rank candidate raw-statement rows for a normalized concept and select the best one.
    """

    def select_best_candidate(
        self,
        concept: str,
        candidate_rows: list[ConceptCandidate],
    ) -> ConceptCandidate | None:  
        ranked = self.rank_candidates(concept, candidate_rows)
        if not ranked:
            return None

        best_score, best_candidate = ranked[0]

        if best_score <= -999.0:
            return None

        return best_candidate



    def rank_candidates(
        self,
        concept: str,
        candidate_rows: list[ConceptCandidate],
    ) -> list[tuple[float, ConceptCandidate]]:
        """
        Score and sort candidate rows from best to worst.
        """
        if not candidate_rows:
            return []

        scored_candidates = []
        for candidate in candidate_rows:
            score = self._score_candidate(candidate)
            scored_candidates.append((score, candidate))

        # Sorting priority:
        # - higher candidate score wins
        # - if scores tie, prefer the row with more non-null periods
        # - if still tied, prefer rows marked as total
        # - if still tied, prefer shallower rows (lower depth)
        scored_candidates.sort(
            key=lambda item: (
                item[0],
                item[1].non_null_periods,
                1 if item[1].is_total is True else 0,
                -(item[1].depth if item[1].depth is not None else 999),
                self._concept_tag_similarity(concept, item[1].raw_tag),
            ),
            reverse=True,
        )
        return scored_candidates

    
    def _score_candidate(self, candidate: ConceptCandidate) -> float:
        if candidate.is_abstract is True:
            return -999.0

        score = 0.0
        score += 2.0 * candidate.non_null_periods

        if candidate.is_total is True:
            score += 2.0

        if candidate.depth is not None:
            if candidate.depth <= 1:
                score += 1.0
            else:
                score -= 0.5 * candidate.depth

        return score
    

    def _concept_tag_similarity(self, concept: str, raw_tag: str) -> float:
        """
        Measure textual similarity between a normalized concept name and a raw tag.

        Used only as a final tie-break among already-approved candidates.
        """
        normalized_concept = normalize_text(concept)
        normalized_raw_tag = normalize_text(raw_tag)
        return SequenceMatcher(None, normalized_concept, normalized_raw_tag).ratio()