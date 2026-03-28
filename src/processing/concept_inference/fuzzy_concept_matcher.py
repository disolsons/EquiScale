import re
from src.processing.concept_inference.model.concept_suggestion import ConceptSuggestion
from src.processing.utils.tag_normalization_utils import normalize_text
from difflib import SequenceMatcher


class FuzzyConceptMatcher:
    def __init__(self, semantic_conflicts_helper):
        self.semantic_conflicts_helper = semantic_conflicts_helper

    def match(
        self,
        text_candidates: list[str],        
        aliases_for_statement: dict[str, list[str]],
    ) -> ConceptSuggestion | None:
        """
            Attempt to infer a concept using approximate string similarity.

            This is a fallback used only when keyword matching fails.

            The method:
            1. compares the normalized input text against concept names and aliases
            2. rejects candidates with hard semantic conflicts
            3. penalizes candidates with soft semantic conflicts
            4. returns the best surviving candidate if similarity is high enough
        """
        hard_conflicts = self.semantic_conflicts_helper.get_hard_conflicts()
        soft_conflicts = self.semantic_conflicts_helper.get_soft_conflicts()

        best_concept = None
        best_alias = None
        best_input_text = None
        best_adjusted_ratio = 0.0
        best_reasons: list[str] = []

        for concept, aliases in aliases_for_statement.items():
            candidates = [concept] + aliases

            for candidate in candidates:
                normalized_candidate = normalize_text(candidate)
                if not normalized_candidate:
                    continue

                for input_text in text_candidates:
                    # Reject immediately if a hard semantic conflict is detected.
                    if self._has_hard_conflict(input_text, normalized_candidate, hard_conflicts):
                        continue

                    raw_ratio = SequenceMatcher(None, input_text, normalized_candidate).ratio()

                    soft_conflict_count = self._count_soft_conflicts(
                        input_text,
                        normalized_candidate,
                        soft_conflicts,
                    )

                    adjusted_ratio = max(0.0, raw_ratio - (0.20 * soft_conflict_count))

                    if adjusted_ratio > best_adjusted_ratio:
                        best_adjusted_ratio = adjusted_ratio
                        best_concept = concept
                        best_alias = candidate
                        best_input_text = input_text

                        reasons = [
                            f"fuzzy match to '{candidate}'",
                            f"input text='{input_text}'",
                            f"raw ratio={raw_ratio:.3f}",
                        ]
                        if soft_conflict_count > 0:
                            reasons.append(
                                f"applied soft conflict penalty for {soft_conflict_count} semantic conflict(s)"
                            )
                        reasons.append(f"adjusted ratio={adjusted_ratio:.3f}")
                        best_reasons = reasons

        if best_concept is None:
            return None

        # Minimum threshold for any fuzzy suggestion
        if best_adjusted_ratio < 0.55:
            return None

        # Strong fuzzy match -> existing concept
        if best_adjusted_ratio >= 0.70:
            return ConceptSuggestion(
                suggested_concept=best_concept,
                suggestion_confidence=round(best_adjusted_ratio, 3),
                suggestion_type="existing_concept",
                suggestion_reason=best_reasons,
            )

        # Weak but potentially meaningful -> candidate for future review
        return ConceptSuggestion(
            suggested_concept=None,
            suggestion_confidence=round(best_adjusted_ratio, 3),
            suggestion_type="new_concept_candidate",
            suggestion_reason=best_reasons + [
                "closest match found, but confidence is below existing_concept threshold"
            ],
        ) 


    def _has_hard_conflict(
        self,
        text_a: str,
        text_b: str,
        hard_conflicts: list[tuple[str, str]],
    ) -> bool:
        """
        Return True if the two texts contain opposite tokens from a hard-conflict pair.

        Hard conflicts indicate mutually exclusive meanings where fuzzy matching
        should be rejected entirely.
        """
        for left, right in hard_conflicts:
            a_has_left = left in text_a
            a_has_right = right in text_a
            b_has_left = left in text_b
            b_has_right = right in text_b

            if (a_has_left and b_has_right) or (a_has_right and b_has_left):
                return True

        return False
    

    def _count_soft_conflicts(
        self,
        text_a: str,
        text_b: str,
        soft_conflicts: list[tuple[str, str]],
    ) -> int:
        """
        Count semantic mismatches between two texts using soft-conflict pairs.

        Soft conflicts do not fully reject a candidate, but reduce confidence.
        """
        conflicts = 0

        for left, right in soft_conflicts:
            a_has_left = left in text_a
            a_has_right = right in text_a
            b_has_left = left in text_b
            b_has_right = right in text_b

            if (a_has_left and b_has_right) or (a_has_right and b_has_left):
                conflicts += 1

        return conflicts
    