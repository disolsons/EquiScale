import re
from dataclasses import dataclass
from difflib import SequenceMatcher

from financials_tracker.mappers.concept_inference.concept_keywords import CONCEPT_KEYWORDS


@dataclass
class ConceptSuggestion:
    suggested_concept: str | None
    suggestion_confidence: float
    suggestion_type: str
    suggestion_reason: list[str]


class ConceptInferenceEngine:
    def __init__(self, concept_keywords: dict | None = None):
        self.concept_keywords = concept_keywords or CONCEPT_KEYWORDS

    def suggest_concept(
        self,
        statement_type: str,
        raw_tag: str | None,
        label: str | None,
    ) -> dict:
        raw_tag = raw_tag or ""
        label = label or ""

        normalized_tag = self._normalize_text(raw_tag)
        normalized_label = self._normalize_text(label)
        combined_text = f"{normalized_tag} {normalized_label}".strip()

        ignore_result = self._check_ignore_candidate(raw_tag, label, combined_text)
        if ignore_result is not None:
            return ignore_result.__dict__

        keywords_for_statement = self.concept_keywords.get(statement_type, {})
        if not keywords_for_statement:
            return ConceptSuggestion(
                suggested_concept=None,
                suggestion_confidence=0.0,
                suggestion_type="unknown",
                suggestion_reason=[f"no keyword dictionary for statement_type={statement_type}"],
            ).__dict__

        keyword_match = self._keyword_match(keywords_for_statement, combined_text)
        if keyword_match is not None:
            return keyword_match.__dict__

        fuzzy_match = self._fuzzy_match(keywords_for_statement, combined_text)
        if fuzzy_match is not None:
            return fuzzy_match.__dict__

        return ConceptSuggestion(
            suggested_concept=None,
            suggestion_confidence=0.0,
            suggestion_type="new_concept_candidate",
            suggestion_reason=["no strong existing concept match found"],
        ).__dict__

    def _check_ignore_candidate(
        self,
        raw_tag: str,
        label: str,
        combined_text: str,
    ) -> ConceptSuggestion | None:
        reasons = []

        if raw_tag.endswith("Abstract"):
            reasons.append("raw tag ends with Abstract")

        if "abstract" in combined_text:
            reasons.append("text contains abstract")

        header_phrases = [
            "cash flows from operating activities",
            "cash flows from investing activities",
            "cash flows from financing activities",
            "supplemental cash flow information",
            "adjustments to reconcile",
            "changes in operating assets and liabilities",
        ]
        if any(phrase in combined_text for phrase in header_phrases):
            reasons.append("looks like section header / presentation row")

        if reasons:
            return ConceptSuggestion(
                suggested_concept=None,
                suggestion_confidence=0.95,
                suggestion_type="ignore_candidate",
                suggestion_reason=reasons,
            )

        return None

    def _keyword_match(
        self,
        keywords_for_statement: dict[str, list[str]],
        combined_text: str,
    ) -> ConceptSuggestion | None:
        best_concept = None
        best_hits = []
        best_score = 0.0

        for concept, aliases in keywords_for_statement.items():
            hits = []
            for alias in aliases:
                normalized_alias = self._normalize_text(alias)
                if normalized_alias and normalized_alias in combined_text:
                    hits.append(alias)

            if hits:
                score = min(0.99, 0.65 + 0.1 * len(hits))
                if score > best_score:
                    best_concept = concept
                    best_hits = hits
                    best_score = score

        if best_concept is None:
            return None

        return ConceptSuggestion(
            suggested_concept=best_concept,
            suggestion_confidence=best_score,
            suggestion_type="existing_concept",
            suggestion_reason=[f"keyword match: {hit}" for hit in best_hits],
        )

    def _fuzzy_match(
        self,
        keywords_for_statement: dict[str, list[str]],
        combined_text: str,
    ) -> ConceptSuggestion | None:
        best_concept = None
        best_alias = None
        best_ratio = 0.0

        for concept, aliases in keywords_for_statement.items():
            candidates = [concept] + aliases
            for candidate in candidates:
                normalized_candidate = self._normalize_text(candidate)
                if not normalized_candidate:
                    continue

                ratio = SequenceMatcher(None, combined_text, normalized_candidate).ratio()
                if ratio > best_ratio:
                    best_ratio = ratio
                    best_concept = concept
                    best_alias = candidate

        if best_concept is None or best_ratio < 0.55:
            return None

        suggestion_type = "existing_concept" if best_ratio >= 0.7 else "unknown"

        return ConceptSuggestion(
            suggested_concept=best_concept if suggestion_type == "existing_concept" else None,
            suggestion_confidence=round(best_ratio, 3),
            suggestion_type=suggestion_type if suggestion_type == "existing_concept" else "new_concept_candidate",
            suggestion_reason=[f"fuzzy match to '{best_alias}' with ratio={best_ratio:.3f}"],
        )

    @staticmethod
    def _normalize_text(value: str) -> str:
        value = value.replace("_", " ")
        value = re.sub(r"([a-z])([A-Z])", r"\1 \2", value)
        value = value.lower()
        value = re.sub(r"[^a-z0-9]+", " ", value)
        value = re.sub(r"\s+", " ", value).strip()
        return value