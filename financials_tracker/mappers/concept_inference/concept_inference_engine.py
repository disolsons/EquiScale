import re
from dataclasses import dataclass
from difflib import SequenceMatcher
from financials_tracker.mappers.concept_inference.concept_aliases_helper import ConceptAliasesHelper

@dataclass
class ConceptSuggestion:
    suggested_concept: str | None
    suggestion_confidence: float
    suggestion_type: str
    suggestion_reason: list[str]


class ConceptInferenceEngine:
    """
        Infers the most likely standardized financial concept for an unmapped raw tag.

        This engine provides rule-based suggestions by analyzing:
        - raw XBRL tag names
        - human-readable labels
        - statement type context (income statement, balance sheet, cash flow)

        The inference process follows a deterministic pipeline:
        1. Ignore detection:
        Identifies structural or presentation rows (e.g., abstract tags, headers)
        that should not be mapped to any concept.

        2. Keyword-based matching:
        Matches normalized tag/label text against predefined concept keyword aliases.
        This is the primary and most reliable inference mechanism.

        3. Fuzzy similarity matching:
        Falls back to approximate string similarity when no keyword match is found.

        The engine outputs structured suggestions including:
        - suggested concept
        - confidence score
        - suggestion type
        - explanation of reasoning

        This component is designed to be:
        - deterministic (no randomness)
        - explainable (clear reasoning for each suggestion)
        - extensible (can be augmented with ML/LLM in the future)
    """
    def __init__(self, concept_aliases_helper: ConceptAliasesHelper):
        self.concept_aliases_helper = concept_aliases_helper

    def suggest_concept(
        self,
        statement_type: str,
        raw_tag: str | None,
        label: str | None,
    ) -> dict:
        """
            Suggest a standardized concept for a given unmapped raw tag.

            Parameters
            ----------
            statement_type : str
                The financial statement type (e.g., "income_statement", "balance_sheet", "cash_flow").
                Used to restrict candidate concepts to the relevant domain.

            raw_tag : str | None
                The original XBRL tag name.

            label : str | None
                The human-readable label associated with the tag.

            Returns
            -------
            dict
                A structured suggestion with the following fields:
                - suggested_concept: str | None
                - suggestion_confidence: float
                - suggestion_type: str
                - suggestion_reason: list[str]

            Notes
            -----
            The inference process is sequential:
            1. Ignore detection: returns early if the tag is likely non-mappable.
            2. Keyword matching: attempts exact/substring matches against concept aliases.
            3. Fuzzy matching: attempts approximate similarity if no keyword match is found.

            Suggestion types:
            - "existing_concept": strong match to an existing standardized concept
            - "new_concept_candidate": meaningful tag but no clear existing mapping
            - "ignore_candidate": structural or non-analytical row
            - "unknown": insufficient signal for inference
        """
        raw_tag = raw_tag or ""
        label = label or ""

        normalized_tag = self._normalize_text(raw_tag)
        normalized_label = self._normalize_text(label)
        combined_text = f"{normalized_tag} {normalized_label}".strip()

        ignore_result = self._check_ignore_candidate(raw_tag, label, combined_text)
        if ignore_result is not None:
            return ignore_result.__dict__

        statement_aliases = self.concept_aliases_helper.get_statement_aliases(statement_type)
        if not statement_aliases:
            return ConceptSuggestion(
                suggested_concept=None,
                suggestion_confidence=0.0,
                suggestion_type="unknown",
                suggestion_reason=[f"no keyword dictionary for statement_type={statement_type}"],
            ).__dict__

        keyword_match = self._keyword_match(statement_aliases, combined_text)
        if keyword_match is not None:
            return keyword_match.__dict__

        fuzzy_match = self._fuzzy_match(statement_aliases, combined_text)
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
        """
            Identify tags that are likely structural or non-analytical and should be ignored.

            Examples include:
            - abstract tags (e.g., ending with 'Abstract')
            - section headers
            - presentation-only rows

            Returns
            -------
            ConceptSuggestion | None
                Returns an "ignore_candidate" suggestion if applicable, otherwise None.
        """
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
        """
            Attempt to match the tag/label text to known concept aliases using keyword matching.

            This method performs substring matching against normalized text and assigns
            a heuristic confidence score based on the number of matching keywords.

            Confidence scoring:
            - Starts from a baseline (0.65) when at least one keyword matches
            - Increases with the number of matches
            - Capped below 1.0 to reflect uncertainty in heuristic matching

            Returns
            -------
            ConceptSuggestion | None
                A suggestion if a match is found, otherwise None.
        """
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
                #Confidence starts from a baseline (0.65) and increases with the number of keyword matches, 
                #capped below 1.0 to reflect heuristic uncertainty.
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
        
        """
            Attempt to infer a concept using approximate string similarity.

            This method compares the normalized input text with:
            - concept names
            - concept keyword aliases

            using a sequence similarity metric.

            Used as a fallback when keyword matching fails.

            Returns
            -------
            ConceptSuggestion | None
                A suggestion if similarity exceeds threshold, otherwise None.
        """
        best_concept = None
        best_alias = None
        best_ratio = 0.0

        for concept, aliases in keywords_for_statement.items():
            candidates = [concept] + aliases
            for candidate in candidates:
                normalized_candidate = self._normalize_text(candidate)
                if not normalized_candidate:
                    continue
                # computes the delta between the input text and candidate, giving a score between 0 and 1. 
                # TODO: How can we avoid high score for oposite meanings (e.g, "operating loss before tax" vs "operating income before tax")?
                #  Maybe we can add a penalty for certain negative keywords like "loss", "expense", "decrease", etc.?
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

        # Replace underscores with spaces so snake_case words can be matched naturally.
        value = value.replace("_", " ")
        # Insert a space between lowercase-to-uppercase transitions to split CamelCase words. Since tags are commonly written like this "DepreciationDepletionAndAmortization"
        value = re.sub(r"([a-z])([A-Z])", r"\1 \2", value)
        # Lowercase everything to make matching case-insensitive.
        value = value.lower()
        # Replace any non-alphanumeric characters with spaces to simplify comparison.
        value = re.sub(r"[^a-z0-9]+", " ", value)
        # Collapse repeated whitespace into a single space and trim leading/trailing spaces.
        value = re.sub(r"\s+", " ", value).strip()
        return value