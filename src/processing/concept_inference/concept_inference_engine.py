from src.processing.concept_inference.fuzzy_concept_matcher import FuzzyConceptMatcher
from src.processing.concept_inference.config_processing_helper import IgnorePatternsHelper, SemanticConflictsHelper, ConceptAliasesHelper
from src.processing.concept_inference.model.concept_suggestion import ConceptSuggestion
from src.processing.utils.tag_normalization_utils import normalize_text

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
    def __init__(self, concept_aliases_helper: ConceptAliasesHelper, ignore_patterns_helper: IgnorePatternsHelper, 
                 semantic_conflicts_helper: SemanticConflictsHelper, fuzzy_matcher: FuzzyConceptMatcher):
        
        self.concept_aliases_helper = concept_aliases_helper
        self.ignore_patterns_helper = ignore_patterns_helper
        self.semantic_conflicts_helper = semantic_conflicts_helper
        self.fuzzy_matcher = fuzzy_matcher

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

        normalized_tag = normalize_text(raw_tag)
        normalized_label = normalize_text(label)
        

        text_candidates = []
        if normalized_tag:
            text_candidates.append(normalized_tag)

        if normalized_label and normalized_label not in text_candidates:
            text_candidates.append(normalized_label)

        combined_text = f"{normalized_tag} {normalized_label}".strip()
        if combined_text and combined_text not in text_candidates:
            text_candidates.append(combined_text)

        ignore_result = self._check_ignore_candidate(raw_tag, text_candidates)
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

        fuzzy_match = self.fuzzy_matcher.match(text_candidates, statement_aliases)        
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
        text_candidates: list[str],
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
        ignore_suffixes = self.ignore_patterns_helper.get_ignore_suffixes()
        ignore_contains = self.ignore_patterns_helper.get_ignore_contains()
        header_phrases = self.ignore_patterns_helper.get_header_phrases()
        combined_text = " ".join(text_candidates)

         # Ignore tags with known structural suffixes, e.g. "...Abstract"
        if any(raw_tag.endswith(suffix) for suffix in ignore_suffixes):
            reasons.append("raw tag ends with ignored structural suffix")

        # Ignore rows whose normalized text contains known ignore markers
        matched_contains = [phrase for phrase in ignore_contains if phrase in combined_text]
        for phrase in matched_contains:
            reasons.append(f"text contains ignored marker: '{phrase}'")

        # Ignore rows that look like section headers or presentation wrappers
        matched_headers = [phrase for phrase in header_phrases if phrase in combined_text]
        for phrase in matched_headers:
            reasons.append(f"looks like section header: '{phrase}'")
       

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
        aliases_for_statement: dict[str, list[str]],
        text_candidates: list[str]
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
        best_hits: list[str] = []
        best_score = 0.0

        for concept, aliases in aliases_for_statement.items():
            hits: list[str] = []

            for alias in aliases:
                normalized_alias = normalize_text(alias)
                if not normalized_alias:
                    continue

                for candidate_text in text_candidates:
                    if normalized_alias in candidate_text:
                        hits.append(alias)
                        break

            if hits:
                # Confidence starts at a baseline and increases with additional alias hits.
                score = min(0.99, 0.65 + 0.10 * len(hits))

                if score > best_score:
                    best_concept = concept
                    best_hits = hits
                    best_score = score

        if best_concept is None:
            return None

        return ConceptSuggestion(
            suggested_concept=best_concept,
            suggestion_confidence=round(best_score, 3),
            suggestion_type="existing_concept",
            suggestion_reason=[f"keyword match: {hit}" for hit in best_hits],
        )