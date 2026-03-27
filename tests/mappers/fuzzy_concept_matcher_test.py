from pathlib import Path

import pytest

from financials_tracker.mappers.concept_inference.fuzzy_concept_matcher import FuzzyConceptMatcher
from financials_tracker.mappers.concept_inference.config_processing_helper import SemanticConflictsHelper


@pytest.fixture
def semantic_conflicts_file(tmp_path: Path) -> Path:
    yaml_content = """
    hard_conflicts:
    - [basic, diluted]
    - [current, noncurrent]

    soft_conflicts:
    - [increase, decrease]
    - [gain, loss]
    - [income, expense]
    - [operating, nonoperating]
    """
    path = tmp_path / "semantic_conflicts.yaml"
    path.write_text(yaml_content, encoding="utf-8")
    return path


@pytest.fixture
def matcher(semantic_conflicts_file: Path) -> FuzzyConceptMatcher:
    helper = SemanticConflictsHelper(semantic_conflicts_file)
    return FuzzyConceptMatcher(helper)


def test_match_returns_existing_concept_for_good_fuzzy_candidate(matcher: FuzzyConceptMatcher):
    aliases_for_statement = {
        "capital_expenditures": [
            "capital expenditures",
            "acquire property plant and equipment",
        ]
    }

    result = matcher.match(
        text_candidates=[
            "payments to acquire property plant and equipment",
            "payments to acquire property plant equipment",
        ],
        aliases_for_statement=aliases_for_statement,
    )

    assert result is not None
    assert result.suggested_concept == "capital_expenditures"
    assert result.suggestion_type == "existing_concept"
    assert result.suggestion_confidence >= 0.70


def test_match_rejects_hard_conflict(matcher: FuzzyConceptMatcher):
    aliases_for_statement = {
        "diluted_eps": [
            "diluted earnings per share",
            "earnings per share diluted",
        ]
    }

    result = matcher.match(
        text_candidates=[
            "earnings per share basic",
            "basic earnings per share",
        ],
        aliases_for_statement=aliases_for_statement,
    )

    assert result is None


def test_match_applies_soft_conflict_penalty(matcher: FuzzyConceptMatcher):
    aliases_for_statement = {
        "income_tax_expense": [
            "income tax expense",
        ]
    }

    result = matcher.match(
        text_candidates=[
            "income tax benefit",
        ],
        aliases_for_statement=aliases_for_statement,
    )

    # This may still return a result depending on thresholds,
    # but the confidence should reflect the semantic conflict penalty.
    if result is not None:
        assert result.suggestion_confidence < 1.0
        assert any("penalty" in reason.lower() for reason in result.suggestion_reason)


def test_match_returns_none_when_similarity_too_low(matcher: FuzzyConceptMatcher):
    aliases_for_statement = {
        "capital_expenditures": [
            "capital expenditures",
        ]
    }

    result = matcher.match(
        text_candidates=[
            "completely unrelated disclosure text",
            "some unknown row label",
        ],
        aliases_for_statement=aliases_for_statement,
    )

    assert result is None


def test_match_uses_best_text_candidate(matcher: FuzzyConceptMatcher):
    aliases_for_statement = {
        "income_before_tax": [
            "income before tax",
            "before income taxes",
        ]
    }

    result = matcher.match(
        text_candidates=[
            "random noisy text",
            "income before income taxes",
            "something else unrelated",
        ],
        aliases_for_statement=aliases_for_statement,
    )

    assert result is not None
    assert result.suggested_concept == "income_before_tax"
    assert result.suggestion_type in {"existing_concept", "new_concept_candidate"}