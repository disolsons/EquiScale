from pathlib import Path

import pytest

from financials_tracker.mappers.concept_inference.concept_aliases_helper import ConceptAliasesHelper
from financials_tracker.mappers.concept_inference.concept_inference_engine import ConceptInferenceEngine


@pytest.fixture
def engine(aliases_file="tests/mappers/concept_aliases_test.yaml") -> ConceptInferenceEngine:
    helper = ConceptAliasesHelper(aliases_file)
    return ConceptInferenceEngine(helper)


def test_suggest_concept_ignores_abstract_tag(engine: ConceptInferenceEngine):
    result = engine.suggest_concept(
        statement_type="cash_flow",
        raw_tag="NetCashProvidedByUsedInOperatingActivitiesAbstract",
        label="Cash flows from operating activities:"
    )

    assert result["suggestion_type"] == "ignore_candidate"
    assert result["suggested_concept"] is None
    assert result["suggestion_confidence"] > 0.9


def test_suggest_concept_keyword_match(engine: ConceptInferenceEngine):
    result = engine.suggest_concept(
        statement_type="cash_flow",
        raw_tag="PaymentsToAcquirePropertyPlantAndEquipment",
        label="Payments to acquire property, plant and equipment"
    )

    assert result["suggestion_type"] == "existing_concept"
    assert result["suggested_concept"] == "capital_expenditures"
    assert result["suggestion_confidence"] >= 0.7


def test_suggest_concept_respects_statement_type(engine: ConceptInferenceEngine):
    result = engine.suggest_concept(
        statement_type="cash_flow",
        raw_tag="IncomeBeforeIncomeTaxes",
        label="Income before income taxes"
    )

    assert result["suggested_concept"] != "income_before_tax"


def test_suggest_concept_fuzzy_fallback(engine: ConceptInferenceEngine):
    result = engine.suggest_concept(
        statement_type="income_statement",
        raw_tag="IncomeBeforeIncomeTaxes",
        label="Income before income taxes"
    )

    assert result["suggested_concept"] == "income_before_tax"
    assert result["suggestion_type"] in {"existing_concept", "new_concept_candidate"}


def test_suggest_concept_returns_new_concept_candidate_when_no_match(engine: ConceptInferenceEngine):
    result = engine.suggest_concept(
        statement_type="balance_sheet",
        raw_tag="CompletelyUnknownThing",
        label="Some completely unknown disclosure"
    )

    assert result["suggested_concept"] is None
    assert result["suggestion_type"] in {"new_concept_candidate", "unknown"}

def test_normalize_text_splits_camel_case(engine: ConceptInferenceEngine):
    normalized = engine._normalize_text("PaymentsToAcquirePropertyPlantAndEquipment")
    assert normalized == "payments to acquire property plant and equipment"