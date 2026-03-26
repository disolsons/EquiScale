from pathlib import Path

import pytest

from financials_tracker.mappers.concept_inference.config_processing_helper import ConceptAliasesHelper, IgnorePatternsHelper, SemanticConflictsHelper
from financials_tracker.mappers.concept_inference.concept_inference_engine import ConceptInferenceEngine
from financials_tracker.mappers.concept_inference.fuzzy_concept_matcher import FuzzyConceptMatcher
from financials_tracker.mappers.tag_normalization_utils import normalize_text

@pytest.fixture
def aliases_file(tmp_path: Path) -> Path:
    yaml_content = """
    income_statement:
        revenue:
            - revenue
            - net sales

        income_before_tax:
            - income before tax
            - income before income taxes
            - before income taxes
            - before tax
            - pretax
            - pre tax

        diluted_eps:
            - diluted earnings per share
            - earnings per share diluted
            - diluted eps

    cash_flow:
        capital_expenditures:
            - capital expenditures
            - capex
            - acquire property plant and equipment
            - payments to acquire property plant and equipment

        operating_cash_flow:
            - operating cash flow
            - net cash from operating activities
            - net cash provided by operating activities

    balance_sheet:
        total_assets:
            - assets
            - total assets
    """
    path = tmp_path / "concept_aliases.yaml"
    path.write_text(yaml_content, encoding="utf-8")
    return path


@pytest.fixture
def ignore_patterns_file(tmp_path: Path) -> Path:
    yaml_content = """
        header_phrases:
        - cash flows from operating activities
        - cash flows from investing activities
        - cash flows from financing activities
        - supplemental cash flow information
        - adjustments to reconcile
        - changes in operating assets and liabilities

        ignore_suffixes:
        - Abstract

        ignore_contains:
        - abstract
    """
    path = tmp_path / "ignore_patterns.yaml"
    path.write_text(yaml_content, encoding="utf-8")
    return path


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
def engine(
    aliases_file: Path,
    ignore_patterns_file: Path,
    semantic_conflicts_file: Path,
) -> ConceptInferenceEngine:
    aliases_helper = ConceptAliasesHelper(aliases_file)
    ignore_helper = IgnorePatternsHelper(ignore_patterns_file)
    conflicts_helper = SemanticConflictsHelper(semantic_conflicts_file)
    fuzzy_matcher = FuzzyConceptMatcher(conflicts_helper)

    return ConceptInferenceEngine(
        concept_aliases_helper=aliases_helper,
        ignore_patterns_helper=ignore_helper,
        semantic_conflicts_helper=conflicts_helper,
        fuzzy_matcher=fuzzy_matcher
    )


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


def test_suggest_concept_uses_fuzzy_fallback(engine: ConceptInferenceEngine):
    result = engine.suggest_concept(
        statement_type="income_statement",
        raw_tag="IncomeBeforIncomeTaxes",
        label=""
    )
    print(result)
    assert result["suggested_concept"] == "income_before_tax"
    assert result["suggestion_type"] in {"existing_concept", "new_concept_candidate"}

def test_normalize_text_splits_camel_case(engine: ConceptInferenceEngine):
    normalized = normalize_text("PaymentsToAcquirePropertyPlantAndEquipment")
    assert normalized == "payments to acquire property plant and equipment"