import pandas as pd

from src.processing.utils.concept_map_helper import ConceptMapHelper
from src.validators.statement_validation_engine import StatementValidationEngine
from src.validators.utils.ignored_tags_helper import IgnoredTagsHelper


def test_coverage_validator_basic():
    helper = ConceptMapHelper("srcconfig/concept_map.yaml")
    ignored_tags_helper = IgnoredTagsHelper("src/config/ignored_tags.yaml")
    engine = StatementValidationEngine(helper, ignored_tags_helper)
    
    mapped_df = pd.DataFrame(
        {
            "FY 2024": [100.0, 40.0],
            "FY 2023": [90.0, 35.0],
        },
        index=["revenue", "gross_profit"],
    )

    result = engine.coverage_validator.validate(mapped_df, "income_statement")

    assert result["expected_concepts_count"] > 0
    assert result["mapped_concepts_count"] == 2
    assert "revenue" in result["mapped_concepts"]
    assert "gross_profit" in result["mapped_concepts"]


def test_reconciliation_income_statement():
    helper = ConceptMapHelper("financials_tracker/mappers/config/concept_map.yaml")
    ignored_tags_helper = IgnoredTagsHelper("financials_tracker/validators/config/ignored_tags.yaml")
    engine = StatementValidationEngine(helper, ignored_tags_helper)
    mapped_df = pd.DataFrame(
        {
            "FY 2024": [100.0, 40.0, 60.0],
        },
        index=["revenue", "cost_of_revenue", "gross_profit"],
    )

    result = engine.reconciliation_validator.validate(mapped_df, "income_statement", tolerance=1e-6)
    check = result["checks"]["gross_profit_check"]

    assert check["passed"] is True
    assert check["by_period"]["FY 2024"]["passed"] is True


def test_reconciliation_balance_sheet_equation():
    helper = ConceptMapHelper("financials_tracker/mappers/config/concept_map.yaml")
    ignored_tags_helper = IgnoredTagsHelper("financials_tracker/validators/config/ignored_tags.yaml")
    engine = StatementValidationEngine(helper, ignored_tags_helper)

    mapped_df = pd.DataFrame(
        {
            "FY 2024": [500.0, 500.0],
        },
        index=["total_assets", "total_liabilities_and_equity"],
    )

    result = engine.reconciliation_validator.validate(mapped_df, "balance_sheet", tolerance=1e-6)
    check = result["checks"]["balance_sheet_equation_check"]

    assert check["passed"] is True


def test_mapping_detail_historical():
    helper = ConceptMapHelper("financials_tracker/mappers/config/concept_map.yaml")
    ignored_tags_helper = IgnoredTagsHelper("financials_tracker/validators/config/ignored_tags.yaml")
    engine = StatementValidationEngine(helper, ignored_tags_helper)

    raw_df = pd.DataFrame(
        {
            "FY 2024": [100.0],
            "label": ["Total Assets"],
        },
        index=pd.Index(["Assets"], name="concept"),
    )
    raw_df.index.name = "concept"
    mapped_df = pd.DataFrame(
        {"FY 2024": [100.0]}, index=["total_assets"]
    )

    result = engine.mapping_detail_validator.validate(raw_df, mapped_df, "balance_sheet")

    assert "total_assets" in result["details"]
    assert result["details"]["total_assets"]["raw_tag"] == "Assets"


def test_validate_statement_combines_metrics():
    helper = ConceptMapHelper("financials_tracker/mappers/config/concept_map.yaml")
    ignored_tags_helper = IgnoredTagsHelper("financials_tracker/validators/config/ignored_tags.yaml")
    engine = StatementValidationEngine(helper, ignored_tags_helper)
    raw_df = pd.DataFrame(
        {"FY 2024": [100.0]},
        index=pd.Index(["GrossProfit"], name="concept"),
    )
    mapped_df = pd.DataFrame(
        {"FY 2024": [100.0]}, index=["gross_profit"]
    )

    result = engine.validate(raw_df, mapped_df, "income_statement")

    assert result["statement_type"] == "income_statement"
    assert "coverage" in result
    assert "mapping_detail" in result
    assert "reconciliation" in result
