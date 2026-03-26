from pathlib import Path
import pandas as pd
import pytest
from financials_tracker.mappers.concept_map_helper import ConceptMapHelper
from financials_tracker.mappers.statement_mapper import StatementMapper


@pytest.fixture
def concept_map_file(tmp_path: Path) -> Path:
    yaml_content = """
income_statement:
  revenue:
    - RevenueFromContractWithCustomerExcludingAssessedTax
    - SalesRevenueNet
"""
    path = tmp_path / "concept_map.yaml"
    path.write_text(yaml_content, encoding="utf-8")
    return path


@pytest.fixture
def mapper(concept_map_file: Path) -> StatementMapper:
    helper = ConceptMapHelper(concept_map_file)
    return StatementMapper(helper)


def test_map_historical_statement_selects_best_candidate_and_persists_all_metadata(
    mapper: StatementMapper,
):
    # First mapped tag is weaker:
    # - not total
    # - fewer populated periods
    # - deeper row
    #
    # Second mapped tag is stronger:
    # - total
    # - fully populated
    # - shallower row
    df = pd.DataFrame(
        [
            {
                "label": "Revenue from Contract with Customer",
                "depth": 2,
                "is_abstract": False,
                "is_total": False,
                "section": None,
                "confidence": 0.50,
                "FY 2025": 100.0,
                "FY 2024": None,
                "FY 2023": None,
            },
            {
                "label": "Sales Revenue, Net",
                "depth": 1,
                "is_abstract": False,
                "is_total": True,
                "section": None,
                "confidence": 0.50,
                "FY 2025": 110.0,
                "FY 2024": 105.0,
                "FY 2023": 95.0,
            },
        ],
        index=[
            "RevenueFromContractWithCustomerExcludingAssessedTax",
            "SalesRevenueNet",
        ],
    )

    result = mapper.map_historical_statement("income_statement", df)

    assert result is not None
    assert result.mapped_df is not None
    assert "revenue" in result.mapped_df.index

    # The mapped values should come from the stronger candidate.
    assert result.mapped_df.loc["revenue", "FY 2025"] == 110.0
    assert result.mapped_df.loc["revenue", "FY 2024"] == 105.0
    assert result.mapped_df.loc["revenue", "FY 2023"] == 95.0

    # We should now persist metadata for ALL candidates, not just the winner.
    revenue_metadata = [m for m in result.selection_metadata if m.concept == "revenue"]
    assert len(revenue_metadata) == 2

    # Check rank ordering
    revenue_metadata = sorted(revenue_metadata, key=lambda m: m.rank_order)

    winner = revenue_metadata[0]
    loser = revenue_metadata[1]

    assert winner.rank_order == 1
    assert winner.is_selected is True
    assert winner.raw_tag == "SalesRevenueNet"
    assert winner.label == "Sales Revenue, Net"
    assert winner.candidate_count == 2

    assert loser.rank_order == 2
    assert loser.is_selected is False
    assert loser.raw_tag == "RevenueFromContractWithCustomerExcludingAssessedTax"
    assert loser.label == "Revenue from Contract with Customer"
    assert loser.candidate_count == 2

    # Winner should score higher than loser
    assert winner.candidate_score > loser.candidate_score