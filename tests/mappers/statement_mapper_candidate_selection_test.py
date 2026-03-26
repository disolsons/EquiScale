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


def test_map_historical_statement_selects_best_candidate_not_first(mapper: StatementMapper):
    # The first mapped tag is present but weaker (fewer non-null periods, not total).
    # The second mapped tag should win because it is total and fully populated.
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

    income_result = mapper.map_historical_statement("income_statement", df)
    income_mapped = income_result.mapped_df

    assert income_mapped is not None
    assert "revenue" in income_mapped.index

    # The selected row should be the stronger candidate (SalesRevenueNet),
    # so the mapped values should come from that row.
    assert income_mapped.loc["revenue", "FY 2025"] == 110.0
    assert income_mapped.loc["revenue", "FY 2024"] == 105.0
    assert income_mapped.loc["revenue", "FY 2023"] == 95.0