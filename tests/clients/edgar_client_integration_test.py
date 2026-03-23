import pandas as pd
import pytest
from financials_tracker.clients.edgar.edgar_client import EdgarClient
from financials_tracker.mappers.concept_map_helper import ConceptMapHelper


@pytest.mark.integration
def test_fetch_income_statement_history_real():
    helper = ConceptMapHelper("financials_tracker/mappers/config/concept_map.yaml")
    client = EdgarClient(concept_helper=helper, ticker="AAPL")

    df = client.fetch_income_statement(
        period_mode="history",
        years=3,
        annual=True
    )

    assert df is not None, "Expected DataFrame, got None"
    assert not df.empty, "DataFrame is empty"

    assert "FY 2025" in df.columns or "FY 2024" in df.columns, "Missing expected FY columns"

    assert "NetIncomeLoss" in df.index, "Net income not found"
    assert "RevenueFromContractWithCustomerExcludingAssessedTax" in df.index, "Revenue not found"

    fy_cols = [c for c in df.columns if str(c).startswith("FY ")]
    assert fy_cols, "No FY columns found"

    net_income = pd.to_numeric(
        df.loc["NetIncomeLoss", fy_cols],
        errors="coerce"
    ).dropna()

    assert not net_income.empty, "No numeric net income values found"
    assert (net_income > 0).any(), "Net income should be positive for at least one year"