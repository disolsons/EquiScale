from pathlib import Path

import pytest

from financials_tracker.mappers.concept_map_helper import ConceptMapHelper
from financials_tracker.clients.edgar.edgar_client import EdgarClient
from financials_tracker.mappers.statement_mapper import StatementMapper


CONFIG_PATH = (
    Path(__file__).resolve().parents[2]
    / "financials_tracker"
    / "mappers"
    / "config"
    / "concept_map.yaml"
)


STATEMENT_CASES = [
    {
        "statement_type": "income_statement",
        "fetch_method": "fetch_income_statement",
        "expected_concepts": ["revenue", "net_income", "operating_income"],
    },
    {
        "statement_type": "balance_sheet",
        "fetch_method": "fetch_balance_sheet",
        "expected_concepts": ["cash_and_equivalents", "total_assets", "shareholder_equity"],
    },
    {
        "statement_type": "cash_flow",
        "fetch_method": "fetch_cash_flow",
        "expected_concepts": ["operating_cash_flow", "capital_expenditures"],
    },
]


@pytest.mark.integration
@pytest.mark.parametrize("ticker", ["AAPL", "TSLA"])
@pytest.mark.parametrize("case", STATEMENT_CASES, ids=[c["statement_type"] for c in STATEMENT_CASES])
def test_historical_statement_pipeline_real(ticker, case):
    helper = ConceptMapHelper(CONFIG_PATH)
    client = EdgarClient(concept_helper=helper, ticker=ticker)
    mapper = StatementMapper(helper)

    fetch_fn = getattr(client, case["fetch_method"])

    raw_df = fetch_fn(
        period_mode="history",
        years=3,
        annual=True,
    )

    assert raw_df is not None, f"Raw historical {case['statement_type']} is None for {ticker}"
    assert not raw_df.empty, f"Raw historical {case['statement_type']} is empty for {ticker}"

    mapped_df = mapper.map_historical_statement(
        df=raw_df,
        statement_type=case["statement_type"],
    )

    assert mapped_df is not None, f"Mapped historical {case['statement_type']} is None for {ticker}"
    assert not mapped_df.empty, f"Mapped historical {case['statement_type']} is empty for {ticker}"

    period_cols = [c for c in mapped_df.columns if str(c).startswith(("FY ", "Q"))]
    assert period_cols, f"No period columns found in mapped historical {case['statement_type']} for {ticker}"

    found_any = [concept for concept in case["expected_concepts"] if concept in mapped_df.index]
    assert found_any, (
        f"None of the expected concepts were found in mapped historical "
        f"{case['statement_type']} for {ticker}. Expected one of {case['expected_concepts']}"
    )


@pytest.mark.integration
@pytest.mark.parametrize("ticker", ["AAPL", "TSLA"])
@pytest.mark.parametrize("case", STATEMENT_CASES, ids=[c["statement_type"] for c in STATEMENT_CASES])
def test_latest_statement_pipeline_real(ticker, case):
    helper = ConceptMapHelper(CONFIG_PATH)
    client = EdgarClient(concept_helper=helper, ticker=ticker)
    mapper = StatementMapper(helper)

    fetch_fn = getattr(client, case["fetch_method"])

    raw_df = fetch_fn(
        period_mode="latest",
        annual=True,
    )

    assert raw_df is not None, f"Raw latest {case['statement_type']} is None for {ticker}"
    assert not raw_df.empty, f"Raw latest {case['statement_type']} is empty for {ticker}"

    mapped_df = mapper.map_latest_statement(
        df=raw_df,
        statement_type=case["statement_type"],
    )

    assert mapped_df is not None, f"Mapped latest {case['statement_type']} is None for {ticker}"
    assert not mapped_df.empty, f"Mapped latest {case['statement_type']} is empty for {ticker}"

    found_any = [concept for concept in case["expected_concepts"] if concept in mapped_df.index]
    assert found_any, (
        f"None of the expected concepts were found in mapped latest "
        f"{case['statement_type']} for {ticker}. Expected one of {case['expected_concepts']}"
    )

    assert "normalized_raw_tag" in mapped_df.columns, (
        f"'normalized_raw_tag' missing in mapped latest {case['statement_type']} for {ticker}"
    )