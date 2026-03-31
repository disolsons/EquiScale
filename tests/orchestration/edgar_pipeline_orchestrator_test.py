from unittest.mock import Mock
import pandas as pd
import pytest as pytest
from src.domain.financial_dataset import FinancialDataset
from src.domain.financial_report import FinancialReport
from src.orchestration.edgar_pipeline_orchestrator import EdgarPipelineOrchestrator
from src.processing.mappers.model.statement_mapping_result import StatementMappingResult


def make_df(index):
    df = pd.DataFrame(
        {
            "FY 2024": [1.0 for _ in index],
            "FY 2025": [2.0 for _ in index],
            "FY 2026": [3.0 for _ in index],
        },
        index=index,
    )
    df.index.name = "concept"
    return df


@pytest.fixture
def income_df():
    return make_df(["revenue", "net_income"])


@pytest.fixture
def balance_df():
    return make_df(["total_assets", "shareholder_equity"])


@pytest.fixture
def cash_df():
    return make_df(["operating_cash_flow", "capital_expenditures"])


@pytest.fixture
def metrics_payload():
    return {
        "profitability": make_df(["gross_margin"]),
        "growth": make_df(["revenue_growth_yoy"]),
        "cash_flow": make_df(["free_cash_flow"]),
        "balance_sheet": make_df(["roa_ending"]),
    }


@pytest.fixture
def orchestrator(income_df, balance_df, cash_df, metrics_payload):
    client = Mock()
    client.fetch_income_statement.return_value = income_df
    client.fetch_balance_sheet.return_value = balance_df
    client.fetch_cash_flow.return_value = cash_df

    enricher = Mock()
    enricher.enrich_missing_direct_tags.return_value = cash_df

    mapper = Mock()
    mapper.map_historical_statement.side_effect = [
        StatementMappingResult(mapped_df=income_df, selection_metadata=[{"selected": "income"}]),
        StatementMappingResult(mapped_df=balance_df, selection_metadata=[{"selected": "balance"}]),
        StatementMappingResult(mapped_df=cash_df, selection_metadata=[{"selected": "cash"}]),
    ]

    report_data_service = Mock()

    metrics_service = Mock()
    metrics_service.calculate_all_metrics.return_value = metrics_payload

    return EdgarPipelineOrchestrator(
        client=client,
        mapper=mapper,
        enricher=enricher,
        report_data_service=report_data_service,
        metrics_service=metrics_service,
    )

def test_build_historical_dataset_happy_path(orchestrator):
    dataset = orchestrator.build_historical_dataset(
        ticker="NVDA",
        years=3,
        annual=True,
    )

    assert isinstance(dataset, FinancialDataset)
    assert dataset.ticker == "NVDA"

    # All reports are generated
    assert isinstance(dataset.income_statement, FinancialReport)
    assert isinstance(dataset.balance_sheet, FinancialReport)
    assert isinstance(dataset.cash_flow, FinancialReport)
    
    # All reports are complete
    assert dataset.income_statement.raw is not None
    assert dataset.balance_sheet.raw is not None
    assert dataset.cash_flow.raw is not None

    assert dataset.cash_flow.enriched is not None

    assert dataset.income_statement.mapped is not None
    assert dataset.balance_sheet.mapped is not None
    assert dataset.cash_flow.mapped is not None

    assert dataset.income_statement.selection_metadata is not None
    assert dataset.balance_sheet.selection_metadata is not None
    assert dataset.cash_flow.selection_metadata is not None

    assert dataset.metrics is not None

    # Correct calls to dependencies
    orchestrator.client.fetch_income_statement.assert_called_once()
    orchestrator.client.fetch_balance_sheet.assert_called_once()
    orchestrator.client.fetch_cash_flow.assert_called_once()

    orchestrator.enricher.enrich_missing_direct_tags.assert_called_once()
    assert orchestrator.mapper.map_historical_statement.call_count == 3
    orchestrator.report_data_service.persist_reports_in_dataset.assert_called_once_with(dataset=dataset)
    orchestrator.metrics_service.calculate_all_metrics.assert_called_once_with(dataset=dataset)


def test_build_historical_dataset_rejects_empty_ticker(orchestrator):
    with pytest.raises(ValueError, match="ticker must be a non-empty string"):
        orchestrator.build_historical_dataset(
            ticker="",
            years=3,
            annual=True,
        )

def test_build_historical_dataset_rejects_invalid_years(orchestrator):
    with pytest.raises(ValueError, match="years must be greater than 0"):
        orchestrator.build_historical_dataset(
            ticker="NVDA",
            years=0,
            annual=True,
        )

def test_build_historical_dataset_raises_when_income_statement_missing(
    orchestrator,
):
    orchestrator.client.fetch_income_statement.return_value = None

    with pytest.raises(RuntimeError, match="No income statement returned"):
        orchestrator.build_historical_dataset(
            ticker="NVDA",
            years=3,
            annual=True,
        )

def test_build_historical_dataset_raises_when_balance_sheet_missing(
    orchestrator,
):
    orchestrator.client.fetch_balance_sheet.return_value = None

    with pytest.raises(RuntimeError, match="No balance sheet returned"):
        orchestrator.build_historical_dataset(
            ticker="NVDA",
            years=3,
            annual=True,
        )

def test_build_historical_dataset_raises_when_cash_flow_missing(
    orchestrator,
):
    orchestrator.client.fetch_cash_flow.return_value = None

    with pytest.raises(RuntimeError, match="No cash flow statement returned"):
        orchestrator.build_historical_dataset(
            ticker="NVDA",
            years=3,
            annual=True,
        )

def test_build_historical_dataset_raises_when_cash_flow_enrichment_fails(
    orchestrator,
):
    orchestrator.enricher.enrich_missing_direct_tags.return_value = None

    with pytest.raises(RuntimeError, match="Could not enrich cash flow report"):
        orchestrator.build_historical_dataset(
            ticker="NVDA",
            years=3,
            annual=True,
        )

def test_build_historical_dataset_raises_when_mapping_result_is_none(
    orchestrator,
):
    orchestrator.mapper.map_historical_statement.side_effect = [
        None,
        StatementMappingResult(mapped_df=make_df(["total_assets"]), selection_metadata=[{"selected": "balance"}]),
        StatementMappingResult(mapped_df=make_df(["operating_cash_flow"]), selection_metadata=[{"selected": "cash"}]),
    ]

    with pytest.raises(RuntimeError, match="Failed to map"):
        orchestrator.build_historical_dataset(
            ticker="NVDA",
            years=3,
            annual=True,
        )

def test_build_historical_dataset_raises_when_mapped_df_is_missing(
    orchestrator,
):
    orchestrator.mapper.map_historical_statement.side_effect = [
        StatementMappingResult(mapped_df=None, selection_metadata=[{"selected": "income"}]),
        StatementMappingResult(mapped_df=make_df(["total_assets"]), selection_metadata=[{"selected": "balance"}]),
        StatementMappingResult(mapped_df=make_df(["operating_cash_flow"]), selection_metadata=[{"selected": "cash"}]),
    ]

    with pytest.raises(RuntimeError, match="Mapper returned no mapped_df"):
        orchestrator.build_historical_dataset(
            ticker="NVDA",
            years=3,
            annual=True,
        )

def test_build_historical_dataset_raises_when_selection_metadata_missing(
    orchestrator,
):
    orchestrator.mapper.map_historical_statement.side_effect = [
        StatementMappingResult(mapped_df=make_df(["revenue"]), selection_metadata=[]),
        StatementMappingResult(mapped_df=make_df(["total_assets"]), selection_metadata=[{"selected": "balance"}]),
        StatementMappingResult(mapped_df=make_df(["operating_cash_flow"]), selection_metadata=[{"selected": "cash"}]),
    ]

    with pytest.raises(RuntimeError, match="Mapper returned no selection_metadata"):
        orchestrator.build_historical_dataset(
            ticker="NVDA",
            years=3,
            annual=True,
        )

def test_build_historical_dataset_raises_when_persistence_fails(
    orchestrator,
):
    orchestrator.report_data_service.persist_reports_in_dataset.side_effect = RuntimeError("db failure")

    with pytest.raises(RuntimeError, match="db failure"):
        orchestrator.build_historical_dataset(
            ticker="NVDA",
            years=3,
            annual=True,
        )

def test_build_historical_dataset_raises_when_metrics_are_none(
    orchestrator,
):
    orchestrator.metrics_service.calculate_all_metrics.return_value = None

    with pytest.raises(RuntimeError, match="Failed to calculate metrics"):
        orchestrator.build_historical_dataset(
            ticker="NVDA",
            years=3,
            annual=True,
        )

def test_build_historical_dataset_calls_cash_flow_enrichment_with_expected_arguments(
    orchestrator,
    cash_df,
):
    orchestrator.build_historical_dataset(
        ticker="NVDA",
        years=5,
        annual=False,
    )

    orchestrator.enricher.enrich_missing_direct_tags.assert_called_once()
    kwargs = orchestrator.enricher.enrich_missing_direct_tags.call_args.kwargs

    assert kwargs["statement_df"] is cash_df
    assert kwargs["statement_type"] == "cash_flow"
    assert kwargs["ticker"] == "NVDA"
    assert kwargs["years"] == 5
    assert kwargs["annual"] is False

def test_build_historical_dataset_persists_dataset_after_mapping(orchestrator):
    dataset = orchestrator.build_historical_dataset(
        ticker="NVDA",
        years=3,
        annual=True,
    )

    orchestrator.report_data_service.persist_reports_in_dataset.assert_called_once()
    persisted_dataset = orchestrator.report_data_service.persist_reports_in_dataset.call_args.kwargs["dataset"]

    assert persisted_dataset is dataset
    assert persisted_dataset.income_statement.mapped is not None
    assert persisted_dataset.balance_sheet.mapped is not None
    assert persisted_dataset.cash_flow.mapped is not None

def test_build_historical_dataset_attaches_metrics_to_dataset(orchestrator, metrics_payload):
    dataset = orchestrator.build_historical_dataset(
        ticker="NVDA",
        years=3,
        annual=True,
    )

    assert dataset.metrics == metrics_payload