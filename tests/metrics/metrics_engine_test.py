import pandas as pd

from src.metrics.model.financial_dataset import FinancialDataset
from src.metrics.metrics_engine import MetricsEngine
from src.metrics.metrics_registry_helper import MetricsRegistryHelper

def build_test_dataset():
    income_statement = pd.DataFrame(
        {
            "FY 2023": [100.0, 40.0, 20.0, 10.0, 2.0],
            "FY 2024": [120.0, 48.0, 24.0, 12.0, 2.4],
            "FY 2025": [150.0, 60.0, 30.0, 15.0, 3.0],
        },
        index=[
            "revenue",
            "gross_profit",
            "operating_income",
            "net_income",
            "diluted_eps",
        ],
    )
    income_statement.index.name = "concept"

    balance_sheet = pd.DataFrame(
        {
            "FY 2023": [200.0, 100.0],
            "FY 2024": [240.0, 120.0],
            "FY 2025": [300.0, 150.0],
        },
        index=[
            "total_assets",
            "shareholder_equity",
        ],
    )
    balance_sheet.index.name = "concept"

    cash_flow = pd.DataFrame(
        {
            "FY 2023": [30.0, 10.0],
            "FY 2024": [36.0, 12.0],
            "FY 2025": [45.0, 15.0],
        },
        index=[
            "operating_cash_flow",
            "capital_expenditures",
        ],
    )
    cash_flow.index.name = "concept"

    return FinancialDataset(
        income_statement=income_statement,
        balance_sheet=balance_sheet,
        cash_flow=cash_flow,
    )


def test_calculate_profitability_metrics():
    dataset = build_test_dataset()
    metrics_registry_helper = MetricsRegistryHelper("financials_tracker/metrics/config/metric_registry.yaml")
    engine = MetricsEngine(dataset=dataset, metrics_registry_helper=metrics_registry_helper)

    result = engine.calculate_profitability_metrics()

    assert result is not None
    assert "gross_margin" in result.index
    assert "operating_margin" in result.index
    assert "net_margin" in result.index

    assert result.loc["gross_margin", "FY 2023"] == 0.40
    assert result.loc["gross_margin", "FY 2024"] == 0.40
    assert result.loc["gross_margin", "FY 2025"] == 0.40

    assert result.loc["operating_margin", "FY 2023"] == 0.20
    assert result.loc["net_margin", "FY 2025"] == 0.10


def test_calculate_cash_flow_metrics():
    dataset = build_test_dataset()
    metrics_registry_helper = MetricsRegistryHelper("financials_tracker/metrics/config/metric_registry.yaml")
    engine = MetricsEngine(dataset=dataset, metrics_registry_helper=metrics_registry_helper)

    result = engine.calculate_cash_flow_metrics()

    assert result is not None
    assert "free_cash_flow" in result.index
    assert "free_cash_flow_margin" in result.index

    # free cash flow = operating cash flow - capex
    assert result.loc["free_cash_flow", "FY 2023"] == 20.0
    assert result.loc["free_cash_flow", "FY 2024"] == 24.0
    assert result.loc["free_cash_flow", "FY 2025"] == 30.0

    # free cash flow margin = free cash flow / revenue
    assert result.loc["free_cash_flow_margin", "FY 2023"] == 0.20
    assert result.loc["free_cash_flow_margin", "FY 2024"] == 0.20
    assert result.loc["free_cash_flow_margin", "FY 2025"] == 0.20


def test_calculate_growth_metrics():
    dataset = build_test_dataset()
    metrics_registry_helper = MetricsRegistryHelper("financials_tracker/metrics/config/metric_registry.yaml")
    engine = MetricsEngine(dataset=dataset, metrics_registry_helper=metrics_registry_helper)

    result = engine.calculate_growth_metrics()

    assert result is not None
    assert "revenue_growth_yoy" in result.index
    assert "net_income_growth_yoy" in result.index
    assert "diluted_eps_growth_yoy" in result.index

    assert pd.isna(result.loc["revenue_growth_yoy", "FY 2023"])
    assert result.loc["revenue_growth_yoy", "FY 2024"] == 0.20
    assert result.loc["revenue_growth_yoy", "FY 2025"] == 0.25

    assert result.loc["net_income_growth_yoy", "FY 2024"] == 0.20
    assert result.loc["diluted_eps_growth_yoy", "FY 2025"] == 0.25


def test_calculate_balance_sheet_metrics():
    dataset = build_test_dataset()
    metrics_registry_helper = MetricsRegistryHelper("financials_tracker/metrics/config/metric_registry.yaml")
    engine = MetricsEngine(dataset=dataset, metrics_registry_helper=metrics_registry_helper)

    result = engine.calculate_balance_sheet_metrics()

    assert result is not None
    assert "roa_ending" in result.index
    assert "roe_ending" in result.index
    assert "roa_avg_assets" in result.index
    assert "roe_avg_equity" in result.index

    # ending-based
    assert result.loc["roa_ending", "FY 2023"] == 10.0 / 200.0
    assert result.loc["roe_ending", "FY 2025"] == 15.0 / 150.0

    # average-based
    # FY 2024 avg assets = (240 + 200) / 2 = 220
    # FY 2024 avg equity = (120 + 100) / 2 = 110
    assert pd.isna(result.loc["roa_avg_assets", "FY 2023"])
    assert result.loc["roa_avg_assets", "FY 2024"] == 12.0 / 220.0
    assert result.loc["roe_avg_equity", "FY 2024"] == 12.0 / 110.0


def test_calculate_metric_dependency_free_cash_flow_margin():
    dataset = build_test_dataset()
    metrics_registry_helper = MetricsRegistryHelper("financials_tracker/metrics/config/metric_registry.yaml")
    engine = MetricsEngine(dataset=dataset, metrics_registry_helper=metrics_registry_helper)

    result = engine._calculate_metric("free_cash_flow_margin")

    assert result is not None
    assert result["FY 2023"] == 0.20
    assert result["FY 2024"] == 0.20
    assert result["FY 2025"] == 0.20