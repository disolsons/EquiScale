from financials_tracker.analytics.financial_dataset import FinancialDataset
from financials_tracker.analytics.metrics_engine import MetricsEngine
from financials_tracker.mappers.concept_map_helper import ConceptMapHelper
from financials_tracker.clients.edgar.edgar_client import EdgarClient
from financials_tracker.mappers.statement_mapper import StatementMapper   

def main():
    helper = ConceptMapHelper("financials_tracker/mappers/config/concept_map.yaml")
    client = EdgarClient(concept_helper=helper, ticker="TSLA")
    mapper = StatementMapper(helper)

    income_raw = client.fetch_income_statement(period_mode="history", years=3, annual=True)
    balance_raw = client.fetch_balance_sheet(period_mode="history", years=3, annual=True)
    cash_raw = client.fetch_cash_flow(period_mode="history", years=3, annual=True)

    income_df = mapper.map_historical_statement(statement_type = "income_statement", df=income_raw)
    balance_df = mapper.map_historical_statement(statement_type = "balance_sheet", df=balance_raw)
    cash_df = mapper.map_historical_statement(statement_type = "cash_flow", df=cash_raw)

    dataset = FinancialDataset(
        income_statement=income_df,
        balance_sheet=balance_df,
        cash_flow=cash_df,
    )
   
   
    engine = MetricsEngine(dataset)

    profitability = engine.calculate_profitability_metrics()
    growth = engine.calculate_growth_metrics()
    cashflow_metrics = engine.calculate_cash_flow_metrics()
    balance_metrics = engine.calculate_balance_sheet_metrics()

    print(f"Profitability Metrics:\n", profitability)
    print(f"Growth Metrics:\n", growth)
    print(f"Cash Flow Metrics:\n", cashflow_metrics)
    print(f"Balance Sheet Metrics:\n", balance_metrics)
    
if __name__ == "__main__":
    main()