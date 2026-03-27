import argparse
from pathlib import Path

import pandas as pd

from financials_tracker.metrics.financial_dataset import FinancialDataset
from financials_tracker.metrics.metrics_engine import MetricsEngine
from financials_tracker.metrics.metrics_registry_helper import MetricsRegistryHelper
from financials_tracker.storage.db_setup import get_session_factory
from financials_tracker.storage.models import MappedConceptValues


def parse_args():
    parser = argparse.ArgumentParser(description="Generate metrics for a ticker.")
    parser.add_argument("--ticker", required=True, help="Ticker symbol")
    parser.add_argument("--db-path", default=None, help="Optional SQLite DB path")
    parser.add_argument("--output-dir", default="outputs/metrics", help="Where to save CSVs")
    return parser.parse_args()


def load_statement_df(session, ticker: str, statement_type: str) -> pd.DataFrame | None:
    rows = (
        session.query(MappedConceptValues)
        .filter_by(ticker=ticker.upper(), statement_type=statement_type)
        .all()
    )

    if not rows:
        return None

    data = {}
    for row in rows:
        data.setdefault(row.concept, {})[row.period] = row.value

    df = pd.DataFrame.from_dict(data, orient="index")
    df.index.name = "concept"
    return df


def main():
    args = parse_args()

    session = get_session_factory(args.db_path)()

    try:
        income_df = load_statement_df(session, args.ticker, "income_statement")
        balance_df = load_statement_df(session, args.ticker, "balance_sheet")
        cash_df = load_statement_df(session, args.ticker, "cash_flow")

        dataset = FinancialDataset(
            income_statement=income_df,
            balance_sheet=balance_df,
            cash_flow=cash_df,
        )

        registry_path = (
            Path(__file__).resolve().parents[1]
            / "metrics"
            / "config"
            / "metric_registry.yaml"
        )
        registry_helper = MetricsRegistryHelper(registry_path)

        engine = MetricsEngine(
            dataset=dataset,
            metrics_registry_helper=registry_helper,
        )

        profitability = engine.calculate_profitability_metrics()
        growth = engine.calculate_growth_metrics()
        cash_flow = engine.calculate_cash_flow_metrics()
        balance = engine.calculate_balance_sheet_metrics()

        output_dir = Path(args.output_dir) / args.ticker.upper()
        output_dir.mkdir(parents=True, exist_ok=True)

        profitability.to_csv(output_dir / "profitability_metrics.csv")
        growth.to_csv(output_dir / "growth_metrics.csv")
        cash_flow.to_csv(output_dir / "cash_flow_metrics.csv")
        balance.to_csv(output_dir / "balance_sheet_metrics.csv")

        print(f"Saved metrics for {args.ticker.upper()} to {output_dir}")

    finally:
        session.close()


if __name__ == "__main__":
    main()