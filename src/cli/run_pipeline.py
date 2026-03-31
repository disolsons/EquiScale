import argparse
import json
from pathlib import Path
from typing import Any
import pandas as pd
from src.clients.edgar.edgar_client import EdgarClient
from src.processing.utils.concept_map_helper import ConceptMapHelper
from src.processing.mappers.statement_mapper import StatementMapper
from src.domain.financial_dataset import FinancialDataset
from src.services.metrics.metrics_service import MetricsService
from src.validators.utils.ignored_tags_helper import IgnoredTagsHelper
from src.orchestration.edgar_pipeline_orchestrator import EdgarPipelineOrchestrator
from src.processing.enrichers.statement_fact_enricher import StatementFactEnricher
from src.services.data_services.report_data_service import ReportDataService
from src.orchestration.factories.edgar_pipeline_orchestrator_factory import EdgarPipelineOrchestratorFactory
import src.utils.config_constants as config



def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the financials tracker pipeline for one or more tickers."
    )
    parser.add_argument(
        "--tickers",
        nargs="+",
        required=True,
        help="One or more tickers, e.g. TSLA AAPL",
    )
    parser.add_argument(
        "--years",
        type=int,
        default=3,
        help="Number of historical years to fetch",
    )
    parser.add_argument(
        "--annual",
        type=str,
        default="true",
        help="Whether to fetch annual statements (true/false)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="outputs",
        help="Base output directory",
    )
    return parser.parse_args()


def run_for_ticker(
    ticker: str,
    years: int,
    annual: bool,
    output_dir: Path,
) -> None:
    print(f"Running pipeline for {ticker}...")

    pipeline_factory = EdgarPipelineOrchestratorFactory(config.CONCEPT_MAP_PATH)
    orchestrator = pipeline_factory.build()

    orchestrator.build_historical_dataset(ticker="NVDA", years=3, annual=True)

    print(f"Finished {ticker}.")

def parse_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "y"}

def main() -> None:
    args = parse_args()
    annual = parse_bool(args.annual)
    output_dir = Path(args.output_dir)

    for ticker in args.tickers:
        try:
            run_for_ticker(
                ticker=ticker,
                years=args.years,
                annual=annual,
                output_dir=output_dir,
            )
        except Exception as e:
            print(f"Pipeline failed for {ticker}: {e}")


if __name__ == "__main__":
    main()


    # # --- Save raw statements ---
    # save_json(dataframe_to_json_payload(income_raw), ticker_dir / "raw" / "income_statement.json")
    # save_json(dataframe_to_json_payload(balance_raw), ticker_dir / "raw" / "balance_sheet.json")
    # save_json(dataframe_to_json_payload(cash_enriched), ticker_dir / "raw" / "cash_flow.json")

    # # --- Save mapped statements ---
    # save_dataframe(income_mapped, ticker_dir / "mapped" / "income_statement.csv")
    # save_dataframe(balance_mapped, ticker_dir / "mapped" / "balance_sheet.csv")
    # save_dataframe(cash_mapped, ticker_dir / "mapped" / "cash_flow.csv")


    # --- Validation reports ---
    # income_validation = validator.validate(
    #     raw_df=income_raw,
    #     mapped_df=income_mapped,
    #     statement_type="income_statement",
    # )
    # balance_validation = validator.validate(
    #     raw_df=balance_raw,
    #     mapped_df=balance_mapped,
    #     statement_type="balance_sheet",
    # )
    # cash_validation = validator.validate(
    #     raw_df=cash_enriched,
    #     mapped_df=cash_mapped,
    #     statement_type="cash_flow",
    # )

    # save_json(income_validation, ticker_dir / "validation" / "income_statement.json")
    # save_json(balance_validation, ticker_dir / "validation" / "balance_sheet.json")
    # save_json(cash_validation, ticker_dir / "validation" / "cash_flow.json")

    # save_dataframe(profitability_metrics, ticker_dir / "metrics" / "profitability.csv")
    # save_dataframe(growth_metrics, ticker_dir / "metrics" / "growth.csv")
    # save_dataframe(cash_flow_metrics, ticker_dir / "metrics" / "cash_flow.csv")
    # save_dataframe(balance_sheet_metrics, ticker_dir / "metrics" / "balance_sheet.csv")





# def save_json(data: dict[str, Any], path: Path) -> None:
#     path.parent.mkdir(parents=True, exist_ok=True)
#     with path.open("w", encoding="utf-8") as f:
#         json.dump(make_json_safe(data), f, indent=2, ensure_ascii=False)

def save_dataframe(df: pd.DataFrame | None, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if df is None or df.empty:
        # Save an empty marker file for consistency
        path.write_text("", encoding="utf-8")
        return
    df.to_csv(path)
