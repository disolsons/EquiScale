import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd

from financials_tracker.clients.edgar.edgar_client import EdgarClient
from financials_tracker.mappers.concept_map_helper import ConceptMapHelper
from financials_tracker.mappers.statement_mapper import StatementMapper
from financials_tracker.metrics.model.financial_dataset import FinancialDataset
from financials_tracker.metrics.metrics_engine import MetricsEngine
from financials_tracker.validators.utils.ignored_tags_helper import IgnoredTagsHelper
from financials_tracker.validators.statement_validation_engine import StatementValidationEngine
from financials_tracker.storage.db_setup import get_session_factory
from financials_tracker.storage.repositories import (
    replace_unmapped_tags_for_statement,
    upsert_mapping_validations,
    replace_mapped_concept_selections_for_statement,
    replace_mapped_concept_values_for_statement
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]

CONCEPT_MAP_PATH = (
    PROJECT_ROOT
    / "financials_tracker"
    / "mappers"
    / "config"
    / "concept_map.yaml"
)

IGNORED_TAGS_PATH = (
    PROJECT_ROOT
    / "financials_tracker"
    / "validators"
    / "config"
    / "ignored_tags.yaml"
)


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


def parse_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "y"}


def make_json_safe(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {str(k): make_json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [make_json_safe(v) for v in obj]
    if isinstance(obj, tuple):
        return [make_json_safe(v) for v in obj]
    if pd.isna(obj):
        return None
    if hasattr(obj, "item"):
        try:
            return obj.item()
        except Exception:
            pass
    return obj


def save_json(data: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(make_json_safe(data), f, indent=2, ensure_ascii=False)


def save_dataframe(df: pd.DataFrame | None, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if df is None or df.empty:
        # Save an empty marker file for consistency
        path.write_text("", encoding="utf-8")
        return
    df.to_csv(path)


def dataframe_to_json_payload(df: pd.DataFrame | None) -> dict[str, Any]:
    if df is None or df.empty:
        return {"rows": [], "columns": []}
    return {
        "columns": [str(c) for c in df.columns],
        "index": [str(i) for i in df.index],
        "rows": df.reset_index().to_dict(orient="records"),
    }


def run_for_ticker(
    ticker: str,
    years: int,
    annual: bool,
    output_dir: Path,
) -> None:
    print(f"Running pipeline for {ticker}...")

    concept_map_helper = ConceptMapHelper(CONCEPT_MAP_PATH)
    ignored_tags_helper = IgnoredTagsHelper(IGNORED_TAGS_PATH)

    client = EdgarClient(concept_helper=concept_map_helper, ticker=ticker)
    mapper = StatementMapper(concept_map_helper)
    validator = StatementValidationEngine(concept_map_helper, ignored_tags_helper)

    ticker_dir = output_dir / ticker.upper()

    # --- Fetch raw historical statements ---
    income_raw = client.fetch_income_statement(period_mode="history", years=years, annual=annual)
    balance_raw = client.fetch_balance_sheet(period_mode="history", years=years, annual=annual)
    cash_raw = client.fetch_cash_flow(period_mode="history", years=years, annual=annual)

    # --- Map historical statements ---
    income_result = mapper.map_historical_statement(df=income_raw, statement_type="income_statement")
    balance_result = mapper.map_historical_statement(df=balance_raw, statement_type="balance_sheet")
    cash_result = mapper.map_historical_statement(df=cash_raw, statement_type="cash_flow")

    income_mapped = income_result.mapped_df
    balance_mapped = balance_result.mapped_df
    cash_mapped = cash_result.mapped_df


    # --- Save raw statements ---
    save_json(dataframe_to_json_payload(income_raw), ticker_dir / "raw" / "income_statement.json")
    save_json(dataframe_to_json_payload(balance_raw), ticker_dir / "raw" / "balance_sheet.json")
    save_json(dataframe_to_json_payload(cash_raw), ticker_dir / "raw" / "cash_flow.json")

    # --- Save mapped statements ---
    save_dataframe(income_mapped, ticker_dir / "mapped" / "income_statement.csv")
    save_dataframe(balance_mapped, ticker_dir / "mapped" / "balance_sheet.csv")
    save_dataframe(cash_mapped, ticker_dir / "mapped" / "cash_flow.csv")


    # --- Validation reports ---
    income_validation = validator.validate(
        raw_df=income_raw,
        mapped_df=income_mapped,
        statement_type="income_statement",
    )
    balance_validation = validator.validate(
        raw_df=balance_raw,
        mapped_df=balance_mapped,
        statement_type="balance_sheet",
    )
    cash_validation = validator.validate(
        raw_df=cash_raw,
        mapped_df=cash_mapped,
        statement_type="cash_flow",
    )

    save_json(income_validation, ticker_dir / "validation" / "income_statement.json")
    save_json(balance_validation, ticker_dir / "validation" / "balance_sheet.json")
    save_json(cash_validation, ticker_dir / "validation" / "cash_flow.json")

    # --- Metrics ---
    dataset = FinancialDataset(
        income_statement=income_mapped,
        balance_sheet=balance_mapped,
        cash_flow=cash_mapped,
    )
    metrics_engine = MetricsEngine(dataset)

    profitability_metrics = metrics_engine.calculate_profitability_metrics()
    growth_metrics = metrics_engine.calculate_growth_metrics()
    cash_flow_metrics = metrics_engine.calculate_cash_flow_metrics()
    balance_sheet_metrics = metrics_engine.calculate_balance_sheet_metrics()

    save_dataframe(profitability_metrics, ticker_dir / "metrics" / "profitability.csv")
    save_dataframe(growth_metrics, ticker_dir / "metrics" / "growth.csv")
    save_dataframe(cash_flow_metrics, ticker_dir / "metrics" / "cash_flow.csv")
    save_dataframe(balance_sheet_metrics, ticker_dir / "metrics" / "balance_sheet.csv")

    SessionFactory = get_session_factory()
    session = SessionFactory()

    # --- Persist to SQLite ---
    try:
        upsert_mapping_validations(session, ticker, "income_statement", income_validation)
        upsert_mapping_validations(session, ticker, "balance_sheet", balance_validation)
        upsert_mapping_validations(session, ticker, "cash_flow", cash_validation)

        replace_unmapped_tags_for_statement(session, ticker, "income_statement", income_validation)
        replace_unmapped_tags_for_statement(session, ticker, "balance_sheet", balance_validation)
        replace_unmapped_tags_for_statement(session, ticker, "cash_flow", cash_validation)

                # --- Save mapping metadata ---
        replace_mapped_concept_selections_for_statement(
            session=session,
            ticker=ticker,
            statement_type="income_statement",
            mapping_metadata=income_result.selection_metadata,
        )
        replace_mapped_concept_selections_for_statement(
            session=session,
            ticker=ticker,
            statement_type="balance_sheet",
            mapping_metadata=balance_result.selection_metadata,
        )
        replace_mapped_concept_selections_for_statement(
            session=session,
            ticker=ticker,
            statement_type="cash_flow",
            mapping_metadata=cash_result.selection_metadata,
        )

        replace_mapped_concept_values_for_statement(
            session=session,
            ticker=ticker,
            statement_type="income_statement",
            mapped_df=income_mapped,
        )
        replace_mapped_concept_values_for_statement(
            session=session,
            ticker=ticker,
            statement_type="balance_sheet",
            mapped_df=balance_mapped,
        )
        replace_mapped_concept_values_for_statement(
            session=session,
            ticker=ticker,
            statement_type="cash_flow",
            mapped_df=cash_mapped,
        )
        session.commit()
    finally:
        session.close()
    print(f"Finished {ticker}.")


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