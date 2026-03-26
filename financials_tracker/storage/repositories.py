import json
from typing import Any
from financials_tracker.mappers.model.concept_selection_metadata import ConceptSelectionMetadata
from sqlalchemy.orm import Session
import pandas as pd
from financials_tracker.storage.models import (
    AggregatedUnmappedTags,
    UnmappedTags,
    MappingValidations,
    TagSuggestions,
    MappedConceptSelections,
    MappedConceptValues,
)

def upsert_mapping_validations(session: Session, ticker: str, statement_type: str, report: dict[str, Any]) -> None:
    coverage = report.get("coverage", {})
    reconciliation = report.get("reconciliation", {}).get("checks", {})

    reconciliation_passed = None
    reconciliation_method = None

    if reconciliation:
        first_check = next(iter(reconciliation.values()))
        reconciliation_passed = first_check.get("passed")
        reconciliation_method = first_check.get("method")

    payload = {
        "ticker": ticker.upper(),
        "statement_type": statement_type,
        "expected_concepts_count": coverage.get("expected_concepts_count", 0),
        "mapped_concepts_count": coverage.get("mapped_concepts_count", 0),
        "coverage_ratio": coverage.get("coverage_ratio", 0.0),
        "reconciliation_passed": reconciliation_passed,
        "reconciliation_method": reconciliation_method,
        "missing_concepts_count": len(coverage.get("missing_concepts", [])),
        "unmapped_tag_count": len(report.get("unmapped_tags", {}).get("unmapped_raw_tags", [])),
    }

    existing = (
        session.query(MappingValidations)
        .filter_by(ticker=ticker.upper(), statement_type=statement_type)
        .one_or_none()
    )

    if existing:
        for key, value in payload.items():
            setattr(existing, key, value)
    else:
        session.add(MappingValidations(**payload))

def replace_unmapped_tags_for_statement(
    session: Session,
    ticker: str,
    statement_type: str,
    report: dict[str, Any],
) -> None:
    session.query(UnmappedTags).filter_by(
        ticker=ticker.upper(),
        statement_type=statement_type,
    ).delete()

    rows = report.get("unmapped_tags", {}).get("unmapped_raw_tags", [])

    for row in rows:
        session.add(
            UnmappedTags(
                ticker=ticker.upper(),
                statement_type=statement_type,
                raw_tag=row.get("raw_tag"),
                label=row.get("label"),
                non_null_periods=row.get("non_null_periods"),
                is_abstract=row.get("is_abstract"),
                is_total=row.get("is_total"),
                depth=row.get("depth"),
                section=row.get("section"),
                confidence=row.get("confidence"),
            )
        )

def replace_aggregated_unmapped_tags(session: Session, rows: list[dict[str, Any]]) -> None:
    session.query(AggregatedUnmappedTags).delete()

    for row in rows:
        session.add(
            AggregatedUnmappedTags(
                statement_type=row.get("statement_type"),
                raw_tag=row.get("raw_tag"),
                count=row.get("count", 0),
                ticker_count=row.get("ticker_count", 0),
                tickers=json.dumps(row.get("tickers", [])),
                example_labels=json.dumps(row.get("example_labels", [])),
                max_non_null_periods=row.get("max_non_null_periods"),
                avg_non_null_periods=row.get("avg_non_null_periods"),
                is_abstract_values=json.dumps(row.get("is_abstract_values", [])),
                is_total_values=json.dumps(row.get("is_total_values", [])),
                depth_values=json.dumps(row.get("depth_values", [])),
                section_values=json.dumps(row.get("section_values", [])),
                avg_confidence=row.get("avg_confidence"),
            )
        )

def upsert_tag_suggestion(
    session,
    statement_type: str,
    raw_tag: str,
    suggestion: dict,
    )-> None:
    payload = {
        "statement_type": statement_type,
        "raw_tag": raw_tag,
        "suggested_concept": suggestion.get("suggested_concept"),
        "suggestion_type": suggestion.get("suggestion_type"),
        "suggestion_confidence": suggestion.get("suggestion_confidence", 0.0),
        "suggestion_reason": json.dumps(suggestion.get("suggestion_reason", [])),
        "source": "concept_inference_engine",
        "ticker_count": suggestion.get("ticker_count"),
        "priority_score": suggestion.get("priority_score"),
        "priority_bucket": suggestion.get("priority_bucket"),
    }

    existing = (
        session.query(TagSuggestions)
        .filter_by(statement_type=statement_type, raw_tag=raw_tag)
        .one_or_none()
    )

    if existing:
        for key, value in payload.items():
            setattr(existing, key, value)
    else:
        session.add(TagSuggestions(**payload))

def delete_all_tag_suggestions(session) -> None:
    """
    Remove all previously generated tag suggestions.

    This keeps the suggestions table in sync with the latest inference run
    instead of accumulating stale rows across runs.
    """
    session.query(TagSuggestions).delete()

def replace_mapped_concept_selections_for_statement(
    session: Session,
    ticker: str,
    statement_type: str,
    mapping_metadata: list[ConceptSelectionMetadata],
) -> None:
    """
    Replace candidate-level mapping metadata rows for one ticker + statement.

    Stores all evaluated candidates, with is_selected indicating the winner.
    """
    session.query(MappedConceptSelections).filter_by(
        ticker=ticker.upper(),
        statement_type=statement_type,
    ).delete()

    for item in mapping_metadata:
        session.add(
            MappedConceptSelections(
                ticker=ticker.upper(),
                statement_type=statement_type,
                concept=item.concept,
                raw_tag=item.raw_tag,
                label=item.label,
                is_abstract=item.is_abstract,
                is_total=item.is_total,
                depth=item.depth,
                non_null_periods=item.non_null_periods,
                candidate_score=item.candidate_score,
                is_selected=item.is_selected,
                rank_order=item.rank_order,
                candidate_count=item.candidate_count,
            )
        )
        
def replace_mapped_concept_values_for_statement(
    session: Session,
    ticker: str,
    statement_type: str,
    mapped_df,
) -> None:
    """
    Replace mapped concept values for one ticker + statement.

    Stores one row per concept-period numeric value.
    """
    session.query(MappedConceptValues).filter_by(
        ticker=ticker.upper(),
        statement_type=statement_type,
    ).delete()

    if mapped_df is None or mapped_df.empty:
        return

    for concept, row in mapped_df.iterrows():
        for period, value in row.items():
            session.add(
                MappedConceptValues(
                    ticker=ticker.upper(),
                    statement_type=statement_type,
                    concept=str(concept),
                    period=str(period),
                     value=None if pd.isna(value) else float(value),                )
            )