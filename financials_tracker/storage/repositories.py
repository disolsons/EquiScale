import json
from typing import Any

from sqlalchemy.orm import Session

from financials_tracker.storage.models import (
    AggregatedUnmappedTag,
    UnmappedTag,
    MappingValidations,
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
    session.query(UnmappedTag).filter_by(
        ticker=ticker.upper(),
        statement_type=statement_type,
    ).delete()

    rows = report.get("unmapped_tags", {}).get("unmapped_raw_tags", [])

    for row in rows:
        session.add(
            UnmappedTag(
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
    session.query(AggregatedUnmappedTag).delete()

    for row in rows:
        session.add(
            AggregatedUnmappedTag(
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