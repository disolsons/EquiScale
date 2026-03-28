from dataclasses import dataclass, field
from typing import Any


@dataclass
class RawStatementRow:
    """
    In-memory pipeline object representing one raw EDGAR/XBRL statement row
    across multiple periods.
    """
    raw_tag: str
    label: str | None
    statement_type: str
    values_by_period: dict[str, float | None]

    is_total: bool | None = None
    is_abstract: bool | None = None
    depth: int | None = None
    section: str | None = None
    confidence: float | None = None
    non_null_periods: int | None = None

    parent_raw_tag: str | None = None
    parent_label: str | None = None
    calculation_weight: float | None = None

    source_layer: str | None = None   # "statement_frame" | "facts_query_backfill"

    ticker: str | None = None
    source_concept: str | None = None
    filing_accession: str | None = None
    filing_date: str | None = None

    extra_metadata: dict[str, Any] = field(default_factory=dict)

    def compute_non_null_periods(self) -> int:
        return sum(v is not None for v in self.values_by_period.values())

    def with_computed_fields(self) -> "RawStatementRow":
        self.non_null_periods = self.compute_non_null_periods()
        return self

    def to_period_records(self) -> list[dict]:
        """
        Flatten to one-record-per-period for DB persistence.
        """
        records = []

        for period_label, value in self.values_by_period.items():
            records.append({
                "ticker": self.ticker,
                "statement_type": self.statement_type,
                "raw_tag": self.raw_tag,
                "label": self.label,
                "period_label": period_label,
                "value": value,
                "is_total": self.is_total,
                "is_abstract": self.is_abstract,
                "depth": self.depth,
                "section": self.section,
                "confidence": self.confidence,
                "non_null_periods": self.non_null_periods,
                "parent_raw_tag": self.parent_raw_tag,
                "parent_label": self.parent_label,
                "calculation_weight": self.calculation_weight,
                "source_layer": self.source_layer,
                "source_concept": self.source_concept,
                "filing_accession": self.filing_accession,
                "filing_date": self.filing_date,
            })

        return records