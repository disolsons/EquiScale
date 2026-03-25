from dataclasses import dataclass
from typing import Any


@dataclass
class ConceptCandidate:
    """
    A raw-statement candidate row for a normalized concept.

    This object stores the structural metadata needed to rank multiple
    mapped raw tags that all belong to the same normalized concept.
    """
    raw_tag: str
    label: str | None
    is_abstract: bool | None
    is_total: bool | None
    depth: int | None
    non_null_periods: int
    row_data: dict[str, Any]