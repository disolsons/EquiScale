from dataclasses import asdict, dataclass


@dataclass
class ConceptSelectionMetadata:
    """
    Candidate-level provenance metadata for a normalized concept mapping.

    Captures all raw-tag candidates considered for a concept, including
    the score assigned by the ranker and whether the candidate was selected.
    """
    concept: str
    raw_tag: str
    label: str | None
    is_abstract: bool | None
    is_total: bool | None
    depth: int | None
    non_null_periods: int
    candidate_score: float
    is_selected: bool
    rank_order: int
    candidate_count: int

    def to_dict(self) -> dict:
        return asdict(self)