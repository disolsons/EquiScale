from dataclasses import asdict, dataclass


@dataclass
class ConceptSelectionMetadata:
    """
    Provenance metadata for a normalized concept mapping.

    Captures which raw tag was selected to represent a normalized concept
    and the structural signals that supported that selection.
    """
    concept: str
    selected_raw_tag: str
    selected_label: str | None
    is_abstract: bool | None
    is_total: bool | None
    depth: int | None
    non_null_periods: int
    selection_score: float
    candidate_count: int

    def to_dict(self) -> dict:
        """
        Convert the metadata object to a plain dictionary,
        useful for DB persistence or JSON serialization.
        """
        return asdict(self)