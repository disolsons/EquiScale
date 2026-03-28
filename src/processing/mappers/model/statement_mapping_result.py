from dataclasses import dataclass
import pandas as pd

from src.processing.mappers.model.concept_selection_metadata import ConceptSelectionMetadata


@dataclass
class StatementMappingResult:
    """
    Result of mapping a statement into normalized concepts.

    Attributes
    ----------
    mapped_df : pd.DataFrame | None
        Normalized concept-value table used by downstream metrics/validation.

    selection_rows : list[dict]
        Per-concept mapping provenance rows describing which raw tag was selected.
    """
    mapped_df: pd.DataFrame | None
    selection_metadata: list[ConceptSelectionMetadata]