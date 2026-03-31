from dataclasses import dataclass
from typing import Any
import pandas as pd
from src.processing.mappers.model.concept_selection_metadata import ConceptSelectionMetadata
@dataclass
class FinancialReport:
    """
    Financial report's data lifecycle
    """
    ticker: str
    report_type: str
    raw: pd.DataFrame | None = None
    enriched: pd.DataFrame | None = None
    mapped: pd.DataFrame | None = None
    selection_metadata: list[ConceptSelectionMetadata] | None = None

    def get_raw_dataframe(self) -> pd.DataFrame | None:
        if isinstance(self.raw, pd.DataFrame):
            return self.raw
        return None

    def get_enriched_dataframe(self) -> pd.DataFrame | None:
        if isinstance(self.enriched, pd.DataFrame):
            return self.enriched
        return None
        
    def get_mapped_dataframe(self) -> pd.DataFrame | None:
        if isinstance(self.mapped, pd.DataFrame):
            return self.mapped
        return None
    
    def get_selection_metadata(self) -> list[ConceptSelectionMetadata] | None:
        if isinstance(self.selection_metadata, list[ConceptSelectionMetadata]):
            return self.selection_metadata
        return None

