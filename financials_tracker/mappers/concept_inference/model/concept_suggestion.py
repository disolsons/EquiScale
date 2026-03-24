
from dataclasses import dataclass

@dataclass
class ConceptSuggestion:
    suggested_concept: str | None
    suggestion_confidence: float
    suggestion_type: str
    suggestion_reason: list[str]
