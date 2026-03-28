from pathlib import Path
import yaml
from typing import Any


class ConceptAliasesHelper:
    """
    Loads and provides access to human-readable aliases for standardized concepts.
    """

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.aliases_map = self._load_aliases(self.path)

    def _load_aliases(self, path: Path) -> dict[str, Any]:
        with path.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}

    def get_aliases_map(self) -> dict[str, Any]:
        """
        Return the full concept aliases map.
        """
        return self.aliases_map

    def get_statement_aliases(self, statement_type: str) -> dict[str, list[str]]:
        """
        Return the concept-to-alias mapping for a single statement type.
        """
        return self.aliases_map.get(statement_type, {})

    def get_aliases_for_concept(self, statement_type: str, concept: str) -> list[str]:
        """
        Return aliases for a specific concept within a statement type.
        """
        return self.aliases_map.get(statement_type, {}).get(concept, [])

    def get_concepts_for_statement(self, statement_type: str) -> list[str]:
        """
        Return all concept names defined for a given statement type.
        """
        return list(self.aliases_map.get(statement_type, {}).keys())

    def has_concept(self, statement_type: str, concept: str) -> bool:
        """
        Check whether a concept exists in the aliases config for the given statement type.
        """
        return concept in self.aliases_map.get(statement_type, {})    
    
class SemanticConflictsHelper:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.conflicts = self._load(path)

    def _load(self, path: str | Path) -> dict:
        with Path(path).open("r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}

    def get_hard_conflicts(self) -> list[tuple[str, str]]:
        return [tuple(pair) for pair in self.conflicts.get("hard_conflicts", [])]

    def get_soft_conflicts(self) -> list[tuple[str, str]]:
        return [tuple(pair) for pair in self.conflicts.get("soft_conflicts", [])]

class IgnorePatternsHelper:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.patterns = self._load(path)

    def _load(self, path: str | Path) -> dict:
        with Path(path).open("r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}

    def get_header_phrases(self) -> list[str]:
        return self.patterns.get("header_phrases", [])

    def get_ignore_suffixes(self) -> list[str]:
        return self.patterns.get("ignore_suffixes", [])

    def get_ignore_contains(self) -> list[str]:
        return self.patterns.get("ignore_contains", [])