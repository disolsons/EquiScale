from pathlib import Path
import yaml


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