from pathlib import Path
import yaml


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