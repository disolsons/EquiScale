import yaml
from pathlib import Path


class IgnoredTagsHelper:
    """
    Helper class to manage ignored tags for validation, these are usually structural tags that are not relevant
    or contain metadata rather than actual financial data.
    """
    def __init__(self, path="financials_tracker/validators/config/ignored_tags.yaml"):
        self.ignored_tags_map = self._load_ignored_tags(path)

    def _load_ignored_tags(self, path):
        with open(path, "r") as f:
            return yaml.safe_load(f)

    def get_ignored_tags(self, statement_type: str) -> set[str]:
        tags = self.ignored_tags_map.get(statement_type, [])
        return set(tags)