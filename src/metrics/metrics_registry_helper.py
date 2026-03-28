from pathlib import Path
from typing import Any

import yaml


class MetricsRegistryHelper:
    """
    Loads and provides access to metric definitions from YAML.
    """

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.registry = self._load_registry(self.path)

    def _load_registry(self, path: Path) -> dict[str, dict[str, Any]]:
        with path.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}

    def get_registry(self) -> dict[str, dict[str, Any]]:
        return self.registry

    def get_metric_definition(self, metric_name: str) -> dict[str, Any] | None:
        return self.registry.get(metric_name)

    def has_metric(self, metric_name: str) -> bool:
        return metric_name in self.registry

    def get_metric_names(self) -> list[str]:
        return list(self.registry.keys())