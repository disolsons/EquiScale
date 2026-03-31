from dataclasses import dataclass
from typing import Any

@dataclass
class ReportDiagnostics:
    """
    Report level processing and metadata outputs
    """
    ticker: str
    report_type: str
    mapping_result: Any | None = None
    validation: dict | None = None
    unmapped_tags: list[dict] | None = None


