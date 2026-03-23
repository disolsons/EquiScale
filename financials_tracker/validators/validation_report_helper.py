import json
from pathlib import Path


def save_validation_report(report: dict, output_path: str | Path) -> None:
    """
    Save a validation report to a prettified JSON file.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)