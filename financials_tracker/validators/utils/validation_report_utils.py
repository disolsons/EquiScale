import json
from pathlib import Path
from typing import Any

import pandas as pd


def make_json_safe(obj: Any) -> Any:
    """
    Recursively convert common pandas/numpy-style values into JSON-safe Python values.
    """
    if isinstance(obj, dict):
        return {str(k): make_json_safe(v) for k, v in obj.items()}

    if isinstance(obj, list):
        return [make_json_safe(v) for v in obj]

    if isinstance(obj, tuple):
        return [make_json_safe(v) for v in obj]

    if pd.isna(obj):
        return None

    if hasattr(obj, "item"):
        try:
            return obj.item()
        except Exception:
            pass

    return obj

def save_validation_report(report: dict, output_path: str | Path) -> None:
    """
    Save a validation report to a prettified JSON file.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    safe_report = make_json_safe(report)

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(safe_report, f, indent=2, ensure_ascii=False)

