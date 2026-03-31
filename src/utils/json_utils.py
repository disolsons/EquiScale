from typing import Any

import pandas as pd
from datetime import date, datetime

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

    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    
    if pd.isna(obj):
        return None

    if hasattr(obj, "item"):
        try:
            return obj.item()
        except Exception:
            pass

    return obj


def dataframe_to_json_payload(df: pd.DataFrame | None) -> dict[str, Any]:
    if df is None or df.empty:
        return {"rows": [], "columns": []}
    return {
        "columns": [str(c) for c in df.columns],
        "index": [str(i) for i in df.index],
        "rows": df.reset_index().to_dict(orient="records"),
    }