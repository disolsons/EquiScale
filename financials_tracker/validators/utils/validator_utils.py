from typing import Any

import pandas as pd

def relative_difference(a: float, b: float) -> float:
    """
        Relative difference measures how far apart two values are compared to their scale.
        Formula: absolute(a - b) / max(absolute(a), absolute(b), 1e-12)
        This makes the comparison proportional rather than using raw absolute distance.
        A small floor (1e-12) is used to avoid division by zero.
    """
    denominator = max(abs(a), abs(b), 1e-12)
    return abs(a - b) / denominator

def to_python_scalar(value: Any) -> Any:
    """
    Convert a NumPy scalar to a native Python scalar, handling NaNs appropriately.
    """
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return value
    return value

def normalize_raw_tag(raw_tag: Any) -> Any:
    if not isinstance(raw_tag, str):
        return raw_tag

    if "_" in raw_tag:
        return raw_tag.split("_", 1)[1]

    if ":" in raw_tag:
        return raw_tag.split(":", 1)[1]

    return raw_tag

def has_historical_raw_format(df: pd.DataFrame) -> bool:
    return any(str(col).startswith(("FY ", "Q")) for col in df.columns)

def get_period_columns(df: pd.DataFrame) -> list[str]:
    return [col for col in df.columns if str(col).startswith(("FY ", "Q"))]