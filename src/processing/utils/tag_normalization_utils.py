
import re

def normalize_text(value: str) -> str:
    # Replace underscores with spaces so snake_case words can be matched naturally.
    value = value.replace("_", " ")
    # Insert a space between lowercase-to-uppercase transitions to split CamelCase words. Since tags are commonly written like this "DepreciationDepletionAndAmortization"
    value = re.sub(r"([a-z])([A-Z])", r"\1 \2", value)
    # Lowercase everything to make matching case-insensitive.
    value = value.lower()
    # Replace any non-alphanumeric characters with spaces to simplify comparison.
    value = re.sub(r"[^a-z0-9]+", " ", value)
    # Collapse repeated whitespace into a single space and trim leading/trailing spaces.
    value = re.sub(r"\s+", " ", value).strip()
    return value