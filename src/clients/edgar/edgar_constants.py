from dataclasses import dataclass

@dataclass(frozen=True)
class EdgarConstants:
    """Constants specifically for SEC Edgar data processing."""

    #Types of document
    STATEMENT_TYPE_INCOME: str = "income_statement"
    STATEMENT_TYPE_BALANCE_SHEET: str = "balance_sheet"
    STATEMENT_TYPE_CASH_FLOW: str = "cash_flow"

    #Retrieval mode
    MODE_LATEST = "latest"
    MODE_HISTORY = "history"

    #Document structure 
    LAYER_STATEMENT_FRAME = "statement_frame"
    COLUMN_PARENT_TAG = "parent_raw_tag"
    COLUMN_CALCULATION_WEIGHT = "calculation_weight"
    COLUMN_SOURCE_LAYER = "source_layer"

    #TAG PREFIX
    US_TAG_PREFIX = "us-gaap"