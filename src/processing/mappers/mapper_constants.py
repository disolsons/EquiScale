from dataclasses import dataclass

@dataclass(frozen=True)
class MapperConstants:
    FISCAL_YEAR_PREFIX: str = "FY "
    QUARTER_PREFIX: str = "Q"
    IS_ABSTRACT_COL: str = "is_abstract"
    CONCEPT_COL: str = "concept"