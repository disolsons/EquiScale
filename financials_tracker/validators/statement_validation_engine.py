from typing import Any

import pandas as pd

from financials_tracker.mappers.concept_map_helper import ConceptMapHelper
from financials_tracker.validators.coverage_validator import StatementCoverageValidator
from financials_tracker.validators.mapping_detail_validator import StatementMappingDetailValidator
from financials_tracker.validators.reconciliation_validator import StatementReconciliationValidator
from financials_tracker.validators.unmapped_tags_validator import UnmappedTagsValidator
from financials_tracker.validators.utils.ignored_tags_helper import IgnoredTagsHelper

class StatementValidationEngine:
    """
    Orchestrator for statement validation.

    Evaluates the quality and completeness of mapped financial statements.

    It does not modify the mapped output. It only inspects and reports:
    - mapped concepts
    - missing concepts
    - raw row mappings for each normalized concept
    - reconciliation checks
    - unmapped raw tags
    """

    def __init__(self, concept_map_helper: ConceptMapHelper, ignored_tags_helper: IgnoredTagsHelper):
        self.coverage_validator = StatementCoverageValidator(concept_map_helper)
        self.mapping_detail_validator = StatementMappingDetailValidator(concept_map_helper)
        self.reconciliation_validator = StatementReconciliationValidator()
        self.unmapped_tags_validator = UnmappedTagsValidator(concept_map_helper)
        self.ignored_tags_helper = ignored_tags_helper

    def validate(
        self,
        raw_df: pd.DataFrame | None,
        mapped_df: pd.DataFrame | None,
        statement_type: str,
        tolerance: float = 1e-6,
        ignored_tags: set[str] | None = None,
    ) -> dict[str, Any]:
        """
        Run the full validation suite for one statement.

        Returns a structured validation report including:
        - coverage
        - mapping detail
        - reconciliation checks
        - unmapped raw tags
        """
        ignored_tags = self.ignored_tags_helper.get_ignored_tags(statement_type)
        
        return {
            "statement_type": statement_type,
            "coverage": self.coverage_validator.validate(mapped_df, statement_type),
            "mapping_detail": self.mapping_detail_validator.validate(raw_df, mapped_df, statement_type),
            "reconciliation": self.reconciliation_validator.validate(mapped_df, statement_type, tolerance),
            "unmapped_tags": self.unmapped_tags_validator.validate(raw_df, statement_type, ignored_tags),
        }