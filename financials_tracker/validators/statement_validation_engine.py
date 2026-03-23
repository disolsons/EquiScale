from typing import Any

import pandas as pd

from financials_tracker.mappers.concept_map_helper import ConceptMapHelper
from financials_tracker.validators.coverage_validator import StatementCoverageValidator
from financials_tracker.validators.mapping_detail_validator import StatementMappingDetailValidator
from financials_tracker.validators.reconciliation_validator import StatementReconciliationValidator


class StatementValidationEngine:
    """
    Orchestrator for statement validation. Evaluates the quality and completeness of mapped financial statements.

    It does not modify the mapped output. Only inspects and reports: 
    - Mapped concepts
    - Missing concepts
    - Raw row mappings for eaach normalized concept
    - Reconciliation checks (e.g. balance sheet equation, gross profit check)
    """
    

    def __init__(self, concept_map_helper: ConceptMapHelper):
        self.coverage_validator = StatementCoverageValidator(concept_map_helper)
        self.mapping_detail_validator = StatementMappingDetailValidator(concept_map_helper)
        self.reconciliation_validator = StatementReconciliationValidator()

    def validate_statement(
        self,
        raw_df: pd.DataFrame | None,
        mapped_df: pd.DataFrame | None,
        statement_type: str,
        tolerance: float = 1e-6,
    ) -> dict[str, Any]:
        """
        Run the full validation suite for one statement.

        Returns:
        - coverage
        - mapping detail
        - reconciliation checks
        """
        return {
            "statement_type": statement_type,
            "coverage": self.coverage_validator.validate(mapped_df, statement_type),
            "mapping_detail": self.mapping_detail_validator.validate(raw_df, mapped_df, statement_type),
            "reconciliation": self.reconciliation_validator.validate(mapped_df, statement_type, tolerance),
        }

    