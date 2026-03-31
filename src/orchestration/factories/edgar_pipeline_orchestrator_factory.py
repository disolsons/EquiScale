from pathlib import Path

from src.clients.edgar.edgar_client import EdgarClient
from src.orchestration.edgar_pipeline_orchestrator import EdgarPipelineOrchestrator
from src.processing.enrichers.statement_fact_enricher import StatementFactEnricher
from src.processing.mappers.raw_statement_row_factory import RawStatementRowFactory
from src.processing.mappers.statement_mapper import StatementMapper
from src.processing.utils.concept_map_helper import ConceptMapHelper
from src.services.data_services.report_data_service import ReportDataService
from src.services.metrics.metrics_service import MetricsService
from src.storage.db_setup import get_session_factory


class EdgarPipelineOrchestratorFactory:
    """
    Factory responsible for constructing a fully wired EdgarPipelineOrchestrator.

    This class acts as the composition root for the EDGAR-based financial pipeline.
    It centralizes dependency creation so the orchestrator itself remains focused
    on workflow coordination rather than infrastructure setup.

    - initialize shared helpers and services
    - build source-specific EDGAR collaborators
    - inject infrastructure dependencies such as session factories
    - return a ready-to-use EdgarPipelineOrchestrator
    """

    def __init__(
        self,
        concept_map_path: str | Path
    ) -> None:
        self.concept_map_path = Path(concept_map_path)

    def build(self) -> EdgarPipelineOrchestrator:
        """
        Build a fully configured EdgarPipelineOrchestrator.

        Returns:
            A ready-to-use EdgarPipelineOrchestrator with all required
            collaborators and services initialized.
        """
        concept_map_helper = ConceptMapHelper(self.concept_map_path)

        session_factory = get_session_factory()

        client = EdgarClient(concept_helper=concept_map_helper)
        mapper = StatementMapper(concept_map_helper=concept_map_helper)

        enricher = StatementFactEnricher(
            concept_helper=concept_map_helper,
            edgar_client=client,
        )
        raw_statement_row_factory = RawStatementRowFactory()
        report_data_service = ReportDataService(
            session_factory=session_factory,
            raw_statement_row_factory=raw_statement_row_factory,
        )
        metrics_service = MetricsService()

        orchestrator = EdgarPipelineOrchestrator(
            client=client,
            mapper=mapper,
            enricher=enricher,
            report_data_service=report_data_service,
            metrics_service=metrics_service,
        )

        return orchestrator