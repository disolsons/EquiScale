from src.domain.financial_dataset import FinancialDataset
from src.domain.financial_report import FinancialReport
from src.clients.edgar.edgar_client import EdgarClient
from src.clients.edgar.edgar_constants import EdgarConstants
from src.processing.mappers.statement_mapper import StatementMapper
from src.processing.enrichers.statement_fact_enricher import StatementFactEnricher
from src.processing.mappers.raw_statement_row_factory import RawStatementRowFactory
from src.processing.mappers.model.statement_mapping_result import StatementMappingResult
from src.services.data_services.report_data_service import ReportDataService
from src.services.metrics.metrics_service import MetricsService
class EdgarPipelineOrchestrator:
    
    def __init__(
        self,
        client: EdgarClient,
        mapper: StatementMapper,
        enricher: StatementFactEnricher,
        report_data_service: ReportDataService,
        metrics_service: MetricsService,
        output_writer=None,
    ):
        self.client = client
        self.mapper = mapper
        self.enricher = enricher
        self.report_data_service = report_data_service
        self.metrics_service = metrics_service
        self.output_writer = output_writer

    
    def build_historical_dataset(self, ticker: str, years: int, annual: bool) -> FinancialDataset:
        """
        Build a historical financial dataset for a single ticker using EDGAR data.
        This is the public orchestration entrypoint for the historical pipeline.

        Args:
            ticker: Public company ticker symbol to process.
            years: Number of historical periods to fetch.
            annual: Whether to fetch annual statements (`True`) or quarterly
                statements (`False`).

        Returns:
            FinancialDataset containing the three fetched historical reports.

        Raises:
            ValueError: If input arguments are invalid.
            RuntimeError: If one of the required financial reports cannot be fetched.
        """
        if not ticker or not ticker.strip():
            raise ValueError("ticker must be a non-empty string")

        if years <= 0:
            raise ValueError("years must be greater than 0 for a historical report")

        dataset = FinancialDataset(ticker=ticker)

        # Retrieve raw statement data from EdgarClient.

        income_statement = self._fetch_income_statement(
            ticker=ticker,
            period_mode="history",
            years=years,
            annual=annual,
        )

        if income_statement is None:
            raise RuntimeError(f"Failed to build historical income statement for ticker={ticker}")
        
        balance_sheet = self._fetch_balance_sheet(
            ticker=ticker,
            period_mode="history",
            years=years,
            annual=annual,
        )
        if balance_sheet is None:
            raise RuntimeError(f"Failed to build historical balance sheet for ticker={ticker}")

        cash_flow = self._fetch_cash_flow(
            ticker=ticker,
            period_mode="history",
            years=years,
            annual=annual,
        )

        if cash_flow is None:
            raise RuntimeError(f"Failed to build historical cash flow for ticker={ticker}")

        # Enrich datasets by backfilling missing direct tags (usually child items not included in certain report structures)
        # FIXME: Currently only CASH_FLOW requires backfill - this might change in the future.
        cash_flow = self._enrich_cash_flow_report(cash_flow_report=cash_flow, years=years, annual=annual)
        
        # Map raw tags values to standarized concepts. 

        income_statement = self._apply_mapping_to_report(
            report = income_statement,
            period_mode=EdgarConstants.MODE_HISTORY)
        
        balance_sheet = self._apply_mapping_to_report(
            report = balance_sheet,
            period_mode=EdgarConstants.MODE_HISTORY)
        
        cash_flow = self._apply_mapping_to_report(
            report = cash_flow,
            period_mode=EdgarConstants.MODE_HISTORY)
        
        #Persist all reports
        dataset.cash_flow = cash_flow
        dataset.balance_sheet = balance_sheet
        dataset.income_statement = income_statement

        self.report_data_service.persist_reports_in_dataset(dataset=dataset)
    
        #Calculate metrics
        
        metrics = self.metrics_service.calculate_all_metrics(dataset=dataset)
        if metrics is None:
            raise RuntimeError(f"Failed to calculate metrics for ticker {dataset.ticker}")

        dataset.metrics = metrics
        self.report_data_service.persist_metrics_in_dataset(dataset=dataset)

        return dataset

    def _fetch_income_statement(self, ticker:str, period_mode: str, years: int, annual: bool) -> FinancialReport:
        """
            Fetch the income statement for a ticker from EDGAR and wrap it in a FinancialReport.

            Args:
                ticker: Public company ticker symbol to fetch.
                period_mode: EDGAR retrieval mode.
                    - "latest": fetch the latest available rendered statement
                    - "history": fetch a multi-period historical statement
                years: Number of periods to request when `period_mode="history"`.
                annual: Whether to fetch annual statements (`True`) or quarterly statements (`False`).

            Returns:
                FinancialReport for the income statement with the raw EDGAR output assigned
                to `raw`.

            Raises:
                ValueError: If input arguments are invalid.
                RuntimeError: If the EDGAR client returns no income statement.
        """
        self._validate_fetch_inputs(ticker=ticker, period_mode=period_mode, years=years)
        
        report = FinancialReport(
            ticker=ticker,
            report_type=EdgarConstants.STATEMENT_TYPE_INCOME,
        )

        raw_df = self.client.fetch_income_statement(
            period_mode=period_mode,
            years=years,
            annual=annual,
            ticker=ticker,
        )

        if raw_df is None:
            raise RuntimeError(f"No income statement returned for ticker={ticker}")

        report.raw = raw_df
        return report
    
    def _fetch_balance_sheet(self, ticker:str, period_mode: str, years: int, annual: bool) -> FinancialReport:
        """
            Fetch the balance sheet for a ticker from EDGAR and wrap it in a FinancialReport.

            Args:
                ticker: Public company ticker symbol to fetch.
                period_mode: EDGAR retrieval mode.
                    - "latest": fetch the latest available rendered statement
                    - "history": fetch a multi-period historical statement
                years: Number of periods to request when `period_mode="history"`.
                annual: Whether to fetch annual statements (`True`) or quarterly statements (`False`).

            Returns:
                FinancialReport for the balance sheet with the raw EDGAR output assigned
                to `raw`.

            Raises:
                ValueError: If input arguments are invalid.
                RuntimeError: If the EDGAR client returns no balance sheet.
        """
        self._validate_fetch_inputs(ticker=ticker, period_mode=period_mode, years=years)
        
        report = FinancialReport(
            ticker=ticker,
            report_type=EdgarConstants.STATEMENT_TYPE_BALANCE_SHEET,
        )

        raw_df = self.client.fetch_balance_sheet(
            period_mode=period_mode,
            years=years,
            annual=annual,
            ticker=ticker,
        )

        if raw_df is None:
            raise RuntimeError(f"No balance sheet returned for ticker={ticker}")

        report.raw = raw_df
        return report
        
    def _fetch_cash_flow(self, ticker:str, period_mode: str, years: int, annual: bool) -> FinancialReport:
        """
            Fetch the cash flow statement for a ticker from EDGAR and wrap it in a FinancialReport.
            Args:
                ticker: Public company ticker symbol to fetch.
                period_mode: EDGAR retrieval mode.
                    - "latest": fetch the latest available rendered statement
                    - "history": fetch a multi-period historical statement
                years: Number of periods to request when `period_mode="history"`.
                annual: Whether to fetch annual statements (`True`) or quarterly statements (`False`).

            Returns:
                FinancialReport for the cash flow statement with the raw EDGAR output assigned
                to `raw`.

            Raises:
                ValueError: If input arguments are invalid.
                RuntimeError: If the EDGAR client returns no cash flow statement.
        """
        self._validate_fetch_inputs(ticker=ticker, period_mode=period_mode, years=years)

        report = FinancialReport(
            ticker=ticker,
            report_type=EdgarConstants.STATEMENT_TYPE_CASH_FLOW,
        )

        raw_df = self.client.fetch_cash_flow(
            period_mode=period_mode,
            years=years,
            annual=annual,
            ticker=ticker,
        )

        if raw_df is None:
            raise RuntimeError(f"No cash flow statement returned for ticker={ticker}")

        report.raw = raw_df
        return report
    
    def _enrich_cash_flow_report(
        self,
        cash_flow_report: FinancialReport,
        years: int,
        annual: bool,
    ) -> FinancialReport:
        """
        Enrich a cash flow FinancialReport by backfilling missing direct-mapping tags.

        This method applies the cash flow enrichment step after the raw report has
        already been fetched. It uses the StatementFactEnricher to recover missing
        child-line concepts that may not appear in the statement-oriented EDGAR
        frame, such as capital expenditure rows, and stores the enriched result in
        the report's `enriched` field.

        Args:
            cash_flow_report: FinancialReport for the cash flow statement. Must contain
                a valid `ticker` and a non-null raw DataFrame in `raw`.
            years: Number of historical periods requested for enrichment.
            annual: Whether the report is annual (`True`) or quarterly (`False`).

        Returns:
            The same FinancialReport instance with its `enriched` field populated.

        Raises:
            ValueError: If the input report or required fields are missing or invalid.
            RuntimeError: If enrichment fails or returns no enriched DataFrame.
        """
        if cash_flow_report is None:
            raise ValueError("cash_flow_report must not be None")

        if not cash_flow_report.ticker or not cash_flow_report.ticker.strip():
            raise ValueError("ticker in cash_flow_report must be a non-empty string")

        if years <= 0:
            raise ValueError("years must be greater than 0 when enriching a historical cash flow report")

        if cash_flow_report.raw is None:
            raise ValueError("cash_flow_report must contain a non-null raw DataFrame before enrichment")

        statement_raw_df = cash_flow_report.raw

        enriched_df = self.enricher.enrich_missing_direct_tags(
            statement_df=statement_raw_df,
            statement_type=EdgarConstants.STATEMENT_TYPE_CASH_FLOW,
            ticker=cash_flow_report.ticker,
            years=years,
            annual=annual,
        )

        if enriched_df is None:
            raise RuntimeError(
                f"Could not enrich cash flow report for ticker={cash_flow_report.ticker}"
            )

        # Remove possible duplicate fiscal period columns introduced during backfill/merge.
        enriched_df = enriched_df.loc[:, ~enriched_df.columns.duplicated()]

        # Keep the first occurrence if duplicate raw tags appear after enrichment.
        enriched_df = enriched_df[~enriched_df.index.duplicated(keep="first")]

        cash_flow_report.enriched = enriched_df
        return cash_flow_report
    
    def _apply_mapping_to_report(self, report: FinancialReport, period_mode: str) -> FinancialReport:
        """
            Map a FinancialReport from raw EDGAR data into normalized concepts.
            Args:
                report: FinancialReport to map. Must contain at least one usable source
                    DataFrame in `raw` or `enriched`.
                period_mode: Mapping mode.
                    - "history": map a multi-period historical statement
                    - "latest": map the latest rendered statement

            Returns:
                The same FinancialReport instance with its `mapped` field populated.

            Raises:
                ValueError: If inputs are missing or invalid.
                RuntimeError: If mapping fails or produces no mapped DataFrame.
        """
        if report is None:
            raise ValueError("report must not be None")

        if period_mode not in {EdgarConstants.MODE_LATEST, EdgarConstants.MODE_HISTORY}:
            raise ValueError(
                f"period_mode must be either '{EdgarConstants.MODE_LATEST}' "
                f"or '{EdgarConstants.MODE_HISTORY}'"
            )
        
        source_df = report.enriched if report.enriched is not None else report.raw
        if source_df is None:
            raise ValueError(
                f"report must contain a non-null raw or enriched DataFrame before mapping "
                f"(ticker={report.ticker}, report_type={report.report_type})"
            )
        
        if period_mode == EdgarConstants.MODE_HISTORY:
            mapping_result = self.mapper.map_historical_statement(
                df=source_df,
                statement_type=report.report_type,
            )
        else:
            mapping_result = self.mapper.map_latest_statement(
                df=source_df,
                statement_type=report.report_type,
            )

        self._validate_mapping_result(mapping_result, report.report_type)
        report.mapped = mapping_result.mapped_df
        report.selection_metadata = mapping_result.selection_metadata

        return report

    def _validate_mapping_result(self, mapping_result: StatementMappingResult, report_type: str):
        """
        Validate mapping result was correctly populated

        Args:
            mapping_result: The response from the mapper, containing the mapped dataframe and selection_metadata
            report_type: Statement identifier, such as:
                - income_statement
                - balance_sheet
                - cash_flow          
        Raises:
            RuntimeError: If the mapper failed to populate any of the required data
        """

        if mapping_result is None:
            raise RuntimeError(
                f"Failed to map {report_type} report for ticker={report_type}"
            )
        if mapping_result.mapped_df is None:
            raise RuntimeError(
                f"Mapper returned no mapped_df for {report_type} report"
            )

        if not mapping_result.selection_metadata:
            raise RuntimeError(f"Mapper returned no selection_metadata for {report_type} report"
            )
        
    def _validate_fetch_inputs(
        self,
        ticker: str,
        period_mode: str,
        years: int,
    ) -> None:
        """
        Validate common inputs used by EDGAR financial report fetch helpers.

        Args:
            ticker: Public company ticker symbol to fetch.
            period_mode: Extraction mode. Expected values are "latest" or "history".
            years: Number of historical periods to request when using history mode.

        Raises:
            ValueError: If any input is missing or invalid.
        """
        if not ticker or not ticker.strip():
            raise ValueError("ticker must be a non-empty string")

        if period_mode not in {"latest", "history"}:
            raise ValueError("period_mode must be either 'latest' or 'history'")

        if period_mode == "history" and years <= 0:
            raise ValueError("years must be greater than 0 when period_mode='history'")