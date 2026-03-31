import pandas as pd
from src.processing.mappers.raw_statement_row_factory import RawStatementRowFactory
from typing import Iterable
from src.domain.financial_report import FinancialReport
from src.domain.financial_dataset import FinancialDataset
from src.storage.repositories import (
    replace_raw_statement_facts_for_statement,
    replace_mapped_concept_selections_for_statement,
    replace_mapped_concept_values_for_statement,
    replace_metric_values_for_ticker
)

class ReportDataService:
    """
    Application-level data service responsible for persisting pipeline outputs.
    
    It owns transaction/session boundaries and coordinates persistence of:
    - raw financial report facts
    - mapped concept values
    - mapping provenance / selection metadata
    - validation results #Not sure
    - unmapped tags #Not sure
    """

    def __init__(
        self,
        session_factory,
        raw_statement_row_factory: RawStatementRowFactory,
    ) -> None:
        self.session_factory = session_factory
        self.raw_statement_row_factory = raw_statement_row_factory

    def persist_reports_in_dataset(
        self,
        dataset: FinancialDataset,
    ) -> None:
        """
            Persist each report in the dataset independently.

            Transaction boundary:
            - one transaction per report

            Allows partial persistence when one report fails
        """
        if dataset is None:
            raise ValueError("dataset must not be None")


        for report in [
            dataset.income_statement,
            dataset.balance_sheet,
            dataset.cash_flow,
        ]:
            if report is None:
                continue

            self.persist_report(report=report)


    def persist_report(
        self,
        report: FinancialReport,
    ) -> None:
        """
        Persist one report in a single transaction.
        """
        self._validate_report(report)

        session = self.session_factory()
        try:
            self._persist_raw_report(session=session, report=report)
            self._persist_mapped_report(session=session, report=report)
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

   
    def persist_reports(
        self,
        reports: Iterable[FinancialReport]
    ) -> None:
        """
        Persist multiple reports in a single transaction.
        """
        session = self.session_factory()
        try:
            for report in reports:
                self._validate_report(report)
                self._persist_raw_report(session=session, report=report)
                self._persist_mapped_report(session=session, report=report)
            
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()


    def persist_metrics_in_dataset(
        self,
        dataset: FinancialDataset,
    ) -> None:
        """
        Persist dataset-level computed metrics for one ticker.

        Transaction boundary:
        - one transaction for all metric rows of the ticker
        """
        if dataset is None:
            raise ValueError("dataset must not be None")

        if not dataset.ticker or not dataset.ticker.strip():
            raise ValueError("dataset.ticker must be a non-empty string")

        records = self._flatten_metrics(dataset)

        session = self.session_factory()
        try:
            replace_metric_values_for_ticker(
                session=session,
                ticker=dataset.ticker,
                records=records,
            )
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def _persist_raw_report(
        self,
        session,
        report: FinancialReport,
    ) -> None:
        """
        Persist raw/enriched report rows into raw_statement_facts.

        Preference:
        - use enriched data if available
        - otherwise fall back to raw
        """
        source_df = report.get_enriched_dataframe()
        if source_df is None:
            source_df = report.get_raw_dataframe()

        if source_df is None:
            return

        raw_rows = self.raw_statement_row_factory.from_statement_dataframe(
            df=source_df,
            statement_type=report.report_type,
            ticker=report.ticker,
        )

        records = [
            record
            for row in raw_rows
            for record in row.to_period_records()
        ]

        replace_raw_statement_facts_for_statement(
            session=session,
            ticker=report.ticker,
            statement_type=report.report_type,
            records=records,
        )

    
    def _persist_mapped_report(
        self,
        session,
        report: FinancialReport,
    ) -> None:
        
        """
        Persist mapped concept values and mapping tag origin for one report.
        """
        mapped_df = report.get_mapped_dataframe()
        if mapped_df is None:
            return

        replace_mapped_concept_values_for_statement(
            session=session,
            ticker=report.ticker,
            statement_type=report.report_type,
            mapped_df=mapped_df,
        )

        if report.selection_metadata:
            replace_mapped_concept_selections_for_statement(
                session=session,
                ticker=report.ticker,
                statement_type=report.report_type,
                mapping_metadata=report.selection_metadata,
            )

    
    def _flatten_metrics(self, dataset: FinancialDataset) -> list[dict]:
        """
        Flatten dataset.metrics into one-record-per-metric-per-period.

        Expected metrics shape:
        dataset.metrics.<category> = pd.DataFrame
        where:
        - index = metric names
        - columns = period labels
        """

        if dataset is None:
            raise ValueError("dataset must not be None")

        if dataset.metrics is None:
            return []

        metrics_obj = dataset.metrics
        records: list[dict] = []

        category_map = {
            "profitability": getattr(metrics_obj, "profitability", None),
            "growth": getattr(metrics_obj, "growth", None),
            "cash_flow": getattr(metrics_obj, "cash_flow", None),
            "balance_sheet": getattr(metrics_obj, "balance_sheet", None),
        }

        for category, df in category_map.items():
            if df is None or df.empty:
                continue

            for metric_name, row in df.iterrows():
                for period_label, value in row.items():
                    records.append({
                        "ticker": dataset.ticker,
                        "category": category,
                        "metric_name": str(metric_name),
                        "period_label": str(period_label),
                        "value": None if pd.isna(value) else value,  
                    })
        return records
    

    def _validate_report(self, report: FinancialReport) -> None:
        if report is None:
            raise ValueError("report must not be None")

        if not report.ticker or not report.ticker.strip():
            raise ValueError("report.ticker must be a non-empty string")

        if not report.report_type or not report.report_type.strip():
            raise ValueError("report.report_type must be a non-empty string")