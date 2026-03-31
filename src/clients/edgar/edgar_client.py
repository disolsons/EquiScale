from edgar import Company, set_identity

from typing import Any
from src.clients.edgar.edgar_constants import EdgarConstants


class EdgarClient:
    set_identity("test yourname@example.com")

    def __init__(self, concept_helper):
        self.helper = concept_helper

    def fetch_income_statement(self, **kwargs):
        return self._fetch_statement(
            statement_type=EdgarConstants.STATEMENT_TYPE_INCOME,
            **kwargs,
        )

    def fetch_balance_sheet(self, **kwargs):
        return self._fetch_statement(
            statement_type=EdgarConstants.STATEMENT_TYPE_BALANCE_SHEET,
            **kwargs,
        )

    def fetch_cash_flow(self, **kwargs):
        return self._fetch_statement(
            statement_type=EdgarConstants.STATEMENT_TYPE_CASH_FLOW,
            **kwargs,
        )

    def _fetch_statement(
        self,
        statement_type,
        ticker=None,
        period_mode=EdgarConstants.MODE_HISTORY,
        years=5,
        annual=True,
    ):
        if not ticker:
            raise ValueError("Ticker is required")
        
        try:
            company = Company(ticker)

            if period_mode == EdgarConstants.MODE_LATEST:
                financials = (
                    company.get_financials()
                    if annual
                    else company.get_quarterly_financials()
                )

                if financials is None:
                    return None

                statement_obj = self._get_statement_from_financials(financials, statement_type)
                if statement_obj is None:
                    return None

                df = statement_obj.to_dataframe()
                if df is None or df.empty:
                    return None

                return df

            if period_mode == EdgarConstants.MODE_HISTORY:
                return self._fetch_historical_statement(
                    ticker=ticker,
                    statement_type=statement_type,
                    years=years,
                    annual=annual,
                )

            raise ValueError("period_mode must be 'latest' or 'history'")

        except Exception as e:
            print(f"SEC Fetch Error for {ticker}: {e}")
            raise

    def _fetch_historical_statement(
        self,
        ticker,
        statement_type,
        years=5,
        annual=True,
    ):
        if not ticker:
            raise ValueError("Ticker is required")
        
        try:
            company = Company(ticker)
            facts = company.get_facts()

            if facts is None:
                return None

            df = self._get_statement_from_facts(
                facts=facts,
                statement_type=statement_type,
                years=years,
                annual=annual,
            )

            if df is None or df.empty:
                return None

            return self._ensure_metadata_columns(df, source_layer=EdgarConstants.LAYER_STATEMENT_FRAME)

        except Exception as e:
            print(f"Historical SEC Fetch Error for {ticker}: {e}")
            raise

    def query_concept_facts(
        self,
        raw_tag,
        ticker=None,
        years=5,
        annual=True,
    ):
        """
        Query a single raw SEC concept directly from EntityFacts and return
        the raw query DataFrame.

        This is intentionally generic:
        - no concept-map logic
        - no statement-type assumptions
        - no merging logic
        """
        if not ticker:
            raise ValueError("Ticker is required")

        company = Company(ticker)
        facts = company.get_facts()
        if facts is None:
            return None

        concept_name = raw_tag if ":" in raw_tag else f"{EdgarConstants.US_TAG_PREFIX}:{raw_tag}"

        q = facts.query()
        q = q.by_concept(concept_name, exact=True)
        q = q.latest_periods(years, annual=annual)

        df = q.to_dataframe()
        if df is None or df.empty:
            return None

        return df

    def _ensure_metadata_columns(self, df, source_layer=EdgarConstants.LAYER_STATEMENT_FRAME):
        """
        Generates metadata columns if they dont exist in the source for structure consistency
        """
        out = df.copy()

        if EdgarConstants.COLUMN_PARENT_TAG not in out.columns:
            out[EdgarConstants.COLUMN_PARENT_TAG] = None

        if EdgarConstants.COLUMN_CALCULATION_WEIGHT not in out.columns:
            out[EdgarConstants.COLUMN_CALCULATION_WEIGHT] = None

        if EdgarConstants.COLUMN_SOURCE_LAYER not in out.columns:
            out[EdgarConstants.COLUMN_SOURCE_LAYER] = source_layer

        return out

    def _get_statement_from_financials(self, financials, statement_type):
        if statement_type == EdgarConstants.STATEMENT_TYPE_INCOME:
            return financials.income_statement()
        elif statement_type == EdgarConstants.STATEMENT_TYPE_BALANCE_SHEET:
            return financials.balance_sheet()
        elif statement_type == EdgarConstants.STATEMENT_TYPE_CASH_FLOW:
            return financials.cashflow_statement()
        else:
            raise ValueError(f"Unsupported statement_type: {statement_type}")

    def _get_statement_from_facts(self, facts, statement_type, years=5, annual=True):
    
        if statement_type == EdgarConstants.STATEMENT_TYPE_INCOME:
            return facts.income_statement(periods=years, annual=annual, as_dataframe=True)
        elif statement_type == EdgarConstants.STATEMENT_TYPE_BALANCE_SHEET:
            return facts.balance_sheet(periods=years, annual=annual, as_dataframe=True)
        elif statement_type == EdgarConstants.STATEMENT_TYPE_CASH_FLOW:
            return facts.cash_flow(periods=years, annual=annual, as_dataframe=True)
        else:
            raise ValueError(f"Unsupported statement_type: {statement_type}")
        