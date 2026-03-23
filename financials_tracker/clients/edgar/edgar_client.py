from edgar import Company, set_identity
from financials_tracker.clients.edgar.edgar_constants import EdgarConstants
class EdgarClient:
    
    set_identity("test yourname@example.com")
    def __init__(self, concept_helper, ticker=None):
        self.helper = concept_helper    
        self.ticker = ticker
    
    def fetch_income_statement(self, **kwargs):
        return self._fetch_statement(statement_type=EdgarConstants.STATEMENT_TYPE_INCOME, **kwargs)

    def fetch_balance_sheet(self, **kwargs):
        return self._fetch_statement(statement_type=EdgarConstants.STATEMENT_TYPE_BALANCE_SHEET, **kwargs)

    def fetch_cash_flow(self, **kwargs):
        return self._fetch_statement(statement_type=EdgarConstants.STATEMENT_TYPE_CASH_FLOW, **kwargs)    
    
    def _fetch_statement(
        self,
        statement_type,
        ticker=None,
        period_mode="latest",
        years=5,
        annual=True,
    ):
        """
        Fetch an income statement.

        latest:
            Return the latest available rendered income statement from the most recent
            10-K (annual=True) or 10-Q (annual=False).

        history:
            Return a multi-period historical income statement suitable for analysis.
            Uses a historical retrieval path rather than slicing the latest statement.
        """
        ticker = ticker or self.ticker

        try:
            if period_mode == "latest":
                company = Company(ticker)
                financials = (
                    company.get_financials() if annual
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

            elif period_mode == "history":
                return self._fetch_historical_statement(
                    ticker=ticker,
                    statement_type=statement_type,
                    years=years,
                    annual=annual
                )

            else:
                raise ValueError("period_mode must be 'latest' or 'history'")

        except Exception as e:
            print(f"SEC Fetch Error for {ticker}: {e}")
            raise
        
    def _fetch_historical_statement(
        self,
        ticker,
        statement_type,
        years=5,
        annual=True
    ):
        """
        Fetch historical income statement using EntityFacts.
        Returns a multi-period DataFrame.
        """
        try:
            company = Company(ticker)
            facts = company.get_facts()

            if facts is None:
                return None

            df = self._get_statement_from_facts(
                facts=facts,
                statement_type=statement_type,
                years=years,
                annual=annual
            )

            if df is None or df.empty:
                return None

            return df

        except Exception as e:
            print(f"Historical SEC Fetch Error for {ticker}: {e}")
            raise

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