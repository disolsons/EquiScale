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



    ## DEBUGGING - REMOVE AFTER. 

    def inspect_fact_concepts(
        self,
        ticker: str | None = None,
        concept_names: list[str] | None = None,
    ):
        """
        Inspect specific concept tags directly from EntityFacts.

        This is useful when a line item does not appear in the rendered statement
        output, but may still exist in the underlying SEC facts.
        """
        ticker = ticker or self.ticker
        if not ticker:
            raise ValueError("Ticker is required")

        if not concept_names:
            raise ValueError("concept_names must be provided")

        company = Company(ticker)
        facts = company.get_facts()

        if facts is None:
            return {}

        concept_map = getattr(facts, "facts", None)
        if concept_map is None:
            raise ValueError("EntityFacts object does not expose a .facts mapping in this environment")

        results = {}

        for concept_name in concept_names:
            concept_result = concept_map.get(concept_name)
            results[concept_name] = concept_result

        return results
    
    def inspect_capex_facts(self, ticker: str | None = None):
        """
        Inspect common capital expenditure concept tags directly from EntityFacts.
        """
        capex_candidates = [
            "PaymentsToAcquirePropertyPlantAndEquipment",
            "PurchasesOfPropertyAndEquipment",
            "PaymentsToAcquirePropertyAndEquipment",
            "PaymentsToAcquireProductiveAssets",
            "PropertyPlantAndEquipmentAdditions",
            "CapitalExpendituresIncurredButNotYetPaid",
        ]
        return self.inspect_fact_concepts(ticker=ticker, concept_names=capex_candidates)
    
    def search_capex_rows(
        self,
        ticker: str | None = None,
        period_mode: str = "history",
        years: int = 5,
        annual: bool = True,
    ):
        """
        Search the cash flow statement for likely capital expenditure rows.
        """
        keywords = ["property", "plant", "equipment", "purchase", "acquire", "capital"]
        return self.search_statement_rows(
            statement_type=EdgarConstants.STATEMENT_TYPE_CASH_FLOW,
            ticker=ticker,
            period_mode=period_mode,
            years=years,
            annual=annual,
            keywords=keywords,
        )
    
    def search_statement_rows(
        self,
        statement_type: str,
        ticker: str | None = None,
        period_mode: str = "history",
        years: int = 5,
        annual: bool = True,
        keywords: list[str] | None = None,
    ):
        """
        Search a fetched statement DataFrame by concept/index and label text.

        Useful for checking whether a concept-like row exists under a different tag or label.
        """
        ticker = ticker or self.ticker
        if not ticker:
            raise ValueError("Ticker is required")

        if not keywords:
            raise ValueError("keywords must be provided")

        df = self._fetch_statement(
            statement_type=statement_type,
            ticker=ticker,
            period_mode=period_mode,
            years=years,
            annual=annual,
        )

        if df is None or df.empty:
            return None

        pattern = "|".join(k.lower() for k in keywords)

        index_series = df.index.to_series().astype(str).str.lower()
        label_series = (
            df["label"].astype(str).str.lower()
            if "label" in df.columns
            else index_series * 0 + ""
        )

        mask = index_series.str.contains(pattern, na=False) | label_series.str.contains(pattern, na=False)

        return df.loc[mask]