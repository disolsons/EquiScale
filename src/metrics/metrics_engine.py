import pandas as pd
from src.metrics.metrics_calculator import MetricsCalculator
from src.metrics.model.financial_dataset import FinancialDataset
from src.metrics.metrics_registry_helper import MetricsRegistryHelper
from src.metrics.pre_processors.metric_input_preprocessor import MetricInputPreprocessor

class MetricsEngine:
    
    def __init__(self, dataset: FinancialDataset):
        self.dataset = dataset
        self.calc = MetricsCalculator()        
        self.metrics_registry_helper = MetricsRegistryHelper("config/metric_registry.yaml")
        self.input_preprocessor = MetricInputPreprocessor()
        self.cache = {}
    
    def calculate_profitability_metrics(self) -> pd.DataFrame:
        """
        Compute core profitability ratios based on the income statement.

        Includes:
        - gross_margin
        - operating_margin
        - net_margin

        These metrics measure how efficiently the company converts revenue
        into profit at different stages (gross, operating, net).

        Returns:
            DataFrame where:
            - index = metric names
            - columns = fiscal periods (e.g. FY 2025, FY 2024)
        """    

        return self._calculate_metrics([
            "gross_margin",
            "operating_margin",
            "net_margin",
        ])

    def calculate_growth_metrics(self) -> pd.DataFrame:
        """
        Compute year-over-year growth rates for key income statement metrics.

        Includes:
        - revenue_growth_yoy
        - net_income_growth_yoy
        - diluted_eps_growth_yoy

        These metrics capture how the company’s top line, bottom line,
        and per-share earnings evolve over time.

        Returns:
            DataFrame where:
            - index = metric names
            - columns = fiscal periods (chronological order)
            - earliest period is typically NaN (no prior comparison)
        """
        return self._calculate_metrics([
            "revenue_growth_yoy",
            "net_income_growth_yoy",
            "diluted_eps_growth_yoy",
        ])

    def calculate_cash_flow_metrics(self) -> pd.DataFrame:
        """
        Compute cash flow–based metrics derived from the cash flow statement.

        Includes:
        - free_cash_flow
        - free_cash_flow_margin

        These metrics assess the company’s ability to generate cash after
        capital investments and relate that cash generation to revenue.

        Returns:
            DataFrame where:
            - index = metric names
            - columns = fiscal periods
        """
        return self._calculate_metrics([
            "free_cash_flow",
            "free_cash_flow_margin",
        ])

    def calculate_balance_sheet_metrics(self) -> pd.DataFrame:
        """
        Compute return metrics based on balance sheet and income data.

        Includes:
        - roa_ending
        - roe_ending
        - roa_avg_assets
        - roe_avg_equity

        These metrics measure how efficiently the company uses its assets
        and equity to generate profits.

        Note:
        - 'ending' metrics use end-of-period balance sheet values
        - 'avg' metrics use the average of current and prior period balances

        Returns:
            DataFrame where:
            - index = metric names
            - columns = fiscal periods
            - earliest period for average-based metrics is typically NaN
        """
        return self._calculate_metrics([
            "roa_ending",
            "roe_ending",
            "roa_avg_assets",
            "roe_avg_equity",
        ])
    
    def _calculate_metrics(self, metric_names: list[str]) -> pd.DataFrame:
        results = {}
        reference_index = self._get_reference_period_index()
        
        for metric_name in metric_names:
            metric_result = self._calculate_metric(metric_name)

            # If a metric cannot be calculated, store an empty Series
            # so pandas can still build a DataFrame consistently.
            if metric_result is None:
                results[metric_name] = pd.Series(index=reference_index, dtype="float64")
            else:
                results[metric_name] = metric_result.reindex(reference_index)
                
        return pd.DataFrame(results).T
    
    def _calculate_metric(
        self,
        metric_name: str,
        visited: set[str] | None = None,
    ) -> pd.Series | None:
        if visited is None:
            visited = set()

        if metric_name in visited:
            raise ValueError(f"Circular dependency detected: {metric_name}")

        visited.add(metric_name)

        metric_definition = self.metrics_registry_helper.get_metric_definition(metric_name)
        if metric_definition is None:
            raise ValueError(f"Unknown metric: {metric_name}")

        inputs = [self._get_values(name, visited) for name in metric_definition["inputs"]]
        operation = metric_definition["operation"]

        if operation == "divide": #TODO: extract operations to constants.
            return self.calc.divide(inputs[0], inputs[1])

        if operation == "subtract":
            return self.calc.subtract(inputs[0], inputs[1])

        if operation == "yoy_growth":
            return self.calc.yoy_growth(inputs[0])

        raise ValueError(f"Unsupported operation: {operation}")

    def _get_values(
        self,
        name: str,
        visited: set[str] | None = None,
    ) -> pd.Series | None:        
        """
            Retrieves the values for a specific metric input, which could be a direct concept from the dataset 
            or a derived concept that requires additional calculation logic.
        """
        if name in self.cache:
            return self.cache[name]
    

        values = self._get_concept_values(name)
        if values is not None:
            values = self._prepare_metric_input(name, values)
            self.cache[name] = values
            return values

        values = self._get_derived_values(name)
        if values is not None:
            self.cache[name] = values
            return values

        #  cascading metrics.
        if self.metrics_registry_helper.has_metric(name):
            values = self._calculate_metric(name, visited=visited)
            self.cache[name] = values
            return values
        
        self.cache[name] = None
        return None

    def _get_concept_values(self, concept: str) -> pd.Series | None:
        """
        Retrieves the values for a specific financial concept directly from the dataset.
        """
        for statement_df in [
            self.dataset.income_statement,
            self.dataset.balance_sheet,
            self.dataset.cash_flow,
        ]:
            if statement_df is not None and concept in statement_df.index:
                values = pd.to_numeric(statement_df.loc[concept], errors="coerce")
                return values

        return None
    
    def _get_derived_values(self, name: str) -> pd.Series | None:
        """
          Retrieves the values for a specific financial concept derived from other concepts present in the dataset.
            E.g. avg_total_assets is derieved from total_assets, but requires additional logic to compute.
        """
        if name == "avg_total_assets":
            base = self._get_concept_values("total_assets")
            result = self.calc.average_with_previous_period(base)
            return result

        if name == "avg_shareholder_equity":
            base = self._get_concept_values("shareholder_equity")
            result = self.calc.average_with_previous_period(base)
            return result

        return None 

    def _get_reference_period_index(self) -> pd.Index:
        """
            Use the first non-empty mapped statement as the reference set of periods
            for metric output alignment.
        """
        for statement_df in [
            self.dataset.income_statement,
            self.dataset.balance_sheet,
            self.dataset.cash_flow,
        ]:
            if statement_df is not None and not statement_df.empty:
                return statement_df.columns

        return pd.Index([])

    def _prepare_metric_input(
        self,
        concept_name: str,
        values: pd.Series | None,
    ) -> pd.Series | None:
        """
        Apply metric-input preprocessing when a concept requires basis normalization
        before metric calculation.

        Currently used for per-share concepts such as basic_eps and diluted_eps.
        """
        if values is None or values.empty:
            return values
        
        #FIXME: Hardcoded strings.
        if concept_name == "diluted_eps":
            return self.input_preprocessor.prepare_metric_input(
                concept_name=concept_name,
                series=values,
                net_income_series=self._get_concept_values("net_income"),
                shares_series=self._get_concept_values("diluted_shares"),
            )

        if concept_name == "basic_eps":
            return self.input_preprocessor.prepare_metric_input(
                concept_name=concept_name,
                series=values,
                net_income_series=self._get_concept_values("net_income"),
                shares_series=self._get_concept_values("basic_shares"),  # will be None if not mapped yet
            )

        return values       
