# Metrics Layer Documentation

## Overview

The metrics layer computes derived financial metrics from normalized financial statements.

It sits on top of:

- EdgarClient (data retrieval)
- StatementMapper (concept normalization)

It does NOT:
- fetch SEC data
- handle XBRL tags
- perform mapping

Its only responsibility is:

Compute financial metrics from normalized concept data.

---

## Data Flow

SEC / edgartools
→ EdgarClient
→ raw statement data
→ StatementMapper
→ normalized statement data
→ Metrics layer
→ computed metrics

---

## Inputs

The metrics layer expects mapped historical statements.

Each statement should have:

- index = normalized concept names (e.g. revenue, net_income)
- columns = fiscal periods (e.g. FY 2025, FY 2024, FY 2023)

Example (conceptual):

income_statement:
  revenue
  gross_profit
  operating_income
  net_income

balance_sheet:
  total_assets
  shareholder_equity

cash_flow:
  operating_cash_flow
  capital_expenditures

---

## Metrics Covered

Profitability:
- gross_margin
- operating_margin
- net_margin

Growth:
- revenue_growth_yoy
- net_income_growth_yoy
- diluted_eps_growth_yoy

Cash Flow:
- free_cash_flow
- free_cash_flow_margin

Balance / Returns:
- roa_ending
- roe_ending
- roa_avg_assets
- roe_avg_equity

---

## Metric Definitions

### gross_margin

gross_margin = gross_profit / revenue

Meaning:
Percentage of revenue remaining after cost of revenue.

---

### operating_margin

operating_margin = operating_income / revenue

Meaning:
Profitability after operating expenses.

---

### net_margin

net_margin = net_income / revenue

Meaning:
Final profitability after all expenses.

---

### revenue_growth_yoy

revenue_growth_yoy = (revenue_t / revenue_t-1) - 1

Meaning:
Year-over-year growth in revenue.

---

### net_income_growth_yoy

net_income_growth_yoy = (net_income_t / net_income_t-1) - 1

Meaning:
Year-over-year growth in net income.

---

### diluted_eps_growth_yoy

diluted_eps_growth_yoy = (diluted_eps_t / diluted_eps_t-1) - 1

Meaning:
Year-over-year growth in diluted earnings per share.

---

### free_cash_flow

free_cash_flow = operating_cash_flow - capital_expenditures

Meaning:
Cash generated after capital investment.

Note:
Assumes capital_expenditures is a positive outflow number.

---

### free_cash_flow_margin

free_cash_flow_margin = free_cash_flow / revenue

Meaning:
Free cash flow relative to revenue.

---

### roa_ending

roa_ending = net_income / total_assets

Meaning:
Return on assets using end-of-period assets.

---

### roe_ending

roe_ending = net_income / shareholder_equity

Meaning:
Return on equity using end-of-period equity.

---

### roa_avg_assets

average_total_assets_t = (total_assets_t + total_assets_t-1) / 2

roa_avg_assets = net_income / average_total_assets_t

Meaning:
Return on assets using average assets across two years.

---

### roe_avg_equity

average_shareholder_equity_t = (shareholder_equity_t + shareholder_equity_t-1) / 2

roe_avg_equity = net_income / average_shareholder_equity_t

Meaning:
Return on equity using average equity across two years.

---

## Missing Data Behavior

If required inputs are missing:

- the metric returns missing values (NaN / None)
- the pipeline should continue

Examples:

- missing capital_expenditures → free_cash_flow unavailable
- missing shareholder_equity → ROE unavailable

---

## Methodology Notes

Metric values may differ from external sources.

Common reasons:

- ending vs average balance sheet values
- annual vs trailing twelve months (TTM)
- different numerator definitions (net income vs operating income)
- rounding differences

The formulas in this document define the system’s current behavior.

---

## Example Usage

Step 1: Fetch data

income_raw = client.fetch_income_statement(period_mode="history", years=3, annual=True)
balance_raw = client.fetch_balance_sheet(period_mode="history", years=3, annual=True)
cash_raw = client.fetch_cash_flow(period_mode="history", years=3, annual=True)

Step 2: Map to concepts

income_df = mapper.map_historical_statement(income_raw, "income_statement")
balance_df = mapper.map_historical_statement(balance_raw, "balance_sheet")
cash_df = mapper.map_historical_statement(cash_raw, "cash_flow")

Step 3: Build dataset

dataset = FinancialDataset(
    income_statement=income_df,
    balance_sheet=balance_df,
    cash_flow=cash_df
)

Step 4: Compute metrics

engine = MetricsEngine(dataset)

profitability = engine.calculate_profitability_metrics()
growth = engine.calculate_growth_metrics()
cash_flow_metrics = engine.calculate_cash_flow_metrics()
balance_metrics = engine.calculate_balance_sheet_metrics()

---

## Output Structure

All metric outputs follow the same structure:

- index = metric names
- columns = fiscal periods

Example:

gross_margin
operating_margin
net_margin

columns:
FY 2025
FY 2024
FY 2023

---

## Current Limitations

- only annual metrics are supported
- no trailing twelve months (TTM)
- no valuation metrics
- no sector-specific adjustments
- ROA / ROE may differ from external sources due to methodology