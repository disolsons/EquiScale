# EdgarClient

## Purpose

`EdgarClient` is responsible for retrieving financial statement data from SEC filings through `edgartools`.

It does not perform normalization or metric computation.

---

## Responsibilities

- Fetch latest statements
- Fetch historical statements
- Support all main statement types:
  - income statement
  - balance sheet
  - cash flow

---

## Public Methods

### `fetch_income_statement(...)`
Fetches the income statement.

### `fetch_balance_sheet(...)`
Fetches the balance sheet.

### `fetch_cash_flow(...)`
Fetches the cash flow statement.

These methods support the same parameter pattern.

---

## Main Parameters

### `period_mode`
Controls which retrieval path is used.

Supported values:
- `"latest"`
- `"history"`

#### `"latest"`
Returns the latest available rendered statement from the most recent filing.

#### `"history"`
Returns a historical multi-period statement.

---

### `annual`
Controls whether annual or quarterly data is requested.

Typical usage:
- `annual=True` for yearly statements
- `annual=False` for quarterly statements

---

### `years`
Used for historical retrieval to control how many periods to request.

Example:
- `years=3`

---

## Return Types
# Latest statement

Returns a raw statement DataFrame that is useful for inspection and mapping.

# Historical statement

Returns a raw historical DataFrame with:

concept-like row index
metadata columns
fiscal-year columns

## Example usage
```
helper = ConceptMapHelper(CONFIG_PATH)
client = EdgarClient(concept_helper=helper, ticker="AAPL")

income_statement = client.fetch_income_statement(
    period_mode="history",
    years=3,
    annual=True
)
balance_sheet = client.fetch_balance_sheet(
        period_mode="latest",
        years=3,
        annual=True
)
cash_flow = client.fetch_cash_flow(
        period_mode="latest",
        annual=True
)
```
- [Back to Index](../README.md)