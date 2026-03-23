# StatementMapper

## Purpose

`StatementMapper` converts raw statement DataFrames from `EdgarClient` into normalized concept-based DataFrames.

It does not fetch data itself.
It works on already retrieved DataFrames.

---

## Why a Mapper Is Needed

Raw Edgar/SEC statement output contains raw concept names such as:

- `NetIncomeLoss`
- `RevenueFromContractWithCustomerExcludingAssessedTax`

The application wants stable internal concepts such as:

- `net_income`
- `revenue`

The mapper bridges this gap.

---

## Two Mapping Paths

Because latest and historical statements come in different shapes, the mapper currently has two separate methods.

### 1. `map_historical_statement(df, statement_type)`
Used for historical statements.

Historical statement shape:
- raw SEC concept tags are usually in the row index
- period values appear in columns like `FY 2025`, `FY 2024`

### 2. `map_latest_statement(df, statement_type)`
Used for latest statements.

Latest statement shape:
- raw SEC concept tags are usually in a `concept` column
- additional metadata columns may be present

---

## Historical Mapping Logic

High level:

1. copy the input DataFrame
2. remove abstract rows if `is_abstract` exists
3. identify period columns
4. iterate through raw row index tags
5. reverse-map each tag using `ConceptMapHelper`
6. keep rows belonging to the requested statement type
7. build a normalized DataFrame keyed by internal concepts

---

## Latest Mapping Logic

High level:

1. copy the input DataFrame
2. read raw SEC tag from the `concept` column
3. normalize the raw tag format if needed
4. reverse-map the normalized tag
5. keep rows belonging to the requested statement type
6. keep first match for each normalized concept

---

## Raw Tag Normalization

Latest statements may contain tags like:

```
us-gaap_RevenueFromContractWithCustomerExcludingAssessedTax
```
while the YAML uses:
```
RevenueFromContractWithCustomerExcludingAssessedTax
```
The mapper normalizes these raw values before reverse lookup.

# is_abstract

Historical statement DataFrames may contain an is_abstract column.

This flag comes from EdgarTools and indicates whether a row is:

- structural/header-only (True)
- a real data row (False)

Example abstract rows:
```
OperatingExpensesAbstract
NonoperatingIncomeExpenseAbstract
```
These rows are usually filtered out before mapping.

