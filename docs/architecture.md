
# Architecture

## High-Level Design

The system is divided into three main layers:

### 1. Retrieval Layer
Implemented by `EdgarClient`.

Purpose:
- fetch latest financial statements
- fetch historical financial statements

### 2. Mapping Layer
Implemented by:
- `ConceptMapHelper`
- `StatementMapper`

Purpose:
- normalize raw SEC/XBRL concepts into internal concept names

### 3. Analytics Layer
Implemented by: 
- `MetricsCalculator`
- `MetricsEngine`

Purpose:
- compute derived metrics such as margins, growth, and cash flow ratios

---

## Current Core Components

### 1. `EdgarClient`
Responsible for retrieving:
- latest statements
- historical statements

Supported statements:
- income statement
- balance sheet
- cash flow

### 2. `ConceptMapHelper`
Responsible for:
- loading the concept map YAML
- forward lookup: concept -> tags
- reverse lookup: tag -> concept + statement type

### 3. `StatementMapper`
Responsible for:
- mapping raw Edgar statement output into normalized concept-based DataFrames



## Data Flow

### Latest Statement Flow

- TODO: Create event-flow for this part of the documentation


---
- [Back to Index](./README.md)