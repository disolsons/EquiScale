
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

## Data Flow

### Latest Statement Flow

- TODO: Create event-flow for this part of the documentation


---
- [Back to Index](../README.md)