# Financials Tracker Docs

This folder contains the internal documentation for the financial statement ingestion and mapping pipeline.


## Documentation Index
- [How To](./docs/how_to.md)
- [Architecture Overview](./docs/architecture.md)
- [EdgarClient](./docs/edgar_client.md)
- [StatementMapper](./docs/statement_mapper.md)
- [Metrics] To be Added.

## Purpose

The project currently focuses on:

1. Retrieving financial statements from SEC data through `edgartools` - Later on we will integrate with HKEX as well.
2. Mapping raw SEC/XBRL concepts into normalized internal concepts
3. Preparing data for the metrics layer
4. Extract valuable metrics for financial analysis.


