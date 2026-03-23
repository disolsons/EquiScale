# Financials Tracker Docs

This folder contains the internal documentation for the financial statement ingestion and mapping pipeline.


## Documentation Index
- [How To](./how_to.md)
- [Architecture Overview](./architecture.md)
- [EdgarClient](./edgar_client.md)
- [StatementMapper](./statement_mapper.md)
- [Testing](./testing.md)

## Purpose

The project currently focuses on:

1. Retrieving financial statements from SEC data through `edgartools` - Later on we will integrate with HKEX as well.
2. Mapping raw SEC/XBRL concepts into normalized internal concepts
3. Preparing data for a future metrics layer


