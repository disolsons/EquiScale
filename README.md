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



# Current work: 
    Validators:
    - Generate reports on maped/unmapped tags for each ticker used.
    - Generate reports on all unmapped tags, importance ranking + possible concept to map to. - Rules 1st, Heuristics 2nd, LLM suggestions 3rd.

# Future work:
    Concept completeness:
    - Currently only 1 (the first) tag is mapped to each concept, to derive metrics for.
    - Some metrics might require multiple tags to compute
    - First tag is not a good way to pick the best value, even when a single tag represents the complete value.

