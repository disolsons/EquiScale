Top recurring unmapped tags
```
session.query(AggregatedUnmappedTag).order_by(AggregatedUnmappedTag.ticker_count.desc()).limit(20).all()
```
All unmapped cash flow tags for TSLA
```
session.query(UnmappedTag).filter_by(ticker="TSLA", statement_type="cash_flow").all()
```
Validation summaries with poor coverage
```
session.query(ValidationSummary).filter(ValidationSummary.coverage_ratio < 1.0).all()
```