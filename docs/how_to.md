# Requirements
- Python
- Poetry

# Initialize the project: 
```
poetry init
```
# Install dependencies: 
```
poetry install
```
# Add new depenencies (during development): 
```
poetry add $(package-name)
```

# Initialize SQLite DB:
```
poetry run python -m financials_tracker.cli.init_db
```
The db will be created under `data`

# Run the project: 
```
poetry run python -m financials_tracker.cli.run_pipeline --tickers TSLA

Multiple tickers:

poetry run python -m financials_tracker.cli.run_pipeline --tickers TSLA AAPL NVDA

Custom output dir:

poetry run python -m financials_tracker.cli.run_pipeline --tickers TSLA AAPL --output-dir outputs/dev_run```

Run unmapped tags aggregator: 

poetry run python -m financials_tracker.cli.aggregate_unmapped_tags --input-dir outputs

Rank the unmap tags by priority score:

poetry run python -m financials_tracker.cli.rank_unmapped_tags

Suggest concepts for unmapped tags: 

poetry run python -m financials_tracker.cli.generate_tag_suggestions
```

# Run integration tests:
```
poetry run pytest -m integration -q
```