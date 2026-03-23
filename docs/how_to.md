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
# Run the project: 
```
poetry run python -m financials_tracker.cli.run_pipeline --tickers TSLA

Multiple tickers:

poetry run python -m financials_tracker.cli.run_pipeline --tickers TSLA AAPL NVDA

Custom output dir:

poetry run python -m financials_tracker.cli.run_pipeline --tickers TSLA AAPL --output-dir outputs/dev_run```
```

# Run integration tests:
```
poetry run pytest -m integration -q
```