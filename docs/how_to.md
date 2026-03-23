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
python -m financials_tracker.cli.run_pipeline --tickers TSLA

Multiple tickers:

python -m financials_tracker.cli.run_pipeline --tickers TSLA AAPL NVDA

Custom output dir:

python -m financials_tracker.cli.run_pipeline --tickers TSLA AAPL --output-dir outputs/dev_run```
```

# Run integration tests:
```
poetry run pytest -m integration -q
```