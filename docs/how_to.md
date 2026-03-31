# Requirements
- Python
- Poetry
- SQLite 

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

Erase db data:
```
poetry run python -m src.cli.reset_db
```

# Run: 

Full edgar pipeline for a ticker: 
    - Retrieve edgar documents
    - enrich and map to standarized concepts
    - persist values
    - calculate metrics
```    
poetry run python -m src.cli.run_pipeline --ticker AAPL
```

# Run integration tests:
```
poetry run pytest -m integration -q
```

# Run all tests: 
```
poetry run pytest
```


- [Back to Index](../README.md)