### How to use
1. Build the Dagster image
2. In the root directory, run the following code:
`docker exec <dagit-container-id> pytest -v pipelines/core/extract/tests/<test_file>.py`
3. Ensure all tests pass after changes

### Files
- `test_assets.py` tests the asset functions in `assets/chain/chain.py`
- `test_utils.py` tests the utility functions in `assets/chain/utils.py`
- `conftest.py` are pytest fixtures containing mock inputs and entities
- `utils.py` are utility functions used by tests

### Coverage
#### `test_assets.py`
- `blocks`
- `transactions`

#### `test_utils.py`
- `try_request`
- `get_entity`
- `get_entities`
- `extract_nested_columns`
- `filter_transactions`