# hedera-raw-block-ingestion
- Language: Python
- Project Type: Data Engineering

This module is part of a larger project focused on ingesting raw blockchain data of Hedera into a database. It uses Dagster as the data orchestrator to manage the ETL process in a DAG format.

The outputs of this module are parquet and CSV files of the raw blockchain data, segmented by block numbers.

## Structure
- [assets/chain/](assets/chain/README.md) contains the logic for data extraction, focusing on all blockchain entities available from the Hedera API.
- [tests/](tests/README.md) contains the unit tests for the module.

All codes in these directories are written by me.

## Dependencies
- Python 3.7
- `pandas` for data manipulation
- `dagster` for data orchestration
- `pytest` for unit testing
- `aiohttp` for asynchronous HTTP requests
- Boilerplate code from client (not included in this repo)

## Sanitization Notice
This module is part of a larger project and it has been sanitized to exclude proprietary information and dependencies. It is not intended to be used as a standalone application.