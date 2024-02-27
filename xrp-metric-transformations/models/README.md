## Notes
The following columns in `network_usage_metrics` are null as there are no concepts of contract calls in XRP ledger:
- `total_contract_calls`
- `new_contracts_called`
- `returning_contracts_called`
- `unique_contract_callers`

The following columns in `network_usage_metrics` uses the 'escrow' concept in XRP ledger in place for 'contract':
- `active_contracts`: Created escrows that are yet to end or finish
- `new_contracts_deployed`: Escrows created in the provided timeframe

All columns below in `consensus` are null as there are no block rewards and new ledgers are validated through a voting process not recorded in the ledger:
- `total_validators` (A possible substitute is the unique node list, but it is not recorded on-chain as well)
- `total_block_producers`
- `total_stake`
- `stake_token`
- `estimated_nakamoto_coefficient`

## QA
Sources used:
1. https://xrpscan.com/metrics
2. https://xrpl-metrics.com/#/statistics

Available non-null metrics in `network_usage_metrics` (and current status):
- `total_transactions`: Model outputs miss up to 2% of transactions. Examining.
- `successful_transactions`: Model outputs miss up to 2% of transactions. Examining.
- `unsuccessful_transactions`: Model outputs miss up to 2% of transactions. Examining.
- `new_addresses`: Unable to examine before running the model for the whole history.

Unavailable non-null metrics in `network_usage_metrics`:
- `active_addresses`
- `returning_addresses`
- `transaction_fees`
- `active_contracts`
- `new_contracts_deployed`
