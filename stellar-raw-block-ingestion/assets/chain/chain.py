"""
ETL for <chain>
ref: <chain main website>
"""
import asyncio
import os

import pandas as pd
import requests

from dagster import (
    Field,
    AssetIn,
    asset,
    get_dagster_logger
)

# Local imports
from pipelines.shared.factories.numeric_block_factory import NumericBlockFactory
from pipelines.core.extract.assets.chain.utils import (
    get_ledgers,
    get_transactions,
    get_operations,
    get_effects,
    get_trades
)

logger = get_dagster_logger()

# NOTE: Stellar has a block time of 5 seconds, that is 720 blocks per hour
block_partition_size = 720

job_name = os.path.dirname(__file__).split('/')[-1]
key_prefix = os.path.splitext(__file__)[0].split("/")[-1]

# NOTE: we want to protect our provider endpoints so please use an env var
provider_uri = os.environ.get('CHAIN_PROVIDER_URI')

config_schema = {
    "provider_uri": Field(str, default_value=provider_uri),
    # Stellar Horizon limits a client to 3600 requests per hour
    "concurrency": Field(int, default_value=1),
}

@asset(
    io_manager_key='fs_io_manager',
)
def get_current_ledger() -> int:
    """
    Get the current block number from the chain

    @return: current block number
    """
    # PLEASE KEEP THIS IF STATEMENT AS A CONSTANT
    if bool(os.getenv('IS_TEST')):
        return block_partition_size * 2

    response = requests.get("https://horizon.stellar.org/ledgers", params={
        "order": "desc",
        "limit": 1
    })

    current_ledger = response.json()['_embedded']["records"][0]["sequence"]

    # Coerce 
    return current_ledger


def ledgers(context, start_block: int, end_block: int, **kwargs) -> pd.DataFrame:
    """
    Get a range of ledgers (blocks) from the chain by their sequences.
    The name blocks and ledgers are interchangeable.

    @param context: dagster context
    @param start_ledger: start ledger sequence
    @param end_ledger: end ledger sequence
    @return: dataframe of ledgers
    """

    # Conform to the naming convention of the rest of the asset codes as well as the naming convention of the chain
    start_ledger = start_block
    end_ledger = end_block

    logger.info(f"Getting ledgers {start_ledger} to {end_ledger}")

    # Get config values
    provider_uri = context.op_config["provider_uri"]
    concurrency = context.op_config["concurrency"]
    sem = asyncio.Semaphore(value=concurrency)

    # Get blocks
    ledger_sequences = list(range(start_ledger, end_ledger + 1))
    ledgers = asyncio.run(get_ledgers(provider_uri, sem, ledger_sequences))

    return ledgers


factory = NumericBlockFactory(
    job_name                = job_name,
    current_block_fn        = get_current_ledger,
    # NOTE: The first ledger is not recorded by Stellar Horizon nodes
    start_block             = 2,
    end_block               = get_current_ledger() - block_partition_size, 
    block_partition_size    = block_partition_size,
    config_schema           = config_schema,
    asset_configs           = {
        'group_name': key_prefix,
        'key_prefix': [key_prefix],
        'defs': [
            {
                'name': 'ledgers',
                'fn': ledgers
            },
        ]
    }
)


@asset(
    key_prefix      = factory.asset_configs['key_prefix'],
    group_name      = factory.asset_configs['group_name'],
    partitions_def  = factory.get_partitions_def(),
    config_schema   = config_schema,
    ins = {
        'ledgers': AssetIn(
            key=['ledgers'],
        )
    }
)
def transactions(context, ledgers: pd.DataFrame, **kwargs) -> pd.DataFrame:
    """
    Get transactions from the chain

    @param context: dagster context
    @param ledgers: dataframe of ledgers for input to the get function
    @return: dataframe of transactions
    """

    # Get config values
    provider_uri = context.op_config["provider_uri"]
    concurrency = context.op_config["concurrency"]
    sem = asyncio.Semaphore(value=concurrency)

    start_ledger = ledgers['sequence'].iloc[0]
    end_ledger = ledgers['sequence'].iloc[-1]

    # logger.info(f'Test is on for transactions. Only getting transactions for the first 10 ledgers. To disable this, comment out the relevent lines in the asset function.')
    # ledgers = ledgers.head(10)

    ledgers = ledgers[(ledgers["successful_transaction_count"] != 0) & (ledgers["failed_transaction_count"] != 0)]
    sequences = ledgers['sequence'].tolist()
    del ledgers

    if not sequences:
        logger.info(f"No ledgers with transactions found in the range {start_ledger} to {end_ledger}")
        return pd.DataFrame()

    logger.info(f"Getting transactions of ledgers from {start_ledger} to {end_ledger}")
    transactions = asyncio.run(get_transactions(provider_uri, sem, sequences))

    if len(transactions) == 0:
        logger.info(f"No transactions found in the range {start_ledger} to {end_ledger}")
        return pd.DataFrame()

    return transactions


@asset(
    key_prefix      = factory.asset_configs['key_prefix'],
    group_name      = factory.asset_configs['group_name'],
    partitions_def  = factory.get_partitions_def(),
    config_schema   = config_schema,
    ins = {
        'ledgers': AssetIn(
            key=['ledgers'],
        )
    }
)
def operations(context, ledgers: pd.DataFrame, **kwargs) -> pd.DataFrame:
    """
    Get operations from the chain

    @param context: dagster context
    @param ledgers: dataframe of ledgers for input to the get function
    @return: dataframe of operations
    """

    # Get config values
    provider_uri = context.op_config["provider_uri"]
    concurrency = context.op_config["concurrency"]
    sem = asyncio.Semaphore(value=concurrency)

    start_ledger = ledgers['sequence'].iloc[0]
    end_ledger = ledgers['sequence'].iloc[-1]

    # logger.info(f'Test is on for operations. Only getting operations for the first 10 ledgers. To disable this, comment out the relevent lines in the asset function.')
    # ledgers = ledgers.head(10)

    ledgers = ledgers[(ledgers["operation_count"] != 0) & (ledgers["tx_set_operation_count"] != 0)]
    sequences = ledgers['sequence'].tolist()
    del ledgers

    if not sequences:
        logger.info(f"No ledgers with operations found in the range {start_ledger} to {end_ledger}")
        return pd.DataFrame()

    logger.info(f"Getting operations of ledgers from {sequences[0]} to {sequences[-1]}")
    operations = asyncio.run(get_operations(provider_uri, sem, sequences))

    return operations


@asset(
    key_prefix      = factory.asset_configs['key_prefix'],
    group_name      = factory.asset_configs['group_name'],
    partitions_def  = factory.get_partitions_def(),
    config_schema   = config_schema,
    ins = {
        'ledgers': AssetIn(
            key=['ledgers'],
        )
    }
)
def effects(context, ledgers: pd.DataFrame, **kwargs) -> pd.DataFrame:
    """
    Get effects from the chain

    @param context: dagster context
    @param ledgers: dataframe of ledgers for input to the get function
    @return: dataframe of effects
    """

    # Get config values
    provider_uri = context.op_config["provider_uri"]
    concurrency = context.op_config["concurrency"]
    sem = asyncio.Semaphore(value=concurrency)

    start_ledger = ledgers['sequence'].iloc[0]
    end_ledger = ledgers['sequence'].iloc[-1]

    # logger.info(f'Test is on for effects. Only getting effects for the first 10 ledgers. To disable this, comment out the relevent lines in the asset function.')
    # ledgers = ledgers.head(10)

    # Effects are derived from operations
    ledgers = ledgers[(ledgers["operation_count"] != 0) & (ledgers["tx_set_operation_count"] != 0)]
    sequences = ledgers['sequence'].tolist()
    del ledgers

    if not sequences:
        logger.info(f"No ledgers with operations found in the range {start_ledger} to {end_ledger}")
        return pd.DataFrame()

    logger.info(f"Getting effects of ledgers from {sequences[0]} to {sequences[-1]}")
    effects = asyncio.run(get_effects(provider_uri, sem, sequences))

    return effects


@asset(
    key_prefix      = factory.asset_configs['key_prefix'],
    group_name      = factory.asset_configs['group_name'],
    partitions_def  = factory.get_partitions_def(),
    config_schema   = config_schema,
    ins = {
        'ledgers': AssetIn(
            key=['ledgers'],
        )
    }
)
def trades(context, ledgers: pd.DataFrame, **kwargs):
    """
    Get trades from the chain

    @param context: dagster context
    @param trades: dataframe of trades for input to the get function
    @return: dataframe of trades
    """

    # Get config values
    provider_uri = context.op_config["provider_uri"]
    concurrency = context.op_config["concurrency"]
    sem = asyncio.Semaphore(value=concurrency)
    
    # logger.info(f'Test is on for trades. Only getting trades for the first 36 ledgers. To disable this, comment out the relevent lines in the asset function.')
    # ledgers = ledgers.head(36)

    # We are taking advantage of ledgers' paging tokens being similar to trades' paging tokens to get trades
    # Ledger paging tokens only mark the lower boundary of the range, so we need to calculate the upper boundary ourselves
    # The difference between the lower and upper boundary is 2^32 in history, but there is no guarantee that it will stay the same in the future
    # Therefore, we need to check if it still holds true
    paging_tokens = ledgers['paging_token'].astype(int)
    assert (paging_tokens.diff()[1:] == 2**32).all()

    del ledgers

    
    # The API parameter has exclusive lower bound and inclusive upper bound
    # So we need to subtract 1 to achieve inclusive lower and 
    # This would not lead to trades belonging to the previous ledger being included, since the paging token differs by at least 4
    # Find the lower bound of the first ledger
    min_paging_token = paging_tokens.min() - 1
    # Find the upper bound of the last ledger
    max_paging_token = paging_tokens.max() + 2**32 - 1

    logger.info(f"Getting trades from paging token {min_paging_token} to {max_paging_token}")

    trades = asyncio.run(get_trades(provider_uri, sem, min_paging_token, max_paging_token))
    return trades



factory.set_passthrough_assets([
    transactions,
    operations,
    effects,
    trades,
    ])
factory_outputs = factory.construct()
