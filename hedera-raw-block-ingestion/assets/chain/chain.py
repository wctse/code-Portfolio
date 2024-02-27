"""
ETL for Hedera
ref: https://hedera.com/
"""

import asyncio
import os

import pandas as pd
import requests

from dagster import Field, AssetIn, asset, get_dagster_logger

# Local imports
from pipelines.core.extract.assets.chain.constants import (
    NAMES_FOR_MESSAGES,
    NAMES_FOR_TOKENS,
    NAMES_FOR_CONTRACTS,
    NAMES_FOR_CONTRACT_STATES,
    NAMES_FOR_CONTRACT_RESULTS,
    NAMES_FOR_CONTRACT_ACTIONS,
)
from pipelines.core.extract.assets.chain.utils import (
    filter_transactions,
    get_blocks,
    get_transactions,
    get_messages,
    get_tokens,
    get_contracts,
    get_contract_states,
    get_contract_results,
    get_contract_actions,
    get_state_proof_alphas,
)

from pipelines.shared.factories.numeric_block_factory import NumericBlockFactory

logger = get_dagster_logger()

job_name = os.path.dirname(__file__).split('/')[-1]
key_prefix = os.path.splitext(__file__)[0].split("/")[-1]

# NOTE: we want to protect our provider endpoints so please use an env var
provider_uri = os.environ.get('CHAIN_PROVIDER_URI')

# NOTE: partition size should roughly equal the number of blocks in 1 hour
# Hedera block time is roughly 1 second, so 3600 blocks per hour
block_partition_size = 3600

# Config schema for Dagster OpExecutionContext imported by the context argument of asset functions
config_schema = {
    # Node provider
    "provider_uri": Field(str, default_value=provider_uri),

    # The API can tolerate 100 calls per second
    "concurrency": Field(int, default_value=100),
}

@asset(
    io_manager_key='fs_io_manager',
)
def get_current_block() -> int:
    """
    Get the current block number from the chain

    @return: current block number
    """
    # PLEASE KEEP THIS IF STATEMENT AS A CONSTANT
    if bool(os.getenv('IS_TEST')):
        return block_partition_size * 2

    response = requests.get(f"{provider_uri}/api/v1/blocks/?order=desc&limit=1")
    current_block = response.json()["blocks"][0]["number"]

    return current_block


def blocks(context, start_block: int, end_block: int, **kwargs) -> pd.DataFrame:
    """
    Get a range of blocks from the chain

    @param context: dagster context
    @param start_block: start block number
    @param end_block: end block number
    @return: dataframe of blocks
    """
    logger.info(f'Getting blocks {start_block} to {end_block}')
    
    # Get config values
    provider_uri = context.op_config["provider_uri"]
    concurrency = context.op_config["concurrency"]
    sem = asyncio.Semaphore(value=concurrency)

    # Get blocks
    block_numbers = list(range(start_block, end_block + 1))

    # logger.info(f'Test is on for blocks. Only getting blocks from {start_block} to {start_block + 60}. To disable this, comment out the relevent lines in the asset function.')
    # block_numbers = list(range(start_block, start_block + 60))

    blocks = asyncio.run(get_blocks(provider_uri, sem, block_numbers))

    return blocks


factory = NumericBlockFactory(
    job_name                = job_name,
    current_block_fn        = get_current_block,
    # NOTE: dagster can get mad about too many paritions 
    # for testing it's okay to set this to a large number like 1 million so less partitions are created
    # NOTE: it's still essential to test that the first partition works though
    start_block             = 0,
    end_block               = get_current_block() - block_partition_size, 
    block_partition_size    = block_partition_size,
    config_schema           = config_schema,
    asset_configs           = {
        'group_name': key_prefix,
        'key_prefix': [key_prefix],
        'defs': [
            {
                'name': 'blocks',
                'fn': blocks
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
        'blocks': AssetIn(
            key=['blocks'],
        )
    }
)
def transactions(context, blocks: pd.DataFrame, **kwargs) -> pd.DataFrame:
    """
    Get transactions and transfers from a range of blocks

    @param context: dagster context
    @param blocks: dataframe of blocks
    @return: dataframe of transactions
    """
    provider_uri = context.op_config["provider_uri"]
    concurrency = context.op_config["concurrency"]
    sem = asyncio.Semaphore(value=concurrency)

    # logger.info(f'Test is on for transactions. Only getting transactions for the first 30 blocks. To disable this, comment out the relevent lines in the asset function.')
    # blocks = blocks.head(30)
    
    block_range = (blocks["number"].min(), blocks["number"].max())
    start_timestamps = blocks["timestamp.from"].tolist()
    end_timestamps = blocks["timestamp.to"].tolist()

    logger.info(f"Extracting transfers from transactions for block range {block_range} between {start_timestamps[0]} and {end_timestamps[-1]}")

    # Save memory
    del blocks

    # Get transactions
    transactions = asyncio.run(get_transactions(provider_uri, sem, start_timestamps, end_timestamps))

    # NOT IMPLEMENTED: Find the block that the transactions belong to
    # Reason: We want the the dataframe to be as-is from the API response, without any modifications

    # # Matches the range of block's timestamp.from and timestamp.to with the transaction's timestamp
    # transactions["block_number"] = transactions["consensus_timestamp"].map(
    #     lambda x: blocks.loc[
    #         (blocks["timestamp.from"].apply(timestamp_to_int) <= x) &
    #         (blocks["timestamp.to"].apply(timestamp_to_int) >= x)
    #     ].index[0]
    # )

    return transactions

@asset(
    key_prefix      = factory.asset_configs['key_prefix'],
    group_name      = factory.asset_configs['group_name'],
    partitions_def  = factory.get_partitions_def(),
    config_schema   = config_schema,
    ins = {
        'transactions': AssetIn(
            key=['transactions'],
        )
    }
)
def messages(context, transactions: pd.DataFrame, **kwargs) -> pd.DataFrame:
    """
    Get messages from a range of transactions that submit messages

    @param context: dagster context
    @param transactions: dataframe of transactions
    @return: dataframe of messages
    """

    provider_uri = context.op_config["provider_uri"]
    concurrency = context.op_config["concurrency"]
    sem = asyncio.Semaphore(value=concurrency)

    first_timestamp = transactions.iloc[0]["consensus_timestamp"]
    last_timestamp = transactions.iloc[-1]["consensus_timestamp"]

    logger.info(f"Extracting messages from transactions from timestamp {first_timestamp} to {last_timestamp}.")
    
    # logger.info('Test is on for messages. Only getting messages for the first 25 transactions. To disable this, comment out the relevent lines in the asset function.')
    # transactions = transactions.head(25)

    # Trim the transaction dataframe to only keep relevent information to minimize memory use
    # Keep only relevant rows and columns for message retrieval
    consensus_timestamps = filter_transactions(
        transactions,
        target_names=NAMES_FOR_MESSAGES,
        selected_columns=["consensus_timestamp"]
    )

    # Save memory
    del transactions

    if not consensus_timestamps:
        return pd.DataFrame()

    # Get the messages
    messages = asyncio.run(get_messages(provider_uri, sem, consensus_timestamps))

    return messages


@asset(
    key_prefix      = factory.asset_configs['key_prefix'],
    group_name      = factory.asset_configs['group_name'],
    partitions_def  = factory.get_partitions_def(),
    config_schema   = config_schema,
    ins = {
        'transactions': AssetIn(
            key=['transactions'],
        )
    }
)
def tokens(context, transactions: pd.DataFrame, **kwargs) -> pd.DataFrame:
    """
    Get token information from a range of transactions that affect a token's state
    
    @param context: dagster context
    @param transactions: dataframe of transactions
    @return: dataframe of tokens
    """

    provider_uri = context.op_config["provider_uri"]
    concurrency = context.op_config["concurrency"]
    sem = asyncio.Semaphore(value=concurrency)

    first_timestamp = transactions.iloc[0]["consensus_timestamp"]
    last_timestamp = transactions.iloc[-1]["consensus_timestamp"]

    logger.info(f"Extracting token information from transactions from timestamp {first_timestamp} to {last_timestamp}.")

    # logger.info('Test is on for tokens. Only getting tokens for the first 25 transactions. To disable this, comment out the relevent lines in the asset function.')
    # transactions = transactions.head(25)

    # Some token transaction names are not included as their entity_id do not record the token_id; detailed explanation in README.md
    token_ids, consensus_timestamps = filter_transactions(
        transactions,
        target_names=NAMES_FOR_TOKENS,
        selected_columns=[
            "entity_id",
            "consensus_timestamp",
        ]
    )

    # Save memory
    del transactions

    if not token_ids:
        return pd.DataFrame()
    
    # Get the token information.
    tokens = asyncio.run(get_tokens(provider_uri, sem, token_ids, consensus_timestamps))

    return tokens


@asset(
    key_prefix      = factory.asset_configs['key_prefix'],
    group_name      = factory.asset_configs['group_name'],
    partitions_def  = factory.get_partitions_def(),
    config_schema   = config_schema,
    ins = {
        'transactions': AssetIn(
            key=['transactions'],
        )
    }
)
def contracts(context, transactions: pd.DataFrame, **kwargs) -> pd.DataFrame:
    """
    Get contracts from a range of transactions

    @param context: dagster context
    @param transactions: dataframe of transactions
    @return: dataframe of contracts
    """

    provider_uri = context.op_config["provider_uri"]
    concurrency = context.op_config["concurrency"]
    sem = asyncio.Semaphore(value=concurrency)

    first_timestamp = transactions.iloc[0]["consensus_timestamp"]
    last_timestamp = transactions.iloc[-1]["consensus_timestamp"]

    logger.info(f"Extracting contract information from transactions from timestamp {first_timestamp} to {last_timestamp}.")

    # logger.info('Test is on for contracts. Only getting contracts for the first 25 transactions. To disable this, comment out the relevent lines in the asset function.')
    # transactions = transactions.head(25)

    contract_ids, consensus_timestamps = filter_transactions(
        transactions,
        target_names=NAMES_FOR_CONTRACTS,
        selected_columns=[
            "entity_id",
            "consensus_timestamp"
        ]
    )
    
    # Save memory
    del transactions

    if not contract_ids:
        return pd.DataFrame()

    # Get the contract information
    contracts = asyncio.run(get_contracts(provider_uri, sem, contract_ids, consensus_timestamps))

    return contracts


@asset(
    key_prefix      = factory.asset_configs['key_prefix'],
    group_name      = factory.asset_configs['group_name'],
    partitions_def  = factory.get_partitions_def(),
    config_schema   = config_schema,
    ins = {
        'transactions': AssetIn(
            key=['transactions'],
        )
    }
)
def contract_states(context, transactions: pd.DataFrame, **kwargs) -> pd.DataFrame:
    """
    Get contract states from a range of transactions that affects a contract's state

    @param context: dagster context
    @param transactions: dataframe of transactions
    @return: dataframe of contract states
    """

    provider_uri = context.op_config["provider_uri"]
    concurrency = context.op_config["concurrency"]
    sem = asyncio.Semaphore(value=concurrency)

    first_timestamp = transactions.iloc[0]["consensus_timestamp"]
    last_timestamp = transactions.iloc[-1]["consensus_timestamp"]

    logger.info(f"Extracting contract state information from transactions from timestamp {first_timestamp} to {last_timestamp}.")

    # logger.info('Test is on for contract states. Only getting contract states for the first 25 transactions. To disable this, comment out the relevent lines in the asset function.')
    # transactions = transactions.head(25)
    
    contract_ids, consensus_timestamps = filter_transactions(
        transactions,
        target_names=NAMES_FOR_CONTRACT_STATES,
        selected_columns=[
            "entity_id",
            "consensus_timestamp",
        ]
    )

    # Save memory
    del transactions

    if not contract_ids:
        return pd.DataFrame()
    
    # Get the contract state information
    contract_states = asyncio.run(get_contract_states(provider_uri, sem, contract_ids, consensus_timestamps))

    return contract_states


@asset(
    key_prefix      = factory.asset_configs['key_prefix'],
    group_name      = factory.asset_configs['group_name'],
    partitions_def  = factory.get_partitions_def(),
    config_schema   = config_schema,
    ins = {
        'transactions': AssetIn(
            key=['transactions'],
        )
    }
)
def contract_results(context, transactions: pd.DataFrame, **kwargs) -> pd.DataFrame:
    """
    Get contract results from a range of transactions that calls a contract
    
    @param context: dagster context
    @param transactions: dataframe of transactions
    @return: dataframe of contract results
    """

    provider_uri = context.op_config["provider_uri"]
    concurrency = context.op_config["concurrency"]
    sem = asyncio.Semaphore(value=concurrency)

    first_timestamp = transactions.iloc[0]["consensus_timestamp"]
    last_timestamp = transactions.iloc[-1]["consensus_timestamp"]

    logger.info(f"Extracting contract result information from transactions from timestamp {first_timestamp} to {last_timestamp}.")
    
    # logger.info('Test is on for contract results. Only getting contract results for the first 25 transactions. To disable this, comment out the relevent lines in the asset function.')
    # transactions = transactions.head(25)
    
    contract_ids, consensus_timestamps = filter_transactions(
        transactions,
        target_names=NAMES_FOR_CONTRACT_RESULTS,
        selected_columns=[
            "entity_id",
            "consensus_timestamp",
        ]
    )

    del transactions

    if not contract_ids:
        return pd.DataFrame()
    
    # Get the contract information
    contract_results = asyncio.run(get_contract_results(provider_uri, sem, contract_ids, consensus_timestamps))

    return contract_results


@asset(
    key_prefix      = factory.asset_configs['key_prefix'],
    group_name      = factory.asset_configs['group_name'],
    partitions_def  = factory.get_partitions_def(),
    config_schema   = config_schema,
    ins = {
        'transactions': AssetIn(
            key=['transactions'],
        )
    }
)
def contract_actions(context, transactions: pd.DataFrame, **kwargs) -> pd.DataFrame:
    """
    Get contract actions from a range of transactions that trigger contract actions
    
    @param context: dagster context
    @param transactions: dataframe of transactions
    @return: dataframe of contract actions
    """

    provider_uri = context.op_config["provider_uri"]
    concurrency = context.op_config["concurrency"]
    sem = asyncio.Semaphore(value=concurrency)

    first_timestamp = transactions.iloc[0]["consensus_timestamp"]
    last_timestamp = transactions.iloc[-1]["consensus_timestamp"]

    logger.info(f"Extracting contract action information from transactions from timestamp {first_timestamp} to {last_timestamp}.")

    # logger.info('Test is on for contract actions. Only getting contract actions for the first 25 transactions. To disable this, comment out the relevent lines in the asset function.')
    # transactions = transactions.head(25)
    
    transaction_ids = filter_transactions(
        transactions,
        target_names=NAMES_FOR_CONTRACT_ACTIONS,
        selected_columns=[
            "transaction_id",
        ]
    )

    del transactions

    if not transaction_ids:
        return pd.DataFrame()
    
    # Get the contract information
    contract_actions = asyncio.run(get_contract_actions(provider_uri, sem, transaction_ids))

    return contract_actions


@asset(
    key_prefix      = factory.asset_configs['key_prefix'],
    group_name      = factory.asset_configs['group_name'],
    partitions_def  = factory.get_partitions_def(),
    config_schema   = config_schema,
    ins = {
        'transactions': AssetIn(
            key=['transactions'],
        )
    }
)
def state_proof_alphas(context, transactions: pd.DataFrame, **kwargs) -> pd.DataFrame:
    """
    Get state proof alphas from a range of transactions

    @param context: dagster context
    @param transactions: dataframe of transactions
    @return: dataframe of state proof alphas
    """
    provider_uri = context.op_config["provider_uri"]
    concurrency = context.op_config["concurrency"]
    sem = asyncio.Semaphore(value=concurrency)

    first_timestamp = transactions.iloc[0]["consensus_timestamp"]
    last_timestamp = transactions.iloc[-1]["consensus_timestamp"]

    logger.info(f"Extracting state proof alpha information from transactions from timestamp {first_timestamp} to {last_timestamp}.")
    
    # logger.info('Test is on for state proof alphas. Only getting state proof alphas for the first 25 transactions. To disable this, comment out the relevent lines in the asset function.')
    # transactions = transactions.head(25)

    transactions = transactions[transactions["result"] == "SUCCESS"]

    transaction_ids = transactions["transaction_id"].tolist()
    consensus_timestamps = transactions["consensus_timestamp"].tolist()
    
    # Save memory
    del transactions

    if not transaction_ids:
        return pd.DataFrame()
    
    # Get the contract information
    state_proof_alphas = asyncio.run(get_state_proof_alphas(provider_uri, sem, transaction_ids, consensus_timestamps))

    return state_proof_alphas


factory.set_passthrough_assets([
    transactions,
    tokens,
    messages,
    contracts,
    contract_states,
    contract_results,
    contract_actions,
    state_proof_alphas,
    ])
factory_outputs = factory.construct()
