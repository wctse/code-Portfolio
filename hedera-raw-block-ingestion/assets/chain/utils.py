import asyncio
from asyncio import Semaphore
from typing import List, Tuple, Union

import aiohttp
from aiohttp import ClientSession, ClientResponse
import pandas as pd

from dagster import get_dagster_logger

logger = get_dagster_logger()


async def try_request(url: str, sem: Semaphore, session: ClientSession, num_retries: int = 5) -> Union[dict, List[dict]]:
    """
    Try a request for a given number of times before raising error to cope with the instability of Hedera API.

    @param url: url to request
    @param sem: semaphore to limit the number of concurrent requests
    @param session: aiohttp session
    @param num_retries: number of times to retry
    @return: response from the request
    """

    for _ in range(num_retries):
        await asyncio.sleep(1)

        try:
            async with sem:
                logger.info(f"Requesting {url}. Remaining tasks for this partition: {len(asyncio.all_tasks())}")

                response = await session.get(url, raise_for_status=True)
                json_response = await response.json()

                return json_response
            
        except Exception as e:
            logger.error(f"Exception occurred when requesting {url}: {e}")
            await asyncio.sleep(1)
    
    raise Exception(f"Failed to request {url} after {num_retries} retries")


async def get_blocks(
        provider_uri: str,
        sem: Semaphore,
        numbers: List[int],
) -> pd.DataFrame:
    """
    Handles getting blocks from the API using try_request.

    @param provider_uri: URI of the provider
    @param sem: semaphore to limit the number of concurrent requests
    @param numbers: list of numbers of blocks to get
    @return: DataFrame of blocks
    """

    async with aiohttp.ClientSession() as session:
        tasks = []

        for number in numbers:
            url = provider_uri + f"/api/v1/blocks/{number}"
            tasks.append(try_request(url, sem, session))

        results = await asyncio.gather(*tasks)

    blocks = [pd.json_normalize(result) for result in results]
    blocks = pd.concat(blocks, ignore_index=True)
    return blocks


async def get_transactions(
        provider_uri: str,
        sem: Semaphore,
        start_consensus_timestamps: List[str],
        end_consensus_timestamps: List[str],
) -> pd.DataFrame:
    """
    Handles getting transactions from the API using try_request.

    @param provider_uri: URI of the provider
    @param sem: semaphore to limit the number of concurrent requests
    @param start_timestamps: list of start timestamps of transactions to get
    @param end_timestamps: list of end timestamps of transactions to get
    @return: DataFrame of transactions
    """
    
    transactions = []

    async with aiohttp.ClientSession() as session:
        tasks = []

        for start_consensus_timestamp, end_consensus_timestamp in zip(start_consensus_timestamps, end_consensus_timestamps):
            url = provider_uri + f"/api/v1/transactions/?timestamp=gte:{start_consensus_timestamp}&timestamp=lte:{end_consensus_timestamp}&limit=100&order=asc"
            tasks.append(try_request(url, sem, session))

        results = await asyncio.gather(*tasks)
        transactions += [result["transactions"] for result in results]

        # Handle multiple pages
        while True:
            next_links = [provider_uri + result["links"]["next"] for result in results if result["links"]["next"]]

            if not next_links:
                break

            tasks = [try_request(next_link, sem, session) for next_link in next_links]

            results = await asyncio.gather(*tasks)
            transactions += [result["transactions"] for result in results]

    transactions = [pd.json_normalize(transaction) for transaction in transactions]
    transactions = pd.concat(transactions, ignore_index=True)
    transactions = transactions.sort_values(by="consensus_timestamp", ignore_index=True)
    return transactions


async def get_messages(
        provider_uri: str,
        sem: Semaphore,
        consensus_timestamps: List[str],
) -> pd.DataFrame:
    """
    Handles getting messages from the API using try_request.
    
    @param provider_uri: URI of the provider
    @param sem: semaphore to limit the number of concurrent requests
    @param consensus_timestamps: list of consensus timestamps of transactions with messages to get
    @return: DataFrame of messages
    """

    async with aiohttp.ClientSession() as session:
        tasks = []

        for consensus_timestamp in consensus_timestamps:
            url = provider_uri + f"/api/v1/topics/messages/{consensus_timestamp}"
            tasks.append(try_request(url, sem, session))

        results = await asyncio.gather(*tasks)

    messages = [pd.json_normalize(result) for result in results]
    messages = pd.concat(messages, ignore_index=True)
    return messages


async def get_tokens(
        provider_uri: str,
        sem: Semaphore,
        token_ids: List[str],
        consensus_timestamps: List[str],
) -> pd.DataFrame:
    """
    Handles getting tokens from the API using try_request.

    @param provider_uri: URI of the provider
    @param sem: semaphore to limit the number of concurrent requests
    @param token_ids: list of token IDs to get
    @param consensus_timestamps: list of the specific consensus timestamp to get the token at
    @return: DataFrame of tokens
    """

    if len(token_ids) != len(consensus_timestamps):
        raise ValueError("token_ids and consensus_timestamps must be the same length")

    async with aiohttp.ClientSession() as session:
        tasks = []

        for token_id, consensus_timestamp in zip(token_ids, consensus_timestamps):
            url = provider_uri + f"/api/v1/tokens/{token_id}/?timestamp={consensus_timestamp}"
            tasks.append(try_request(url, sem, session))

        results = await asyncio.gather(*tasks)

    tokens = [pd.json_normalize(result) for result in results]
    tokens = pd.concat(tokens, ignore_index=True)
    return tokens
    

async def get_contracts(
        provider_uri: str,
        sem: Semaphore,
        contract_ids: List[str],
        consensus_timestamps: List[str],
) -> pd.DataFrame:
    """
    Handles getting contracts from the API using try_request.
    
    @param provider_uri: URI of the provider
    @param sem: semaphore to limit the number of concurrent requests
    @param contract_ids: list of contract IDs to get
    @param consensus_timestamps: list of the specific consensus timestamp to get the contract at
    @return: DataFrame of contracts
    """

    if len(contract_ids) != len(consensus_timestamps):
        raise ValueError("contract_ids and consensus_timestamps must be the same length")
    
    async with aiohttp.ClientSession() as session:
        tasks = []

        for contract_id, consensus_timestamp in zip(contract_ids, consensus_timestamps):
            url = provider_uri + f"/api/v1/contracts/{contract_id}/?timestamp={consensus_timestamp}"
            tasks.append(try_request(url, sem, session))

        results = await asyncio.gather(*tasks)

    contracts = [pd.json_normalize(result) for result in results]
    contracts = pd.concat(contracts, ignore_index=True)
    contracts["consensus_timestamps"] = consensus_timestamps
    return contracts


async def get_contract_states(
        provider_uri: str,
        sem: Semaphore,
        contract_ids: List[str],
        consensus_timestamps: List[str],
) -> pd.DataFrame:
    """
    Handles getting contract states from the API using try_request.

    @param provider_uri: URI of the provider
    @param sem: semaphore to limit the number of concurrent requests
    @param contract_ids: list of contract IDs to get
    @param consensus_timestamps: list of the specific consensus timestamp to get the contract state at
    @return: DataFrame of contract states
    """

    if len(contract_ids) != len(consensus_timestamps):
        raise ValueError("contract_ids and consensus_timestamps must be the same length")

    async with aiohttp.ClientSession() as session:
        tasks = []

        for contract_id, consensus_timestamp in zip(contract_ids, consensus_timestamps):
            url = provider_uri + f"/api/v1/contracts/{contract_id}/state/?timestamp={consensus_timestamp}"
            tasks.append(try_request(url, sem, session))

        results = await asyncio.gather(*tasks)

    contract_states = [pd.json_normalize(result) for result in results]
    contract_states = pd.concat(contract_states, ignore_index=True)
    return contract_states


async def get_contract_results(
        provider_uri: str,
        sem: Semaphore,
        contract_ids: List[str],
        consensus_timestamps: List[str],
) -> pd.DataFrame:
    """
    Handles getting contract results from the API using try_request.
    
    @param provider_uri: URI of the provider
    @param sem: semaphore to limit the number of concurrent requests
    @param contract_ids: list of contract IDs to get
    @param consensus_timestamps: list of the specific consensus timestamp to get the contract result at
    @return: DataFrame of contract results
    """

    if len(contract_ids) != len(consensus_timestamps):
        raise ValueError("contract_ids and consensus_timestamps must be the same length")

    async with aiohttp.ClientSession() as session:
        tasks = []

        for contract_id, consensus_timestamp in zip(contract_ids, consensus_timestamps):
            url = provider_uri + f"/api/v1/contracts/{contract_id}/results/{consensus_timestamp}"
            tasks.append(try_request(url, sem, session))

        results = await asyncio.gather(*tasks)

    contract_results = [pd.json_normalize(result) for result in results]
    contract_results = pd.concat(contract_results, ignore_index=True)
    return contract_results


async def get_contract_actions(
        provider_uri: str,
        sem: Semaphore,
        transaction_ids: List[str],
) -> pd.DataFrame:
    """
    Handles getting contract actions from the API using try_request.
    
    @param provider_uri: URI of the provider
    @param sem: semaphore to limit the number of concurrent requests
    @param transaction_ids: list of transaction IDs to get
    @return: DataFrame of contract actions
    """

    contract_actions = []

    async with aiohttp.ClientSession() as session:
        tasks = []

        for transaction_id in transaction_ids:
            url = provider_uri + f"/api/v1/contracts/results/{transaction_id}/actions"
            tasks.append(try_request(url, sem, session))

        results = await asyncio.gather(*tasks)
        contract_actions += [result["actions"] for result in results]

        # Handle multiple pages
        while True:
            next_links = [provider_uri + result["links"]["next"] for result in results if result["links"]["next"]]

            if not next_links:
                break

            tasks = [try_request(next_link, sem, session) for next_link in next_links]

            results = await asyncio.gather(*tasks)
            contract_actions += [result["actions"] for result in results]

    # Transactions may not contain any contract actions, therefore we need to handle the case of empty lists in contract_actions
    contract_actions = [action for action in contract_actions if action]

    if len(contract_actions) == 0:
        return pd.DataFrame()

    contract_actions = [pd.json_normalize(contract_action) for contract_action in contract_actions]
    contract_actions = pd.concat(contract_actions, ignore_index=True)
    contract_actions = contract_actions.sort_values(by="timestamp", ignore_index=True)
    return contract_actions


async def get_state_proof_alphas(
        provider_uri: str,
        sem: Semaphore,
        transaction_ids: List[str],
        consensus_timestamps: List[str],
) -> pd.DataFrame:
    
    """
    Get state proofs for a list of transactions.

    @param provider_uri: URI of the provider
    @param sem: semaphore to limit the number of concurrent requests
    @param transaction_ids: list of transaction IDs to get
    @param consensus_timestamps: list of the consensus timestamps of the transactions to be inserted into the DataFrame
    @return: DataFrame of state proofs
    """
    
    if len(transaction_ids) != len(consensus_timestamps):
        raise ValueError("transaction_ids and consensus_timestamps must be the same length")
    
    async with aiohttp.ClientSession() as session:
        tasks = []

        for transaction_id in transaction_ids:
            url = provider_uri + f"/api/v1/transactions/{transaction_id}/stateproof"
            tasks.append(try_request(url, sem, session))

        results = await asyncio.gather(*tasks)

    state_proof_alphas = [pd.json_normalize(result) for result in results]
    state_proof_alphas = pd.concat(state_proof_alphas, ignore_index=True)
    state_proof_alphas["transaction_id"] = transaction_ids
    state_proof_alphas["consensus_timestamp"] = consensus_timestamps
    return state_proof_alphas


def extract_nested_columns(
        df: pd.DataFrame,
        nested_column_name: str,
        selected_columns: List[str]
    ) -> pd.DataFrame:
    """
    Extract objects nested in a column of a DataFrame into its own DataFrame.
    NOTE: Not used in the current version of the pipeline.

    @param df: DataFrame with nested json objects. Should have meaningful index to be used in the new DataFrame.
    @param nested_column_name: name of the column with nested objects
    @param selected_columns: list of other columns to include from the original DataFrame
    @return: DataFrame with nested json objects extracted
    """

    if nested_column_name not in df.columns:
        return pd.DataFrame()
    
    # Remove rows without any data in the nested column, in which it may be an empty list or NaN
    df = df[df[nested_column_name].notna()]
    df = df[df[nested_column_name].apply(lambda x: len(x) > 0 if isinstance(x, list) else x)]

    if len(df) == 0:
        return pd.DataFrame()

    extracted_df = df[[nested_column_name] + selected_columns]
    extracted_df = extracted_df.explode(nested_column_name)
    extracted_df = pd.concat([
        extracted_df.drop([nested_column_name], axis=1),
        extracted_df[nested_column_name].apply(pd.Series),
    ], axis=1)

    return extracted_df


def filter_transactions(
        transactions: pd.DataFrame,
        target_names: List[str],
        selected_columns: List[str]
    )-> Union[List[str], List[List[str]]]:
    """
    Filter transactions by name and select only relevant columns for downstream asset inputs.
    This helps prevent signal 9 termination from running out of memory of the dagster op.

    @param transactions: DataFrame of transactions 
    @param target_names: list of transaction names (i.e. types) used to filter rows of transactions
    @param selected_columns: list of necessary columns used in API calls for downstream assets
    @return: the column data kept as lists, or a list of lists of column data if multiple columns are kept
    """

    filtered_transactions = transactions[(transactions["name"].isin(target_names)) & (transactions["result"] == "SUCCESS")]
    filtered_transactions = filtered_transactions[selected_columns]
    filtered_transactions = filtered_transactions.dropna(how="any")

    # Generate a list for each column kept for downstream assets' API calls
    filtered_column_data = [filtered_transactions[column].tolist() for column in selected_columns]

    if len(selected_columns) == 1:
        filtered_column_data = filtered_column_data[0]

    return filtered_column_data


def timestamp_to_str(nanosecond_timestamp: int) -> str:
    """
    Convert an int nanosecond timestamp to the Hedera string format

    @param nanosecond_timestamp: timestamp to convert
    @return: string representation of timestamp. Format: "1234567890.123456789"
    """

    hedera_timestamp = str(nanosecond_timestamp)[:-9] + "." + str(nanosecond_timestamp)[-9:]
    return hedera_timestamp


def timestamp_to_int(hedera_timestamp: str) -> int:
    """
    Convert a Hedera nanosecond timestamp to an integer

    @param hedera_timestamp: timestamp to convert. Format: "1234567890.123456789"
    @return: integer representation of timestamp
    """

    nanosecond_timestamp = hedera_timestamp.replace(".", "")
    return int(nanosecond_timestamp)
