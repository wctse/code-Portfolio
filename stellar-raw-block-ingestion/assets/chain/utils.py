"""
utils for Stellar ETL
ref: https://developers.stellar.org/api/horizon
"""

import asyncio
from asyncio import Semaphore
from typing import List, Union

import aiohttp
from aiohttp import ClientSession
import pandas as pd

from dagster import get_dagster_logger

logger = get_dagster_logger()


async def try_request(url: str, sem: Semaphore, session: ClientSession, params: dict = {}, num_retries: int = 5) -> Union[dict, List[dict]]:
    """
    Try a request for a given number of times before raising error to cope with the instability of Hedera API.

    @param url: url to request
    @param sem: semaphore to limit the number of concurrent requests
    @param session: aiohttp session
    @param params: params for the request
    @param num_retries: number of times to retry
    @return: response from the request
    """

    for _ in range(num_retries):
        await asyncio.sleep(1)

        try:
            async with sem:
                logger.info(f"Requesting {url}. Remaining tasks for this partition: {len(asyncio.all_tasks())}")

                response = await session.get(url, params=params, raise_for_status=True)
                json_response = await response.json()

                return json_response
            
        except Exception as e:
            logger.error(f"Exception occurred when requesting {url}: {e}")
            await asyncio.sleep(1)
    
    raise Exception(f"Failed to request {url} after {num_retries} retries")


async def get_ledgers(
        provider_uri: str,
        sem: Semaphore,
        sequences: List[int],
) -> pd.DataFrame:
    """
    Handles getting ledgers from the API using try_request.
    The logic assumes that the ledger sequences are consecutive to achieve an efficient request calling,
    although the end results are checked to only return the input sequences.
    
    @param provider_uri: URI of the provider
    @param sem: semaphore to limit the number of concurrent requests
    @param sequences: list of sequences of ledgers to get
    """
    
    # Stellar ledger endpoints limits the maximum number of ledgers per request to 200
    # We calculate the number of loops required to get all the ledgers
    min_sequence = min(sequences)
    max_sequence = max(sequences)

    # For typical cases of 720 blocks per partition, async is actually slower because we need to divide into partitions and obtain the initial cursor for each partition first
    # Therefore, the code below is actually ran synchronously despite the await keywords and the use of sem and session
    async with aiohttp.ClientSession() as session:

        # Get the cursor position
        url = provider_uri + f"/ledgers/{min_sequence}"
        json_response = await try_request(url, sem, session)
        initial_cursor = json_response["paging_token"]

        # Initialize the current response as the results list, because the next call using the initial cursor will exclude the current response
        ledgers = pd.json_normalize(json_response)

        # Call the first batch of 200 ledgers
        json_response = await try_request(f"https://horizon.stellar.org/ledgers?cursor={initial_cursor}&limit=200&order=asc", sem, session)

        ledgers = pd.concat([ledgers, pd.json_normalize(json_response["_embedded"]["records"])], ignore_index=True)
        next_link = json_response["_links"]["next"]["href"]

        # Handle multiple pages
        while True:
            if max_sequence in set(ledgers["sequence"]):
                break

            json_response = await try_request(next_link, sem, session)
            ledgers = pd.concat([ledgers, pd.json_normalize(json_response["_embedded"]["records"])], ignore_index=True)

            next_link = json_response["_links"]["next"]["href"]

    # As the ledgers are requested in batches, we need to filter out the ledgers that are not in the input sequences
    ledgers = ledgers[ledgers["sequence"].isin(sequences)]
    ledgers = ledgers.sort_values(by="sequence", ignore_index=True)

    return ledgers


async def get_transactions(
        provider_uri: str,
        sem: Semaphore,
        ledger_sequences: List[int],
) -> pd.DataFrame:
    """
    Handles getting transactions from the API using try_request.
    
    @param provider_uri: URI of the provider
    @param sem: semaphore to limit the number of concurrent requests
    @param ledger_sequences: list of sequences of ledgers to get transactions from
    """
    transactions = []

    async with aiohttp.ClientSession() as session:
        tasks = []

        for sequence in ledger_sequences:
            url = provider_uri + f"/ledgers/{sequence}/transactions?&order=asc&limit=200&include_failed=true"
            tasks.append(try_request(url, sem, session))

        json_responses = await asyncio.gather(*tasks)
        transactions += [json_responses["_embedded"]["records"] for json_responses in json_responses]
        next_links = []

        # Handle multiple pages
        while True:
            # If next links are different to the latest link, it means that there are more pages to request
            next_links = [json_response["_links"]["next"]["href"] for json_response in json_responses
                          if json_response["_links"]["next"]["href"] not in next_links]
            
            if not next_links:
                break
        
            tasks = [try_request(next_link, sem, session) for next_link in next_links]
            json_responses = await asyncio.gather(*tasks)

            transactions += [json_response["_embedded"]["records"] for json_response in json_responses]
    
    if not transactions:
        return pd.DataFrame()

    transactions = [pd.json_normalize(transaction) for transaction in transactions]
    transactions = pd.concat(transactions, ignore_index=True)
    transactions = transactions.sort_values(by="paging_token", ignore_index=True)

    return transactions


async def get_operations(
        provider_uri: str,
        sem: Semaphore,
        ledger_sequences: List[int],
) -> pd.DataFrame:
    """
    Handles getting operations from the API using try_request.
    
    @param provider_uri: URI of the provider
    @param sem: semaphore to limit the number of concurrent requests
    @param ledger_sequences: list of sequences of ledgers to get operations from
    """
    operations = []

    async with aiohttp.ClientSession() as session:
        tasks = []

        for sequence in ledger_sequences:
            url = provider_uri + f"/ledgers/{sequence}/operations?order=asc&limit=200&include_failed=true"
            tasks.append(try_request(url, sem, session))

        json_responses = await asyncio.gather(*tasks)
        operations += [json_responses["_embedded"]["records"] for json_responses in json_responses]
        next_links = []

        # Handle multiple pages
        while True:
            next_links = [json_response["_links"]["next"]["href"] for json_response in json_responses
                          if json_response["_links"]["next"]["href"] not in next_links]
            
            if not next_links:
                break
        
            tasks = [try_request(next_link, sem, session) for next_link in next_links]
            json_responses = await asyncio.gather(*tasks)

            operations += [json_response["_embedded"]["records"] for json_response in json_responses]

    if not operations:
        return pd.DataFrame()

    operations = [pd.json_normalize(operation) for operation in operations]
    operations = pd.concat(operations, ignore_index=True)
    operations = operations.sort_values(by="paging_token", ignore_index=True)

    return operations



async def get_effects(
        provider_uri: str,
        sem: Semaphore,
        ledger_sequences: List[int],
) -> pd.DataFrame:
    """
    Handles getting effects from the API using try_request.
    
    @param provider_uri: URI of the provider
    @param sem: semaphore to limit the number of concurrent requests
    @param ledger_sequences: list of sequences of ledgers to get effects from
    @param ledger_paging_tokens: list of paging tokens of ledgers to get effects from
    """
    effects = []

    async with aiohttp.ClientSession() as session:
        tasks = []

        for sequence in ledger_sequences:
            url = provider_uri + f"/ledgers/{sequence}/effects?order=asc&limit=200&include_failed=true"
            tasks.append(try_request(url, sem, session))

        json_responses = await asyncio.gather(*tasks)
        effects += [json_responses["_embedded"]["records"] for json_responses in json_responses]
        next_links = []

        # Handle multiple pages
        while True:
            next_links = [json_response["_links"]["next"]["href"] for json_response in json_responses
                          if json_response["_links"]["next"]["href"] not in next_links]
            
            if not next_links:
                break
        
            tasks = [try_request(next_link, sem, session) for next_link in next_links]
            json_responses = await asyncio.gather(*tasks)

            effects += [json_response["_embedded"]["records"] for json_response in json_responses]

    if not effects:
        return pd.DataFrame()

    effects = [pd.json_normalize(effect) for effect in effects]
    effects = pd.concat(effects, ignore_index=True)
    effects = effects.sort_values(by="paging_token", ignore_index=True)

    return effects


async def get_trades(
        provider_uri: str,
        sem: Semaphore,
        min_paging_token: int,
        max_paging_token: int,
) -> pd.DataFrame:
    """
    Handles getting trades from the API using try_request.

    @param provider_uri: URI of the provider
    @param sem: semaphore to limit the number of concurrent requests
    @param min_paging_token: minimum paging token of trades to get
    @param max_paging_token: maximum paging token of trades to get
    """

    # Each ledger usually has ~ 100 trades each, much less than the request limit of 200
    # Therefore we create different segments here to efficiently use the API request limit
    num_segments = 10
    step = (max_paging_token - min_paging_token) / num_segments

    # Generate lists of min and max paging_tokens for each segment
    min_paging_tokens = [f"{(min_paging_token + i * step):.0f}" for i in range(num_segments)]
    max_paging_tokens = [f"{(min_paging_token + (i + 1) * step):.0f}: .0f" for i in range(num_segments)]

    trades = []

    async with aiohttp.ClientSession() as session:
        tasks = []

        # Create tasks for each segment
        for min_token in min_paging_tokens:
            url = provider_uri + f"/trades?cursor={min_token}&order=asc&limit=200"
            tasks.append(try_request(url, sem, session))

        json_responses = await asyncio.gather(*tasks)
        trades += [json_responses["_embedded"]["records"] for json_responses in json_responses]

        # Handle multiple pages for each of the tasks, while keeping track of the completion status of each segment
        # It is turned and kept true when completed
        completed_segments = [False] * num_segments

        while not all(completed_segments):
            for i in range(num_segments):
                # If a segment has not been completed, check if it is completed now
                # It is completed when the page returned has reached the max paging token for that segment
                if not completed_segments[i]:
                    latest_paging_token = json_responses.pop(0)["_embedded"]["records"][-1]["paging_token"]
                    completed_segments.append(latest_paging_token > max_paging_tokens[i])

                # If a segment has been completed, nothing was requested so keep the completion status as true
                else:
                    completed_segments.append(True)

            # If a segment has not been completed, get the next link from the previous json response
            next_links = [json_response["_links"]["next"]["href"]
                          for json_response, completed in zip(json_responses, completed_segments)
                          if not completed]
            
            if not next_links:
                break
        
            # Create and run new tasks for each segment not yet completed
            tasks = [try_request(next_link, sem, session) for next_link in next_links]
            json_responses = await asyncio.gather(*tasks)

            trades += [json_response["_embedded"]["records"] for json_response in json_responses]
    
    if not trades:
        return pd.DataFrame()

    trades = [pd.json_normalize(trade) for trade in trades]
    trades = pd.concat(trades, ignore_index=True)

    # The last segment may return trades larger than the max paging token, so we remove them here
    # The format of the paging tokens in trades are "<paging_token>-<order>". So we need to split the string and just extract the paging token
    trades = trades[trades["paging_token"].apply(lambda x: x.split("-")[0]).astype(int) <= max_paging_token]

    # There will be duplicates between segments when the amount of trades in a segment is not a multiple of 200 (the request limit)
    trades = trades.drop_duplicates(ignore_index=True)

    # We need to sort the values as the segment results will not be in order
    # Separate the paging token and the order into two columns for sorting, as paging tokens are following time order
    # We are not sorting by string values of the paging tokens as it will mess up the order
    trades[["sort_value1", "sort_value2"]] = trades["id"].str.split("-", expand=True)
    trades = trades.sort_values(by=["sort_value1", "sort_value2"], ignore_index=True)
    trades = trades.drop(columns=["sort_value1", "sort_value2"])
    
    return trades

