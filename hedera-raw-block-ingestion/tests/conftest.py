import itertools
import random
from typing import List, Tuple

import pandas as pd
import pytest
from aiohttp import web
from dagster import OpExecutionContext, build_op_context

from pipelines.core.extract.assets.chain.constants import (
    ALL_TRANSACTION_NAMES,
    NAMES_WITHOUT_TRANSFERS,
)
from pipelines.core.extract.assets.chain.utils import (
    timestamp_to_int,
    timestamp_to_str,
)
from pipelines.core.extract.tests.utils import (
    generate_random_string,
    generate_random_accounts,
    generate_transfer_amounts,
)

from dagster import build_op_context, OpExecutionContext

pytest_plugins = 'aiohttp.pytest_plugin'

@pytest.fixture(scope="function")
def mock_context() -> OpExecutionContext:
    """
    Creates a mock context for the assets ops

    @return: mock context
    """
    context = build_op_context(
        op_config={
            "provider_uri": "https://mock-api.com",
            "concurrency": 10,
        }
    )
    return context


@pytest.fixture(scope="function")
def mock_blocks(request: Tuple[List[int]]) -> pd.DataFrame():
    """
    Creates a mock blocks DataFrame to replace result of calling get_blocks. Follows the data schema of the API.
    Depending on the start_block and end_block, the DataFrame will have different number of rows and different data.

    @param request: list of blocks to be mocked, in a tuple of a single parameter
    @return: mock blocks DataFrame
    """

    blocks = request.param[0]

    mock_blocks = pd.DataFrame([
        {
            "count": random.randint(1, 2048),

            "gas_used": random.randint(1, 100) * 3000,

            "hapi_version": "1.0.0",

            # 0x000...0000 followed by block number
            "hash": f"0x{'0' * 86}{block:010d}",

            "logs_bloom": "0x",

            "name": "1970-01-01T00_00_00.000000000Z.rcd",

            "number": block,

            # 0x000...0000 followed by previous block number, unless it is block 0 then it is the same as the hash
            "previous_hash": f"0x{'0' * 86}{block - 1:010d}" if block > 0 else f"0x{'0' * 96}",

            # The first 34M blocks of Hedera doesn't have a size
            "size": 8192 if block > 34000000 else None,

            # Simulating the range of timestamps you would actually get
            "timestamp.from": f"{1600000000 + block}.000000000",
            "timestamp.to": f"{1600000000 + block}.999999999",
        }
        for block in blocks]
    )

    return mock_blocks


@pytest.fixture(scope="function")
def mock_transactions(request: Tuple[List[str], List[str]]) -> pd.DataFrame():
    """
    Creates a mock transactions DataFrame to replace result of calling get_transactions. Follows the data schema of the API.

    @param request: start_timestamps list and end_timestamps list to be mocked, as a tuple of two parameters
    @return: mock transactions DataFrame
    """

    start_timestamps = request.param[0]
    end_timestamps = request.param[1]

    transaction_series = []

    for start_timestamp, end_timestamp in zip(start_timestamps, end_timestamps):

        start_timestamp_int = timestamp_to_int(start_timestamp)
        end_timestamp_int = timestamp_to_int(end_timestamp)

        random_senders = generate_random_accounts(100)
        random_nodes = generate_random_accounts(100)
        random_transfers = generate_transfer_amounts(num_accounts=3, how_many=100)
        
        # # Activate these 2 lines when the test is expanded to cover the three types of transfers
        # # Also need to inspect the exhaustive list of transaction names that has token transfers and staking reward transfers
        # random_token_transfers = generate_transfer_amounts(num_accounts=3, how_many=100, max_amount=1e5)
        # random_staking_reward_transfers = generate_transfer_amounts(num_accounts=3, how_many=100, max_amount=1e3)

        for timestamp, sender, node, transfers, transaction_name in zip(
            # 1e9 nanoseconds in timestamp_int is 1 second. By setting step to 1e7 we are generating 100 transactions per second
            range(start_timestamp_int, end_timestamp_int + int(1e7), int(1e7)),
            random_senders,
            random_nodes,
            random_transfers,
            # Itertools to help cycling through the possible transaction names for each generated transaction to ensure every one of them is simulated
            itertools.cycle(ALL_TRANSACTION_NAMES)
        ):
            series = pd.Series(
                {
                    "bytes": None,

                    "charged_tx_fee": random.randint(1, 1e6),

                    # Example valid start timestamp: "1234567890.123456789"
                    "consensus_timestamp": timestamp_to_str(timestamp),

                    "entity_id": generate_random_accounts(),

                    "max_fee": 2e9,

                    # Mimic some transactions having a memo and some not
                    "memo_base64": generate_random_string(24, empty_probability=0.5), 

                    # Rotate through the possible transaction names
                    "name": transaction_name,

                    "node": generate_random_accounts(),

                    "nonce": 0,

                    "parent_consensus_timestamp": None,

                    "result": random.choices(["SUCCESS", "ERROR"], weights=[0.8, 0.2])[0],

                    "scheduled": False,

                    "staking_reward_transfers": [],

                    "transaction_hash": generate_random_string(64),

                    # Example transction ID: "0.0.10000-1234567890-123456789"
                    "transaction_id": f"{sender}-{timestamp_to_str(timestamp).replace('.', '-')}",

                    "token_transfers": [],

                    "transfers": (
                        []
                        if transaction_name in NAMES_WITHOUT_TRANSFERS
                        else
                        [
                            {"account": sender, "amount": transfers[0], "is_approval": random.choices([False, True], weights=[0.8, 0.2])[0],},
                            {"account": node, "amount": transfers[1], "is_approval": False,},
                            {"account": generate_random_accounts(), "amount": transfers[2], "is_approval": False,},
                        ]
                    ),

                    "valid_duration_seconds": random.randint(1, 256),

                    # Example valid start timestamp: "1234567890.123456789"
                    "valid_start_timestamp": timestamp_to_str(timestamp),
                }
            )

            transaction_series.append(series)

    mock_transactions = pd.concat(transaction_series, axis=1).transpose().reset_index(drop=True)
    return mock_transactions
