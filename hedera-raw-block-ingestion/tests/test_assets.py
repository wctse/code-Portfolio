from unittest.mock import patch

import pytest
import pandas as pd
from dagster import OpExecutionContext

from pipelines.core.extract.assets.chain.chain import (
    blocks,
    transactions,
    messages,
    tokens,
    contracts,
    contract_states,
    contract_results,
    contract_actions,
    state_proof_alphas, 
)
from pipelines.core.extract.assets.chain.utils import timestamp_to_int
from pipelines.core.extract.tests.constants import SAMPLE_BLOCK_DATAFRAME


@pytest.mark.usefixtures("mock_context", "mock_blocks")
@pytest.mark.parametrize(
        # NOTE: Is there any way to pass the start_block and end_block arguments to mock_blocks fixture and test_blocks function together,
        # so we don't have to repeat the values here?
        "mock_blocks, start_block, end_block",
        [
            ([tuple(range(0, 0 + 1))], 0, 0),
            ([tuple(range(0, 100 + 1))], 0, 100),
        ],
        indirect=["mock_blocks"],
)
def test_blocks(mock_context: OpExecutionContext, mock_blocks: pd.DataFrame, start_block: int, end_block: int) -> None:
    """
    Tests the blocks function
    
    @param mock_context: mock context for op configs, used by the mock_context fixture
    @param mock_blocks: mock blocks DataFrame to substitute the result of get_blocks, used by the mock_blocks fixture
    @param start_block: start block of the mock blocks
    @param end_block: end block of the mock blocks
    @return: None
    """

    # These are essential columns that contain important information or for other assets to work
    columns_to_check_notnull = ["count", "number", "timestamp.from", "timestamp.to"]
    expected_length = end_block - start_block + 1

    with patch("pipelines.core.extract.assets.chain.chain.get_blocks") as mock_get_blocks:
        mock_get_blocks.return_value = mock_blocks
        blocks_df = blocks(mock_context, start_block, end_block)

    # Check the dataframe structure
    assert len(blocks_df) == expected_length
    assert blocks_df.columns.equals(mock_blocks.columns)
    assert blocks_df.dtypes.equals(mock_blocks.dtypes)
    
    # Check the dataframe values
    assert (blocks_df["number"] >= 0).all()
    for column in columns_to_check_notnull:
        assert blocks_df[column].notnull().all()


@pytest.mark.usefixtures("mock_context", "mock_transactions")
@pytest.mark.parametrize(
    "mock_transactions, blocks, expected_length",
    [
        # We can use a single-row sample block dataframe here because it is not used to actually make the calls to get the transactions
        # Instead, the transcation call results are substituted with the mock_transactions fixture result instead
        # The sample block dataframe is to test the procedure extracting start_timestamp and end_timestamp from the blocks dataframe
        ([tuple([["1600000000.000000000"], ["1600000000.999999999"]]), SAMPLE_BLOCK_DATAFRAME, 100])
    ],
    indirect=["mock_transactions"]
)
def test_transactions(mock_context: OpExecutionContext, mock_transactions: pd.DataFrame, blocks: pd.DataFrame, expected_length: int) -> None:
    """
    Test the transactions and transfers function. Right now the test is only limited to transactions, not transfers.

    @param mock_context: mock context for op configs, used by the mock_context fixture
    @param mock_transactions: mock transactions DataFrame to substitute the result of get_transactions, used by the mock_transactions fixture
    @param blocks: mock blocks DataFrame to substitute the result of get_blocks
    @param expected_length: expected length of the returned DataFrame
    @return: None
    """

    expected_transactions_df = mock_transactions

    columns_to_check_notnull = ["name", "node", "result", "scheduled", "transaction_hash", "transaction_id", "valid_start_timestamp"]

    with patch("pipelines.core.extract.assets.chain.chain.get_transactions") as mock_get_transactions:
        mock_get_transactions.return_value = mock_transactions
        transactions_df = transactions(mock_context, blocks)

    # Check the dataframe structure
    assert len(transactions_df) == expected_length
    assert transactions_df.columns.equals(expected_transactions_df.columns)
    assert transactions_df.dtypes.equals(expected_transactions_df.dtypes)
    
    # Check the dataframe values
    assert (transactions_df["consensus_timestamp"].apply(timestamp_to_int) > 0).all()
    assert (transactions_df["valid_start_timestamp"].apply(timestamp_to_int) > 0).all()
    assert (transactions_df["valid_duration_seconds"] > 0).all()
    for transaction_id in transactions_df["transaction_id"]:
        assert transaction_id.count("-") >= 2
    for column in columns_to_check_notnull:
        assert transactions_df[column].notnull().all()
