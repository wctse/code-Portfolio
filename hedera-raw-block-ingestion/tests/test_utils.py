import asyncio
from typing import List
from unittest.mock import patch

import aiohttp
from aiohttp import web
from aiohttp.test_utils import TestServer
import pandas as pd
import pytest

from pipelines.core.extract.assets.chain.utils import (
    try_request,
    extract_nested_columns,
    filter_transactions,
)

start_timestamp = "1600000000.000000000"
end_timestamp = "1600000000.999999999"

@pytest.mark.usefixtures("aiohttp_client", "aiohttp_unused_port")
@pytest.mark.asyncio
async def test_try_request(aiohttp_client, aiohttp_unused_port) -> None:
    """
    Tests the try_request function

    @param aiohttp_client: aiohttp client fixture
    @param aiohttp_unused_port: aiohttp unused port fixture
    @return: None
    """

    async def handler(request: web.Request) -> web.Response:
        """
        Mock request handler for the aiohttp server

        request: aiohttp Request object
        return: aiohttp Response object
        """
        path = request.path
        if path == '/':
            data = {'message': 'Hello, world!'}
            return web.json_response(data)
        
        # Handled errors
        elif path == '/404':
            return web.Response(status=404)
        elif path == '/502':
            return web.Response(status=502)
        elif path == '/503':
            return web.Response(status=503)
        
        # Unhandled errors
        elif path == '/401':
            return web.Response(status=401)
        
    concurrency = 1
    num_retries = 1
    sem = asyncio.Semaphore(concurrency)

    port = aiohttp_unused_port()
    app = web.Application()
    app.router.add_get('/', handler)
    app.router.add_get('/401', handler)
    app.router.add_get('/404', handler)
    app.router.add_get('/502', handler)
    app.router.add_get('/503', handler)
    
    server = TestServer(app, port=port)
    await server.start_server()

    client = await aiohttp_client(server)

    url = f"http://localhost:{port}"
    sem = asyncio.Semaphore(1)
    session = client.session

    # Test 200
    response = await try_request(url, sem, session, num_retries)
    assert response == {'message': 'Hello, world!'}
    
    # Test 404, 502, 503 would lead to RequestRetriesExhaustedError
    with pytest.raises(Exception):
        await try_request(f"{url}/404", sem, session, num_retries)
        await try_request(f"{url}/502", sem, session, num_retries)
        await try_request(f"{url}/503", sem, session, num_retries)

    await server.close()


@pytest.mark.usefixtures("mock_transactions")
@pytest.mark.parametrize(
    "mock_transactions, nested_column_name, selected_columns",
    [
        # Zero length transactions input
        (tuple([[start_timestamp], [start_timestamp]]), "transfers", ["consensus_timestamp"]),

        # Non-zero length transactions input
        (tuple([[start_timestamp], [end_timestamp]]), "transfers", ["consensus_timestamp", "transaction_id"]),

        # Non-zero length transactions input with possibly empty nested column
        (tuple([[start_timestamp], [end_timestamp]]), "token_transfers", ["consensus_timestamp", "transaction_id"]),
    ],
    indirect=["mock_transactions"]
)
def test_extract_nested_columns(mock_transactions: pd.DataFrame, nested_column_name: str, selected_columns: List[str]) -> None:
    """
    Tests the extract_nested_columns function

    @param mock_transactions: mock transactions DataFrame to substitute the result of get_transactions, used by the mock_transactions fixture
    @param nested_column: name of the nested column to extract
    @param other_columns: list of other columns to include in the returned DataFrame
    @return: None
    """
    result = extract_nested_columns(mock_transactions, nested_column_name, selected_columns)

    assert isinstance(result, pd.DataFrame)
    assert nested_column_name not in result

    if not len(result):
        return
    
    # Get all the keys from the nested column
    nested_keys = set()
    for nested_data in mock_transactions[nested_column_name]:
        if isinstance(nested_data, list):
            for nested in nested_data:
                nested_keys.update(nested.keys())

    expected_columns = set(selected_columns) | nested_keys
    assert set(result.columns) == expected_columns


@pytest.mark.usefixtures("mock_transactions")
@pytest.mark.parametrize(
    "mock_transactions, target_names, selected_columns",
    [
        # Zero length transactions input
        (tuple([[start_timestamp], [start_timestamp]]), ["CONSENSUSSUBMITMESSAGE"], ["consensus_timestamp"]),

        # Non-zero length transactions input, 1 output
        (tuple([[start_timestamp], [end_timestamp]]), ["TOKENCREATION, TOKENDELETION"], ["consensus_timestamp"]),

        # Non-zero length transactions input, multiple outputs
        (tuple([[start_timestamp], [end_timestamp]]), ["TOKENCREATION, TOKENDELETION"], ["consensus_timestamp", "entity_id"]),

        # Non-zero length transactions input with possibly possibly non-existent names;
        # From the mock transaction generating processm, CONSENSUSCREATETOPIC should exist and TOKENWIPE should not
        (tuple([[start_timestamp], ["1600000000.100000000"]]), ["CONSENSUSCREATETOPIC, TOKENWIPE"], ["consensus_timestamp", "entity_id"]),
    ],
    indirect=["mock_transactions"]
)
def test_filter_transactions(mock_transactions: pd.DataFrame, target_names: List[str], selected_columns: List[str]) -> None:
    """
    Tests the filter_transactions function

    @param mock_transactions: mock transactions DataFrame to substitute the result of get_transactions, used by the mock_transactions fixture
    @param target_names: list of transaction names to keep
    @param selected_columns: list of columns to keep
    @return: None
    """

    result = filter_transactions(mock_transactions, target_names, selected_columns)

    assert isinstance(result, list)

    if len(selected_columns) == 1:
        assert all(isinstance(item, str) for item in result)
    else:
        assert all(isinstance(item, list) for item in result)
        assert all(all(isinstance(subitem, str) for subitem in sublist) for sublist in result)
