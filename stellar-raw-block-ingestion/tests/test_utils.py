import re
import itertools
from unittest import mock

import asyncio
from aiohttp import web
from aiohttp.test_utils import TestServer
import pytest

from pipelines.core.extract.assets.chain.utils import (
    try_request,
    get_ledgers,
    get_trades,
)

from pipelines.core.extract.tests.utils import (
    generate_random_hash,
)

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
    response = await try_request(url, sem=sem, session=session, num_retries=num_retries)
    assert response == {'message': 'Hello, world!'}
    
    # Test 404, 502, 503 would lead to RequestRetriesExhaustedError
    with pytest.raises(Exception):
        await try_request(f"{url}/404", sem=sem, session=session, num_retries=num_retries)
        await try_request(f"{url}/502", sem=sem, session=session, num_retries=num_retries)
        await try_request(f"{url}/503", sem=sem, session=session, num_retries=num_retries)

    await server.close()


@pytest.mark.asyncio
async def test_get_ledgers() -> None:
    """
    Tests the get_ledgers function.
    Creates a mocked_try_request to mimic the responses from the Horizon API.
    """

    step = 2**32

    provider_uri = "https://mocked.com"
    sem = asyncio.Semaphore(1)
    sequences = list(range(2, 901))

    def mocked_try_request(url, *args, **kwargs):
        # Mock the specific ledger API response from /ledgers/<sequence>
        if "/ledgers/" in url:
            sequence = int(re.search(r"(?<=ledgers/)(\d+)", url)[0])

            json_response = {'_links': {'self':{'href': f'https://horizon.stellar.org/ledgers/{sequence}'}},
                             'id': generate_random_hash(64),
                             'paging_token': str(step * sequence),
                             'sequence': sequence,}

            return json_response

        # Mock the list of ledgers API response from /ledgers?cursor=<paging_token>
        if "/ledgers?" in url:
            initial_cursor = int(re.search(r"(?<=cursor=)(\d+)", url)[0])
            initial_sequence = initial_cursor // step

            sequences = list(range(initial_sequence + 1, initial_sequence + 201))
            paging_tokens = list(range(initial_cursor + step, initial_cursor + (201 * step), step))

            json_response = {
                '_links': {'next': {'href': f'https://mocked.com/ledgers?cursor={paging_tokens[-1]}&order=asc&limit=200'}},
                '_embedded': {
                    'records': [{'sequence': sequence, 'paging_token': str(paging_token)} for sequence, paging_token in zip(sequences, paging_tokens)]
                }
            }

            return json_response
    
    with mock.patch('pipelines.core.extract.assets.chain.utils.try_request', side_effect=mocked_try_request) as mock_try_request:
        ledgers = await get_ledgers(provider_uri, sem, sequences)

        assert mock_try_request.call_count == 6
        assert len(ledgers) == 899
        assert ledgers['sequence'].tolist() == sequences
        assert 902 not in ledgers['sequence'].tolist()


@pytest.mark.asyncio
async def test_get_trades() -> None:
    """
    Tests the get_trades function.
    Creates a mocked_try_request to mimic the responses from the Horizon API.
    """

    step = 2**32

    provider_uri = "https://mocked.com"
    sem = asyncio.Semaphore(1)
    min_paging_token = 100 * step
    max_paging_token = 200 * step

    def mocked_try_request(url, *args, **kwargs):
        initial_cursor = int(re.search(r"(?<=cursor=)(\d+)", url)[0])

        # We are simulating 11 trades per paging_token, thus the range upper end is (200 // 11) + 2 = 20
        # Plus 1 for the end of the range, plus another 1 to round up the division
        ids = [
            [f"{str(paging_token)}-{i}"
             for paging_token in range(initial_cursor + step, initial_cursor + 20 * step, step)] for i in range(1, 12)]
        ids = sorted(itertools.chain(*ids))
        paging_tokens = sorted(list(range(initial_cursor + step, initial_cursor + (20 * step), step)) * 11)

        json_response = {
            '_links': {'next': {'href': f'https://mocked.com/ledgers?cursor={paging_tokens[-1]}&order=asc&limit=200'}},
            '_embedded': {
                'records': [{'id': id, 'paging_token': str(paging_token)} for id, paging_token in zip(ids, paging_tokens)]
            }
        }

        return json_response
    
    with mock.patch('pipelines.core.extract.assets.chain.utils.try_request', side_effect=mocked_try_request) as mock_try_request:
        trades = await get_trades(provider_uri, sem, min_paging_token, max_paging_token)

        assert mock_try_request.call_count == 10
        assert len(trades) == 1100