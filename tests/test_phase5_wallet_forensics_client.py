import asyncio
import json

import httpx

from btc_v3.forensics.client import PolymarketDataClient

WALLET = "0xdf4c6a942bd95bf903d6066b4ba7051e6f914f22"


def test_cursor_pagination_preserves_user_filters() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        cursor = request.url.params.get("cursor")
        if cursor is None:
            body = {
                "data": [{"type": "TRADE", "timestamp": 100, "price": 0.5, "size": 1}],
                "pagination": {"has_more": True, "next_cursor": "next"},
            }
        else:
            body = {
                "data": [{"type": "REDEEM", "timestamp": 101, "size": 1}],
                "pagination": {"has_more": False, "next_cursor": None},
            }
        return httpx.Response(200, text=json.dumps(body))

    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            client = PolymarketDataClient(client=http)
            return await client.activities(WALLET, start=1, max_pages=None)

    rows = asyncio.run(run())
    assert len(rows) == 2
    assert len(requests) == 2
    assert requests[0].url.params["user"] == WALLET
    assert requests[1].url.params["user"] == WALLET
    assert requests[1].url.params["cursor"] == "next"
    assert requests[1].url.params["start"] == "1"


def test_max_pages_stops_cursor_walk() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(
            200,
            text=json.dumps(
                {
                    "data": [{"type": "TRADE", "timestamp": calls}],
                    "pagination": {"has_more": True, "next_cursor": str(calls)},
                }
            ),
        )

    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            client = PolymarketDataClient(client=http)
            return await client.activities(WALLET, max_pages=2)

    rows = asyncio.run(run())
    assert len(rows) == 2
    assert calls == 2


def test_retry_after_429_then_success() -> None:
    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(429, headers={"Retry-After": "0.01"}, text='{"error":"busy"}')
        return httpx.Response(200, text='{"data":{"trades":1}}')

    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            client = PolymarketDataClient(client=http, max_retries=2)
            return await client.user_stats(WALLET)

    result = asyncio.run(run())
    assert result == {"trades": 1}
    assert calls == 2
