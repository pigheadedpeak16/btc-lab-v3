from __future__ import annotations

import json
from decimal import Decimal
from typing import Any

import httpx

ETHERSCAN_V2_URL = "https://api.etherscan.io/v2/api"
POLYGON_CHAIN_ID = 137


class OnchainApiError(RuntimeError):
    pass


def _loads(text: str) -> Any:
    return json.loads(text, parse_float=Decimal)


class PolygonOnchainClient:
    """Optional Polygon cross-check using the unified Etherscan V2 API.

    An API key is required by Etherscan V2.  This client is strictly read-only.
    """

    def __init__(self, api_key: str, *, client: httpx.AsyncClient | None = None) -> None:
        if not api_key.strip():
            raise ValueError("api_key is required")
        self.api_key = api_key.strip()
        self._owned_client = client is None
        self.client = client or httpx.AsyncClient(timeout=30.0, headers={"Accept": "application/json"})

    async def __aenter__(self) -> "PolygonOnchainClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._owned_client:
            await self.client.aclose()

    async def _account(self, action: str, address: str, *, page: int = 1, offset: int = 10_000) -> list[dict[str, Any]]:
        params = {
            "chainid": POLYGON_CHAIN_ID,
            "module": "account",
            "action": action,
            "address": address,
            "startblock": 0,
            "endblock": 99999999,
            "page": page,
            "offset": offset,
            "sort": "asc",
            "apikey": self.api_key,
        }
        response = await self.client.get(ETHERSCAN_V2_URL, params=params)
        if response.status_code != 200:
            raise OnchainApiError(f"Etherscan V2 HTTP {response.status_code}")
        payload = _loads(response.text)
        if not isinstance(payload, dict):
            raise OnchainApiError("invalid Etherscan V2 response")
        result = payload.get("result")
        status = str(payload.get("status", ""))
        message = str(payload.get("message", ""))
        if isinstance(result, list):
            return [x for x in result if isinstance(x, dict)]
        if status == "0" and "No transactions" in str(result):
            return []
        raise OnchainApiError(f"Etherscan V2 error: {message}: {str(result)[:300]}")

    async def normal_transactions(self, address: str, *, page: int = 1) -> list[dict[str, Any]]:
        return await self._account("txlist", address, page=page)

    async def erc20_transfers(self, address: str, *, page: int = 1) -> list[dict[str, Any]]:
        return await self._account("tokentx", address, page=page)

    async def erc1155_transfers(self, address: str, *, page: int = 1) -> list[dict[str, Any]]:
        return await self._account("token1155tx", address, page=page)
