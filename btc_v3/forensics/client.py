from __future__ import annotations

import asyncio
import json
from decimal import Decimal
from typing import Any, AsyncIterator, Mapping

import httpx

from .models import WalletActivity, WalletTrade

DATA_API_BASE = "https://data-api.polymarket.com"
GAMMA_API_BASE = "https://gamma-api.polymarket.com"


class PublicDataApiError(RuntimeError):
    pass


def _loads_decimal(text: str) -> Any:
    return json.loads(text, parse_float=Decimal)


class PolymarketDataClient:
    """Read-only public Polymarket wallet client with bounded retry/pagination."""

    def __init__(
        self,
        *,
        client: httpx.AsyncClient | None = None,
        timeout_seconds: float = 20.0,
        max_retries: int = 5,
    ) -> None:
        self._owned_client = client is None
        self.client = client or httpx.AsyncClient(
            timeout=timeout_seconds,
            headers={"Accept": "application/json", "User-Agent": "btc-lab-v3-wallet-forensics/0.5.2"},
            follow_redirects=True,
        )
        self.max_retries = max_retries

    async def __aenter__(self) -> "PolymarketDataClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._owned_client:
            await self.client.aclose()

    async def _get(self, base: str, path: str, params: Mapping[str, Any]) -> Any:
        url = f"{base.rstrip('/')}/{path.lstrip('/')}"
        for attempt in range(self.max_retries + 1):
            try:
                response = await self.client.get(url, params={k: v for k, v in params.items() if v is not None})
            except httpx.HTTPError as exc:
                if attempt >= self.max_retries:
                    raise PublicDataApiError(f"network error requesting {path}: {exc}") from exc
                await asyncio.sleep(min(8.0, 0.5 * (2**attempt)))
                continue
            if response.status_code == 200:
                try:
                    return _loads_decimal(response.text)
                except json.JSONDecodeError as exc:
                    raise PublicDataApiError(f"invalid JSON from {path}") from exc
            if response.status_code in {429, 503} and attempt < self.max_retries:
                retry_after = response.headers.get("Retry-After")
                try:
                    delay = float(retry_after) if retry_after else min(8.0, 0.5 * (2**attempt))
                except ValueError:
                    delay = min(8.0, 0.5 * (2**attempt))
                await asyncio.sleep(max(0.05, min(delay, 30.0)))
                continue
            trace = response.headers.get("x-trace-id", "-")
            body = response.text[:800]
            raise PublicDataApiError(
                f"HTTP {response.status_code} from {path}; trace={trace}; body={body}"
            )
        raise PublicDataApiError(f"retry budget exhausted for {path}")

    async def resolve_proxy_wallet(self, address: str) -> tuple[str, dict[str, Any] | None]:
        """Resolve an EOA/profile address to proxy wallet when Gamma exposes one."""
        try:
            payload = await self._get(GAMMA_API_BASE, "/public-profile", {"address": address})
        except PublicDataApiError:
            return address.lower(), None
        if not isinstance(payload, dict):
            return address.lower(), None
        proxy = payload.get("proxyWallet") or payload.get("proxy_wallet")
        if isinstance(proxy, str) and proxy.startswith("0x") and len(proxy) == 42:
            return proxy.lower(), payload
        return address.lower(), payload

    async def user_stats(self, wallet: str) -> dict[str, Any] | None:
        payload = await self._get(DATA_API_BASE, "/v2/user-stats", {"user": wallet})
        if not isinstance(payload, dict):
            raise PublicDataApiError("unexpected user-stats response")
        data = payload.get("data")
        return data if isinstance(data, dict) else None

    async def user_volume(self, wallet: str, *, start: int = 1, end: int = 0) -> dict[str, Any]:
        payload = await self._get(
            DATA_API_BASE,
            "/v2/user-volume",
            {"user": wallet, "start": start, "end": end},
        )
        if not isinstance(payload, dict) or not isinstance(payload.get("data"), dict):
            raise PublicDataApiError("unexpected user-volume response")
        return payload["data"]

    async def user_pnl(self, wallet: str) -> dict[str, Any]:
        payload = await self._get(
            DATA_API_BASE,
            "/v2/user-pnl",
            {"user": wallet, "interval": "all", "fidelity": "1d"},
        )
        if not isinstance(payload, dict) or not isinstance(payload.get("data"), dict):
            raise PublicDataApiError("unexpected user-pnl response")
        return payload["data"]

    async def _paged_rows(
        self,
        path: str,
        params: dict[str, Any],
        *,
        max_pages: int | None,
    ) -> AsyncIterator[dict[str, Any]]:
        page = 0
        cursor: str | None = None
        while True:
            page += 1
            request_params = dict(params)
            if cursor:
                request_params["cursor"] = cursor
            payload = await self._get(DATA_API_BASE, path, request_params)
            if not isinstance(payload, dict):
                raise PublicDataApiError(f"unexpected page response for {path}")
            data = payload.get("data")
            if data is None:
                data = []
            if not isinstance(data, list):
                raise PublicDataApiError(f"expected data list for {path}")
            for row in data:
                if isinstance(row, dict):
                    yield row
            pagination = payload.get("pagination") or {}
            has_more = bool(pagination.get("has_more") or pagination.get("hasMore"))
            cursor_value = pagination.get("next_cursor") or pagination.get("nextCursor")
            if not has_more or not cursor_value:
                break
            if max_pages is not None and page >= max_pages:
                break
            cursor = str(cursor_value)

    async def activities(
        self,
        wallet: str,
        *,
        start: int = 1,
        end: int = 0,
        limit: int = 500,
        max_pages: int | None = None,
    ) -> list[WalletActivity]:
        params = {
            "user": wallet,
            "start": start,
            "end": end,
            "limit": max(1, min(limit, 500)),
            "sort_direction": "DESC",
        }
        rows: list[WalletActivity] = []
        async for raw in self._paged_rows("/v2/activity", params, max_pages=max_pages):
            rows.append(WalletActivity.from_api(wallet, raw))
        return rows

    async def trades(
        self,
        wallet: str,
        *,
        start: int = 1,
        end: int = 0,
        limit: int = 500,
        max_pages: int | None = None,
        include_maker_rows: bool = True,
    ) -> list[WalletTrade]:
        params = {
            "user": wallet,
            "start": start,
            "end": end,
            "limit": max(1, min(limit, 500)),
            "taker_only": not include_maker_rows,
        }
        rows: list[WalletTrade] = []
        async for raw in self._paged_rows("/v2/trades", params, max_pages=max_pages):
            rows.append(WalletTrade.from_api(wallet, raw))
        return rows
