from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Iterable
from urllib.parse import quote

import httpx

from ..timeutil import to_epoch_us, utc_now

GAMMA_BASE_URL = "https://gamma-api.polymarket.com"
CLOB_BASE_URL = "https://clob.polymarket.com"
CLOB_MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
BTC_5M_SLUG_PREFIX = "btc-updown-5m-"
ETH_5M_SLUG_PREFIX = "eth-updown-5m-"
ROUND_SECONDS = 300


class MarketDiscoveryError(RuntimeError):
    pass


@dataclass(frozen=True)
class DiscoveredMarket:
    slug: str
    event_id: str | None
    market_id: str | None
    condition_id: str
    question: str | None
    rules_text: str | None
    round_start_us: int
    round_end_us: int
    up_token_id: str
    down_token_id: str
    active: bool
    closed: bool
    raw_event: dict[str, Any]
    raw_market: dict[str, Any]

    @property
    def token_ids(self) -> tuple[str, str]:
        return (self.up_token_id, self.down_token_id)

    def outcome_for_token(self, token_id: str) -> str:
        if token_id == self.up_token_id:
            return "UP"
        if token_id == self.down_token_id:
            return "DOWN"
        raise KeyError(f"token is not part of {self.slug}: {token_id}")

    def phase_at(self, now_us: int) -> str:
        if now_us < self.round_start_us:
            return "UPCOMING"
        if now_us < self.round_end_us:
            return "LIVE"
        return "ENDED"


def btc_5m_round_start_s(now: datetime | None = None) -> int:
    current = (now or utc_now()).astimezone(UTC)
    epoch = int(current.timestamp())
    return (epoch // ROUND_SECONDS) * ROUND_SECONDS


def btc_5m_slug(round_start_s: int) -> str:
    if round_start_s % ROUND_SECONDS != 0:
        raise ValueError("BTC 5m round start must be aligned to a 300-second boundary")
    return f"{BTC_5M_SLUG_PREFIX}{round_start_s}"


def eth_5m_slug(round_start_s: int) -> str:
    if round_start_s % ROUND_SECONDS != 0:
        raise ValueError("ETH 5m round start must be aligned to a 300-second boundary")
    return f"{ETH_5M_SLUG_PREFIX}{round_start_s}"


def _decode_json_list(value: object) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return []
        return decoded if isinstance(decoded, list) else []
    return []


def _lower(value: object) -> str:
    return str(value or "").strip().lower()


def _boolish(value: object, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"true", "1", "yes"}:
        return True
    if text in {"false", "0", "no", ""}:
        return False
    return default


def _find_market(event: dict[str, Any], slug: str) -> dict[str, Any] | None:
    markets = event.get("markets")
    if not isinstance(markets, list):
        return None
    exact = [m for m in markets if isinstance(m, dict) and m.get("slug") == slug]
    if exact:
        return exact[0]
    candidates = [m for m in markets if isinstance(m, dict)]
    return candidates[0] if len(candidates) == 1 else None


def parse_btc_5m_event(event: dict[str, Any], expected_slug: str) -> DiscoveredMarket:
    if not expected_slug.startswith(BTC_5M_SLUG_PREFIX):
        raise MarketDiscoveryError(f"not a BTC 5m slug: {expected_slug}")
    try:
        round_start_s = int(expected_slug.removeprefix(BTC_5M_SLUG_PREFIX))
    except ValueError as exc:
        raise MarketDiscoveryError(f"invalid BTC 5m slug timestamp: {expected_slug}") from exc
    if round_start_s % ROUND_SECONDS != 0:
        raise MarketDiscoveryError(f"misaligned BTC 5m slug: {expected_slug}")

    market = _find_market(event, expected_slug)
    if market is None:
        raise MarketDiscoveryError(f"event has no unique market for slug {expected_slug}")

    market_slug = str(market.get("slug") or event.get("slug") or "")
    if market_slug != expected_slug:
        raise MarketDiscoveryError(
            f"Gamma returned unexpected market slug {market_slug!r} for {expected_slug!r}"
        )

    outcomes = _decode_json_list(market.get("outcomes"))
    token_ids = _decode_json_list(market.get("clobTokenIds"))
    if not token_ids:
        token_ids = _decode_json_list(market.get("clob_token_ids"))
    if len(outcomes) != len(token_ids) or len(token_ids) != 2:
        raise MarketDiscoveryError(
            f"expected exactly two aligned outcomes/token IDs for {expected_slug}; "
            f"got outcomes={outcomes!r}, token_count={len(token_ids)}"
        )

    mapping = {_lower(outcome): str(token_id) for outcome, token_id in zip(outcomes, token_ids)}
    up = mapping.get("up") or mapping.get("yes")
    down = mapping.get("down") or mapping.get("no")
    if not up or not down:
        raise MarketDiscoveryError(
            f"could not map Up/Down token IDs for {expected_slug}; outcomes={outcomes!r}"
        )

    condition_id = str(market.get("conditionId") or market.get("condition_id") or "")
    if not condition_id:
        raise MarketDiscoveryError(f"missing condition ID for {expected_slug}")

    question = market.get("question") or event.get("title")
    question_text = str(question) if question is not None else None
    combined_text = f"{question_text or ''} {event.get('title') or ''}".lower()
    if "btc" not in combined_text and "bitcoin" not in combined_text:
        raise MarketDiscoveryError(f"unexpected non-BTC market metadata for {expected_slug}")

    active = _boolish(market.get("active", event.get("active")), True)
    closed = _boolish(market.get("closed", event.get("closed")), False)
    rules = market.get("description") or event.get("description")

    return DiscoveredMarket(
        slug=expected_slug,
        event_id=str(event.get("id")) if event.get("id") is not None else None,
        market_id=str(market.get("id")) if market.get("id") is not None else None,
        condition_id=condition_id,
        question=question_text,
        rules_text=str(rules) if rules is not None else None,
        round_start_us=round_start_s * 1_000_000,
        round_end_us=(round_start_s + ROUND_SECONDS) * 1_000_000,
        up_token_id=up,
        down_token_id=down,
        active=active,
        closed=closed,
        raw_event=event,
        raw_market=market,
    )


def parse_eth_5m_event(event: dict[str, Any], expected_slug: str) -> DiscoveredMarket:
    if not expected_slug.startswith(ETH_5M_SLUG_PREFIX):
        raise MarketDiscoveryError(f"not a ETH 5m slug: {expected_slug}")
    try:
        round_start_s = int(expected_slug.removeprefix(ETH_5M_SLUG_PREFIX))
    except ValueError as exc:
        raise MarketDiscoveryError(f"invalid ETH 5m slug timestamp: {expected_slug}") from exc
    if round_start_s % ROUND_SECONDS != 0:
        raise MarketDiscoveryError(f"misaligned ETH 5m slug: {expected_slug}")

    market = _find_market(event, expected_slug)
    if market is None:
        raise MarketDiscoveryError(f"event has no unique market for slug {expected_slug}")

    market_slug = str(market.get("slug") or event.get("slug") or "")
    if market_slug != expected_slug:
        raise MarketDiscoveryError(
            f"Gamma returned unexpected market slug {market_slug!r} for {expected_slug!r}"
        )

    outcomes = _decode_json_list(market.get("outcomes"))
    token_ids = _decode_json_list(market.get("clobTokenIds"))
    if not token_ids:
        token_ids = _decode_json_list(market.get("clob_token_ids"))
    if len(outcomes) != len(token_ids) or len(token_ids) != 2:
        raise MarketDiscoveryError(
            f"expected exactly two aligned outcomes/token IDs for {expected_slug}; "
            f"got outcomes={outcomes!r}, token_count={len(token_ids)}"
        )

    mapping = {_lower(outcome): str(token_id) for outcome, token_id in zip(outcomes, token_ids)}
    up = mapping.get("up") or mapping.get("yes")
    down = mapping.get("down") or mapping.get("no")
    if not up or not down:
        raise MarketDiscoveryError(
            f"could not map Up/Down token IDs for {expected_slug}; outcomes={outcomes!r}"
        )

    condition_id = str(market.get("conditionId") or market.get("condition_id") or "")
    if not condition_id:
        raise MarketDiscoveryError(f"missing condition ID for {expected_slug}")

    question = market.get("question") or event.get("title")
    question_text = str(question) if question is not None else None
    combined_text = f"{question_text or ''} {event.get('title') or ''}".lower()
    if "eth" not in combined_text and "ethereum" not in combined_text:
        raise MarketDiscoveryError(f"unexpected non-ETH market metadata for {expected_slug}")

    active = _boolish(market.get("active", event.get("active")), True)
    closed = _boolish(market.get("closed", event.get("closed")), False)
    rules = market.get("description") or event.get("description")

    return DiscoveredMarket(
        slug=expected_slug,
        event_id=str(event.get("id")) if event.get("id") is not None else None,
        market_id=str(market.get("id")) if market.get("id") is not None else None,
        condition_id=condition_id,
        question=question_text,
        rules_text=str(rules) if rules is not None else None,
        round_start_us=round_start_s * 1_000_000,
        round_end_us=(round_start_s + ROUND_SECONDS) * 1_000_000,
        up_token_id=up,
        down_token_id=down,
        active=active,
        closed=closed,
        raw_event=event,
        raw_market=market,
    )


class PolymarketPublicRest:
    """Unauthenticated Gamma/CLOB reader. This class contains no trading methods."""

    def __init__(self, timeout_seconds: float = 10.0, client: httpx.AsyncClient | None = None) -> None:
        self._owned_client = client is None
        self.client = client or httpx.AsyncClient(
            timeout=httpx.Timeout(timeout_seconds),
            headers={"User-Agent": "btc-lab-v3/0.2 research-paper-only"},
            follow_redirects=True,
        )

    async def close(self) -> None:
        if self._owned_client:
            await self.client.aclose()

    async def __aenter__(self) -> "PolymarketPublicRest":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def get_event_by_slug(self, slug: str) -> dict[str, Any] | None:
        url = f"{GAMMA_BASE_URL}/events/slug/{quote(slug, safe='')}"
        response = await self.client.get(url)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise MarketDiscoveryError(f"unexpected Gamma payload type for {slug}: {type(payload)!r}")
        return payload

    async def get_order_book(self, token_id: str) -> dict[str, Any]:
        response = await self.client.get(f"{CLOB_BASE_URL}/book", params={"token_id": token_id})
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise RuntimeError(f"unexpected CLOB book payload type: {type(payload)!r}")
        return payload

    async def get_clob_market_info(self, condition_id: str) -> dict[str, Any]:
        """Return current public CLOB market metadata for a condition.

        Phase 5.9 uses the ``fd`` block, when present, to snapshot the market's
        actual fee model instead of assuming that one global fee coefficient is
        permanent.  This is read-only public metadata; failures are handled by
        the forward lab with a conservative configured fallback.
        """
        response = await self.client.get(
            f"{CLOB_BASE_URL}/clob-markets/{quote(condition_id, safe='')}"
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise RuntimeError(f"unexpected CLOB market-info payload type: {type(payload)!r}")
        return payload

    async def discover_btc_5m(
        self,
        *,
        now: datetime | None = None,
        candidate_offsets: Iterable[int] = (0, 1, -1, 2, -2, 3, -3),
    ) -> DiscoveredMarket:
        current = (now or utc_now()).astimezone(UTC)
        now_us = to_epoch_us(current)
        anchor = btc_5m_round_start_s(current)
        found: list[DiscoveredMarket] = []
        errors: list[str] = []
        for offset in candidate_offsets:
            start_s = anchor + int(offset) * ROUND_SECONDS
            slug = btc_5m_slug(start_s)
            try:
                payload = await self.get_event_by_slug(slug)
                if payload is None:
                    continue
                market=parse_btc_5m_event(payload,slug)
                if market.round_start_us<=now_us<market.round_end_us and not market.closed:
                    return market
                found.append(market)
            except (MarketDiscoveryError, httpx.HTTPError) as exc:
                errors.append(f"{slug}: {exc}")

        live = [m for m in found if m.round_start_us <= now_us < m.round_end_us and not m.closed]
        if live:
            return sorted(live, key=lambda m: m.round_start_us)[-1]

        upcoming = [m for m in found if m.round_start_us > now_us and not m.closed]
        if upcoming:
            return min(upcoming, key=lambda m: m.round_start_us)

        recent = [m for m in found if m.round_end_us <= now_us]
        if recent:
            return max(recent, key=lambda m: m.round_end_us)

        detail = "; ".join(errors[-3:]) if errors else "no candidate events found"
        raise MarketDiscoveryError(f"unable to discover a BTC Up/Down 5m market: {detail}")

    async def discover_eth_5m(
        self,
        *,
        now: datetime | None = None,
        candidate_offsets: Iterable[int] = (0, 1, -1, 2, -2, 3, -3),
    ) -> DiscoveredMarket:
        current = (now or utc_now()).astimezone(UTC)
        now_us = to_epoch_us(current)
        anchor = btc_5m_round_start_s(current)
        found: list[DiscoveredMarket] = []
        errors: list[str] = []
        for offset in candidate_offsets:
            start_s = anchor + int(offset) * ROUND_SECONDS
            slug = eth_5m_slug(start_s)
            try:
                payload = await self.get_event_by_slug(slug)
                if payload is None:
                    continue
                found.append(parse_eth_5m_event(payload, slug))
            except (MarketDiscoveryError, httpx.HTTPError) as exc:
                errors.append(f"{slug}: {exc}")
        live = [m for m in found if m.round_start_us <= now_us < m.round_end_us and not m.closed]
        if live:
            return sorted(live, key=lambda m: m.round_start_us)[-1]
        upcoming = [m for m in found if m.round_start_us > now_us and not m.closed]
        if upcoming:
            return min(upcoming, key=lambda m: m.round_start_us)
        recent = [m for m in found if m.round_end_us <= now_us]
        if recent:
            return max(recent, key=lambda m: m.round_end_us)
        detail = "; ".join(errors[-3:]) if errors else "no candidate events found"
        raise MarketDiscoveryError(f"unable to discover an ETH Up/Down 5m market: {detail}")
