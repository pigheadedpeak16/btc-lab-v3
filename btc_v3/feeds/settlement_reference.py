from __future__ import annotations

import asyncio
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable

from ..feeds.polymarket import DiscoveredMarket
from ..storage.external_store import ExternalStore, positive_price
from ..timeutil import now_epoch_us, parse_source_timestamp_us
from .official_resolution import OfficialResolutionReader

COMPONENT = "settlement_reference"
OFFICIAL_SOURCE = "CHAINLINK_BTC_USD_TWAP_60S"
POLYMARKET_STREAM = "prices.crypto.twap:btcusd:60s"
AUTH_ENABLED_STATE = "AUTH_ENABLED"
MISSING_CREDENTIALS_STATE = "MISSING_PRIVATE_KEY"
SKIP_REASON = "NO_CAUSAL_PRICE_REFERENCE"
PRIVATE_KEY_ENV = "POLYMARKET_PRIVATE_KEY"


@dataclass(frozen=True)
class SettlementReferencePolicy:
    source: str = OFFICIAL_SOURCE
    stream: str = POLYMARKET_STREAM
    authenticated_read_only_allowed: bool = True
    state: str = AUTH_ENABLED_STATE
    reason: str = (
        "Authenticated read-only reference-price access is permitted. The project uses authentication "
        "only to subscribe to the official 60-second BTC/USD TWAP feed; no order placement, cancellation, "
        "deposit, withdrawal, or transaction execution path is implemented."
    )


@dataclass(frozen=True)
class SettlementObservation:
    source_ts_us: int | None
    received_at_us: int
    value: str
    source: str
    stream_id: str
    raw_payload: dict[str, Any]


def _dt_to_epoch_us(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return int(value.timestamp() * 1_000_000)
    return parse_source_timestamp_us(value)


def parse_polymarket_twap_event(
    message: dict[str, Any], received_at_us: int
) -> list[SettlementObservation]:
    """Parse documented prices.crypto.twap payloads and preserve exact decimal strings."""
    if message.get("topic") != "prices.crypto.twap":
        return []
    event_type = message.get("type")
    payload = message.get("payload")
    if not isinstance(payload, dict) or payload.get("symbol") != "btcusd":
        return []
    window = payload.get("windowSeconds", payload.get("window_seconds"))
    if window is not None and int(window) != 60:
        return []
    if event_type == "update":
        value = payload.get("value")
        if value is None:
            return []
        return [
            SettlementObservation(
                source_ts_us=_dt_to_epoch_us(payload.get("timestamp")),
                received_at_us=received_at_us,
                value=str(value),
                source=OFFICIAL_SOURCE,
                stream_id=POLYMARKET_STREAM,
                raw_payload=message,
            )
        ]
    if event_type == "subscribe":
        result: list[SettlementObservation] = []
        for point in payload.get("data") or []:
            if not isinstance(point, dict) or point.get("value") is None:
                continue
            result.append(
                SettlementObservation(
                    source_ts_us=_dt_to_epoch_us(point.get("timestamp")),
                    received_at_us=received_at_us,
                    value=str(point["value"]),
                    source=OFFICIAL_SOURCE,
                    stream_id=POLYMARKET_STREAM,
                    raw_payload={"envelope": message, "point": point},
                )
            )
        return result
    return []


def _sdk_event_to_dict(event: Any) -> dict[str, Any]:
    payload = getattr(event, "payload", None)
    event_type = getattr(event, "type", None)
    topic = getattr(event, "topic", None)
    envelope: dict[str, Any] = {
        "topic": str(topic) if topic is not None else None,
        "type": str(event_type) if event_type is not None else None,
    }
    sequence = getattr(event, "seq", None)
    dropped = getattr(event, "dropped", None)
    if sequence is not None:
        envelope["seq"] = sequence
    if dropped is not None:
        envelope["dropped"] = dropped

    if payload is None:
        envelope["payload"] = None
        return envelope

    symbol = getattr(payload, "symbol", None)
    window = getattr(payload, "window_seconds", None)
    body: dict[str, Any] = {
        "symbol": str(symbol) if symbol is not None else None,
        "window_seconds": window,
    }
    if event_type == "subscribe":
        data = []
        for point in getattr(payload, "data", ()) or ():
            data.append(
                {
                    "timestamp": getattr(point, "timestamp", None),
                    "value": str(getattr(point, "value", "")),
                }
            )
        body["data"] = data
    else:
        body["timestamp"] = getattr(payload, "timestamp", None)
        value = getattr(payload, "value", None)
        body["value"] = str(value) if value is not None else None
    envelope["payload"] = body
    return envelope


def parse_sdk_twap_event(event: Any, received_at_us: int) -> list[SettlementObservation]:
    """Normalize official polymarket-client TWAP event objects into project observations."""
    message = _sdk_event_to_dict(event)
    payload = message.get("payload")
    if isinstance(payload, dict):
        for key in ("timestamp",):
            value = payload.get(key)
            if isinstance(value, datetime):
                payload[key] = value.isoformat()
        data = payload.get("data")
        if isinstance(data, list):
            for point in data:
                if isinstance(point, dict) and isinstance(point.get("timestamp"), datetime):
                    point["timestamp"] = point["timestamp"].isoformat()
    return parse_polymarket_twap_event(message, received_at_us)


def set_reference_policy_status(store: ExternalStore, policy: SettlementReferencePolicy | None = None) -> None:
    policy = policy or SettlementReferencePolicy()
    store.set_status(
        component=COMPONENT,
        session_id=None,
        state=policy.state,
        updated_at_us=now_epoch_us(),
        last_message_at_us=None,
        last_tick_at_us=None,
        reconnect_count=0,
        error_count=0,
        details={
            "source": policy.source,
            "stream": policy.stream,
            "authenticated_read_only_allowed": policy.authenticated_read_only_allowed,
            "reason": policy.reason,
            "skip_reason": SKIP_REASON,
            "private_key_env_present": bool(os.environ.get(PRIVATE_KEY_ENV)),
            "real_trading": "DISABLED",
        },
    )


class PriceReferenceMapper:
    """Maps only exact round-boundary official TWAP observations to Price to Beat."""

    def __init__(self, store: ExternalStore) -> None:
        self.store = store

    def ingest_observation(self, market: DiscoveredMarket, observation: SettlementObservation, sequence: int) -> bool:
        if (not positive_price(observation.value) or observation.source != OFFICIAL_SOURCE
                or observation.stream_id != POLYMARKET_STREAM or observation.source_ts_us is None
                or observation.source_ts_us > observation.received_at_us):
            raise ValueError("INVALID_SETTLEMENT_OBSERVATION")
        self.store.persist_settlement_observation(
            source_ts_us=observation.source_ts_us,
            received_at_us=observation.received_at_us,
            receive_sequence=sequence,
            value=observation.value,
            source=observation.source,
            stream_id=observation.stream_id,
            raw_payload=observation.raw_payload,
        )
        if observation.source_ts_us != market.round_start_us:
            return False
        self.store.persist_price_reference(
            slug=market.slug,
            round_start_us=market.round_start_us,
            round_end_us=market.round_end_us,
            price_to_beat=observation.value,
            reference_ts_us=observation.source_ts_us,
            observed_at_us=observation.received_at_us,
            source=observation.source,
            source_stream=observation.stream_id,
            mapping_method="EXACT_ROUND_START_SOURCE_TIMESTAMP",
            raw_payload=observation.raw_payload,
        )
        return True

    def map_existing_exact(self, market: DiscoveredMarket, decision_time_us: int) -> bool:
        existing = self.store.price_reference_exact(
            slug=market.slug,
            reference_ts_us=market.round_start_us,
            decision_time_us=decision_time_us,
        )
        if existing is not None:
            return True
        row = self.store.settlement_observation_exact(
            source_ts_us=market.round_start_us,
            decision_time_us=decision_time_us,
        )
        if row is None:
            return False
        source_ts_us, observed_at_us, value, source, stream_id, raw_payload = row
        self.store.persist_price_reference(
            slug=market.slug,
            round_start_us=market.round_start_us,
            round_end_us=market.round_end_us,
            price_to_beat=value,
            reference_ts_us=source_ts_us,
            observed_at_us=observed_at_us,
            source=source,
            source_stream=stream_id,
            mapping_method="EXACT_ROUND_START_SOURCE_TIMESTAMP",
            raw_payload=raw_payload,
        )
        return True

    def ingest_many(
        self,
        market: DiscoveredMarket,
        observations: Iterable[SettlementObservation],
        *,
        start_sequence: int = 0,
    ) -> int:
        mapped = 0
        sequence = start_sequence
        for observation in observations:
            sequence += 1
            mapped += int(self.ingest_observation(market, observation, sequence))
        return mapped


class AuthenticatedTwapCollector:
    """Authenticated read-only collector for Polymarket's official 60-second BTC/USD TWAP.

    The SDK secure client is created with wallet=signer_address, forcing EOA mode so client
    creation cannot deploy a Deposit Wallet. This collector calls subscribe() only; it has no
    order, cancellation, deposit, withdrawal, or transaction methods.
    """

    def __init__(
        self,
        *,
        root: Path,
        store: ExternalStore,
        market: DiscoveredMarket,
        private_key: str | None = None,
        client_factory: Any | None = None,
        spec_factory: Any | None = None,
    ) -> None:
        self.root = root
        self.store = store
        self.market = market
        self.private_key = private_key or os.environ.get(PRIVATE_KEY_ENV)
        self.client_factory = client_factory
        self.spec_factory = spec_factory
        self.session_id = str(uuid.uuid4())
        self.receive_sequence = 0
        self.observation_count = 0
        self.mapped_ptb_count = 0
        self.error_count = 0
        self.last_message_at_us: int | None = None
        self.last_tick_at_us: int | None = None
        self._stop_requested = asyncio.Event()
        self._active_stream: Any | None = None

    def _status(self, state: str, **extra: object) -> None:
        self.store.set_status(
            component=COMPONENT,
            session_id=self.session_id,
            state=state,
            updated_at_us=now_epoch_us(),
            last_message_at_us=self.last_message_at_us,
            last_tick_at_us=self.last_tick_at_us,
            reconnect_count=0,
            error_count=self.error_count,
            details={
                "source": OFFICIAL_SOURCE,
                "stream": POLYMARKET_STREAM,
                "slug": self.market.slug,
                "observation_count": self.observation_count,
                "mapped_ptb_count": self.mapped_ptb_count,
                "mapping_counter_scope": "INLINE_INITIAL_MARKET_ONLY; cached references mapped by other consumers are not counted",
                "authentication": "CLOB_L1_L2_READ_ONLY",
                "private_key_stored": False,
                "real_trading": "DISABLED",
                **extra,
            },
        )

    async def request_stop(self) -> None:
        self._stop_requested.set()
        stream = self._active_stream
        if stream is not None:
            try:
                await stream.close()
            except Exception:
                pass

    async def run_for(self, seconds: float) -> None:
        task = asyncio.create_task(self.run())
        try:
            await asyncio.sleep(seconds)
        finally:
            await self.request_stop()
            await task

    async def _make_client_and_spec(self) -> tuple[Any, Any]:
        if self.client_factory is not None:
            client = await self.client_factory(self.private_key)
            spec = self.spec_factory() if self.spec_factory is not None else object()
            return client, spec
        if not self.private_key:
            raise RuntimeError(
                f"{PRIVATE_KEY_ENV} is not set. Use the provided PowerShell secure prompt; do not paste the key into chat."
            )
        from eth_account import Account
        from polymarket import AsyncSecureClient
        from polymarket.streams import CryptoTwapPriceSpec

        signer_address = Account.from_key(self.private_key).address
        client = await AsyncSecureClient.create(
            private_key=self.private_key,
            wallet=signer_address,
        )
        return client, CryptoTwapPriceSpec(symbols=["btcusd"])

    async def run(self) -> None:
        started = now_epoch_us()
        self.store.start_session(self.session_id, COMPONENT, started)
        fatal = False
        client: Any | None = None
        try:
            if not self.private_key:
                self._status(MISSING_CREDENTIALS_STATE, skip_reason=SKIP_REASON)
                raise RuntimeError(
                    f"{PRIVATE_KEY_ENV} is required for the current authenticated TWAP stream."
                )
            self._status("AUTHENTICATING")
            client, spec = await self._make_client_and_spec()
            self._status("SUBSCRIBING")
            stream = await client.subscribe(spec)
            self._active_stream = stream
            mapper = PriceReferenceMapper(self.store)
            self._status("RUNNING")
            async with stream:
                async for event in stream:
                    if self._stop_requested.is_set():
                        break
                    received_at_us = now_epoch_us()
                    self.last_message_at_us = received_at_us
                    observations = parse_sdk_twap_event(event, received_at_us)
                    for observation in observations:
                        self.receive_sequence += 1
                        self.observation_count += 1
                        self.last_tick_at_us = received_at_us
                        if mapper.ingest_observation(self.market, observation, self.receive_sequence):
                            self.mapped_ptb_count += 1
                    self.store.mark_healthy(self.session_id, received_at_us)
                    self._status("RUNNING")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.error_count += 1
            self.store.bump_error(self.session_id)
            if not self._stop_requested.is_set():
                fatal = True
                self._status("FATAL", exception=repr(exc), skip_reason=SKIP_REASON)
                raise
        finally:
            self._active_stream = None
            if client is not None:
                try:
                    await client.close()
                except Exception:
                    pass
            if not fatal:
                self._status("STOPPED")
            self.store.finish_session(self.session_id, now_epoch_us(), fatal=fatal)
