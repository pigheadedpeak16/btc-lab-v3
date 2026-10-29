from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from btc_v3.feeds.polymarket import DiscoveredMarket
from btc_v3.feeds.settlement_reference import (
    AUTH_ENABLED_STATE,
    MISSING_CREDENTIALS_STATE,
    SKIP_REASON,
    AuthenticatedTwapCollector,
    PriceReferenceMapper,
    SettlementObservation,
    SettlementReferencePolicy,
    parse_polymarket_twap_event,
    parse_sdk_twap_event,
    set_reference_policy_status,
)
from btc_v3.storage.db import initialize_project_databases
from btc_v3.storage.external_store import ExternalStore


def market() -> DiscoveredMarket:
    return DiscoveredMarket(
        slug="btc-updown-5m-1791183600",
        event_id="e",
        market_id="m",
        condition_id="c",
        question="q",
        rules_text="rules",
        round_start_us=1_791_183_600_000_000,
        round_end_us=1_791_183_900_000_000,
        up_token_id="u",
        down_token_id="d",
        active=True,
        closed=False,
        raw_event={},
        raw_market={},
    )


def test_twap_update_preserves_high_precision_decimal_string() -> None:
    payload = {
        "topic": "prices.crypto.twap",
        "type": "update",
        "payload": {
            "symbol": "btcusd",
            "timestamp": 1791183600000,
            "value": "86000.123456789012345678",
            "windowSeconds": 60,
        },
    }
    rows = parse_polymarket_twap_event(payload, 1_791_183_600_100_000)
    assert len(rows) == 1
    assert rows[0].source_ts_us == 1_791_183_600_000_000
    assert rows[0].value == "86000.123456789012345678"


def test_twap_parser_rejects_wrong_window() -> None:
    payload = {
        "topic": "prices.crypto.twap",
        "type": "update",
        "payload": {"symbol": "btcusd", "timestamp": 1, "value": "1", "windowSeconds": 30},
    }
    assert parse_polymarket_twap_event(payload, 2) == []


def test_price_reference_maps_only_exact_round_start(tmp_path: Path) -> None:
    paths = initialize_project_databases(tmp_path)
    m = market()
    with ExternalStore(paths["external"]) as store:
        mapper = PriceReferenceMapper(store)
        wrong = SettlementObservation(
            source_ts_us=m.round_start_us - 1,
            received_at_us=m.round_start_us + 100_000,
            value="85999.9",
            source="CHAINLINK_BTC_USD_TWAP_60S",
            stream_id="prices.crypto.twap:btcusd:60s",
            raw_payload={},
        )
        assert mapper.ingest_observation(m, wrong, 1) is False
        assert store.causal_price_reference(m.slug, m.round_start_us + 200_000) is None

        exact = SettlementObservation(
            source_ts_us=m.round_start_us,
            received_at_us=m.round_start_us + 300_000,
            value="86000.123456789012345678",
            source="CHAINLINK_BTC_USD_TWAP_60S",
            stream_id="prices.crypto.twap:btcusd:60s",
            raw_payload={},
        )
        assert mapper.ingest_observation(m, exact, 2) is True
        assert store.causal_price_reference(m.slug, m.round_start_us + 299_999) is None
        ref = store.causal_price_reference(m.slug, m.round_start_us + 300_000)
        assert ref is not None
        assert ref.price_to_beat == exact.value


def test_current_reference_policy_allows_authenticated_read_only(tmp_path: Path) -> None:
    policy = SettlementReferencePolicy()
    assert policy.authenticated_read_only_allowed is True
    assert policy.state == AUTH_ENABLED_STATE
    paths = initialize_project_databases(tmp_path)
    with ExternalStore(paths["external"]) as store:
        set_reference_policy_status(store, policy)
        health = store.current_health()
    ref = health.components["settlement_reference"]
    assert ref.state == AUTH_ENABLED_STATE
    assert ref.details["skip_reason"] == SKIP_REASON
    assert ref.details["real_trading"] == "DISABLED"
    assert health.price_reference_count == 0


@dataclass
class FakePoint:
    timestamp: datetime
    value: Decimal


@dataclass
class FakePayload:
    symbol: str
    window_seconds: int
    data: tuple[FakePoint, ...] = ()
    timestamp: datetime | None = None
    value: Decimal | None = None


@dataclass
class FakeEvent:
    topic: str
    type: str
    payload: FakePayload
    seq: int = 1
    dropped: int = 0


def test_sdk_event_parser_preserves_decimal_and_timestamp() -> None:
    ts = datetime(2026, 10, 5, 7, 0, 0, tzinfo=timezone.utc)
    event = FakeEvent(
        topic="prices.crypto.twap",
        type="update",
        payload=FakePayload(
            symbol="btcusd",
            window_seconds=60,
            timestamp=ts,
            value=Decimal("86000.123456789012345678"),
        ),
    )
    rows = parse_sdk_twap_event(event, 1_791_183_600_500_000)
    assert len(rows) == 1
    assert rows[0].source_ts_us == 1_791_183_600_000_000
    assert rows[0].value == "86000.123456789012345678"


class FakeStream:
    def __init__(self, events: list[FakeEvent]) -> None:
        self.events = events
        self.closed = False

    async def __aenter__(self) -> "FakeStream":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self.closed = True

    def __aiter__(self):
        async def gen():
            for event in self.events:
                yield event
        return gen()

    async def close(self) -> None:
        self.closed = True


class FakeClient:
    def __init__(self, events: list[FakeEvent]) -> None:
        self.events = events
        self.closed = False
        self.subscribe_count = 0

    async def subscribe(self, spec):
        self.subscribe_count += 1
        return FakeStream(self.events)

    async def close(self) -> None:
        self.closed = True


def test_authenticated_collector_maps_round_start_from_history_snapshot(tmp_path: Path) -> None:
    paths = initialize_project_databases(tmp_path)
    m = market()
    point = FakePoint(
        timestamp=datetime.fromtimestamp(m.round_start_us / 1_000_000, tz=timezone.utc),
        value=Decimal("86000.123456789012345678"),
    )
    event = FakeEvent(
        topic="prices.crypto.twap",
        type="subscribe",
        payload=FakePayload(symbol="btcusd", window_seconds=60, data=(point,)),
    )
    fake_client = FakeClient([event])

    async def factory(_private_key):
        return fake_client

    with ExternalStore(paths["external"]) as store:
        collector = AuthenticatedTwapCollector(
            root=tmp_path,
            store=store,
            market=m,
            private_key="not-used-by-injected-factory",
            client_factory=factory,
            spec_factory=lambda: object(),
        )
        asyncio.run(collector.run())
        ref = store.causal_price_reference(m.slug, 2_000_000_000_000_000)
        assert ref is not None
        assert ref.price_to_beat == "86000.123456789012345678"
        assert collector.observation_count == 1
        assert collector.mapped_ptb_count == 1
        health = store.current_health()
        status = health.components["settlement_reference"]
        assert status.state == "STOPPED"
        assert status.details["real_trading"] == "DISABLED"


def test_authenticated_collector_fails_closed_without_private_key(tmp_path: Path) -> None:
    paths = initialize_project_databases(tmp_path)
    with ExternalStore(paths["external"]) as store:
        collector = AuthenticatedTwapCollector(root=tmp_path, store=store, market=market(), private_key=None)
        with pytest.raises(RuntimeError, match="POLYMARKET_PRIVATE_KEY"):
            asyncio.run(collector.run())
        health = store.current_health()
        status = health.components["settlement_reference"]
        assert status.state in {MISSING_CREDENTIALS_STATE, "FATAL"}
        assert health.settlement_observation_count == 0
        assert health.price_reference_count == 0
