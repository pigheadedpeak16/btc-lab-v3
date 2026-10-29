from __future__ import annotations

from pathlib import Path

import pytest

from btc_v3.features.btc import CausalBtcFeatureEngine, FeatureUnavailable
from btc_v3.feeds.polymarket import DiscoveredMarket
from btc_v3.storage.db import initialize_project_databases
from btc_v3.storage.external_store import ExternalStore

MICROS = 1_000_000


def market() -> DiscoveredMarket:
    return DiscoveredMarket(
        slug="btc-updown-5m-1000",
        event_id=None,
        market_id=None,
        condition_id="0xabc",
        question=None,
        rules_text=None,
        round_start_us=1000 * MICROS,
        round_end_us=1300 * MICROS,
        up_token_id="up",
        down_token_id="down",
        active=True,
        closed=False,
        raw_event={},
        raw_market={},
    )


def seed_store(root: Path, *, decision_us: int = 1240 * MICROS) -> ExternalStore:
    initialize_project_databases(root)
    store = ExternalStore.open_project(root)
    seq = 0
    for sec in range(1100, 1241):
        seq += 1
        price = 86000.0 + (sec - 1100) * 0.5
        store.persist_coinbase_tick(
            source_ts_us=sec * MICROS,
            received_at_us=sec * MICROS + 1000,
            receive_sequence=seq,
            price=f"{price:.6f}",
            raw_payload={},
        )
        store.persist_kraken_tick(
            source_ts_us=sec * MICROS,
            received_at_us=sec * MICROS + 2000,
            receive_sequence=seq,
            price=f"{price + 2.0:.6f}",
            raw_payload={},
        )
        store.persist_settlement_observation(
            source_ts_us=sec * MICROS,
            received_at_us=sec * MICROS + 3000,
            receive_sequence=seq,
            value=f"{price + 1.0:.12f}",
            source="CHAINLINK_BTC_USD_TWAP_60S",
            stream_id="prices.crypto.twap:btcusd:60s",
            raw_payload={},
        )
    store.persist_price_reference(
        slug=market().slug,
        round_start_us=market().round_start_us,
        round_end_us=market().round_end_us,
        price_to_beat="86020.000000000000",
        reference_ts_us=market().round_start_us,
        observed_at_us=1100 * MICROS,
        source="CHAINLINK_BTC_USD_TWAP_60S",
        source_stream="prices.crypto.twap:btcusd:60s",
        mapping_method="EXACT_ROUND_START_SOURCE_TIMESTAMP",
        raw_payload={},
    )
    return store


def test_feature_engine_is_causal_and_computes_expected_fields(tmp_path: Path) -> None:
    decision = 1240 * MICROS
    with seed_store(tmp_path, decision_us=decision) as store:
        engine = CausalBtcFeatureEngine(store)
        f = engine.build(market=market(), decision_time_us=decision)
        assert f.seconds_remaining == 60
        assert f.price_to_beat == pytest.approx(86020.0)
        assert f.coinbase_price < f.kraken_price
        assert f.predictor_price == pytest.approx((f.coinbase_price + f.kraken_price) / 2)
        assert set((5, 10, 15, 30, 60)).issubset(f.returns)
        assert 60 in f.realized_volatility
        assert f.distance_dollars > 0


def test_future_price_reference_is_rejected(tmp_path: Path) -> None:
    initialize_project_databases(tmp_path)
    with ExternalStore.open_project(tmp_path) as store:
        store.persist_price_reference(
            slug=market().slug,
            round_start_us=market().round_start_us,
            round_end_us=market().round_end_us,
            price_to_beat="86000",
            reference_ts_us=market().round_start_us,
            observed_at_us=1241 * MICROS,
            source="CHAINLINK_BTC_USD_TWAP_60S",
            source_stream="x",
            mapping_method="EXACT_ROUND_START_SOURCE_TIMESTAMP",
            raw_payload={},
        )
        engine = CausalBtcFeatureEngine(store)
        with pytest.raises(FeatureUnavailable, match="NO_CAUSAL_PRICE_REFERENCE"):
            engine.build(market=market(), decision_time_us=1240 * MICROS)


def test_sparse_kraken_history_does_not_destroy_volatility(tmp_path: Path) -> None:
    initialize_project_databases(tmp_path)
    decision = 1240 * MICROS
    with ExternalStore.open_project(tmp_path) as store:
        seq = 0
        for sec in range(1160, 1241):
            seq += 1
            price = 86000.0 + (sec - 1160) * 0.4
            store.persist_coinbase_tick(
                source_ts_us=sec * MICROS,
                received_at_us=sec * MICROS + 1000,
                receive_sequence=seq,
                price=f"{price:.6f}",
                raw_payload={},
            )
            # Simulate Kraken's event-driven trade feed being quiet for multi-second stretches.
            if sec % 12 == 0 or sec >= 1239:
                store.persist_kraken_tick(
                    source_ts_us=sec * MICROS,
                    received_at_us=sec * MICROS + 2000,
                    receive_sequence=seq,
                    price=f"{price + 1.5:.6f}",
                    raw_payload={},
                )
            store.persist_settlement_observation(
                source_ts_us=sec * MICROS,
                received_at_us=sec * MICROS + 3000,
                receive_sequence=seq,
                value=f"{price + 0.5:.12f}",
                source="CHAINLINK_BTC_USD_TWAP_60S",
                stream_id="prices.crypto.twap:btcusd:60s",
                raw_payload={},
            )
        store.persist_price_reference(
            slug=market().slug,
            round_start_us=market().round_start_us,
            round_end_us=market().round_end_us,
            price_to_beat="86010",
            reference_ts_us=market().round_start_us,
            observed_at_us=1160 * MICROS,
            source="CHAINLINK_BTC_USD_TWAP_60S",
            source_stream="prices.crypto.twap:btcusd:60s",
            mapping_method="EXACT_ROUND_START_SOURCE_TIMESTAMP",
            raw_payload={},
        )
        engine = CausalBtcFeatureEngine(store)
        features = engine.build(market=market(), decision_time_us=decision)
        assert 60 in features.realized_volatility
        assert features.realized_volatility[60] >= 0


def test_historical_grid_fails_when_all_predictor_sources_are_stale(tmp_path: Path) -> None:
    initialize_project_databases(tmp_path)
    with ExternalStore.open_project(tmp_path) as store:
        store.persist_coinbase_tick(
            source_ts_us=1100 * MICROS,
            received_at_us=1100 * MICROS,
            receive_sequence=1,
            price="86000",
            raw_payload={},
        )
        store.persist_kraken_tick(
            source_ts_us=1100 * MICROS,
            received_at_us=1100 * MICROS,
            receive_sequence=1,
            price="86001",
            raw_payload={},
        )
        engine = CausalBtcFeatureEngine(store, historical_grid_max_age_seconds=15)
        with pytest.raises(FeatureUnavailable, match="MISSING_EXTERNAL_HISTORY"):
            engine.composite_grid(
                start_us=1120 * MICROS,
                end_us=1125 * MICROS,
                decision_us=1125 * MICROS,
            )
