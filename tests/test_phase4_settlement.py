from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from btc_v3.features.btc import CausalBtcFeatureEngine
from btc_v3.models.settlement import SettlementModelConfig, TwapSettlementModel
from btc_v3.storage.external_store import ExternalStore
from test_phase4_features import MICROS, market, seed_store


def test_settlement_probability_is_bounded_and_deterministic(tmp_path: Path) -> None:
    decision = 1240 * MICROS
    with seed_store(tmp_path) as store:
        engine = CausalBtcFeatureEngine(store)
        features = engine.build(market=market(), decision_time_us=decision)
        model = TwapSettlementModel(engine, SettlementModelConfig(paths=4000))
        a = model.evaluate(market=market(), decision_time_us=decision, features=features)
        b = model.evaluate(market=market(), decision_time_us=decision, features=features)
        assert 0 <= a.p_up <= 1
        assert a.p_down == pytest.approx(1 - a.p_up)
        assert a.p_up == b.p_up
        assert a.seed == b.seed
        assert 0 <= a.analytical_p_up <= 1


def test_far_above_ptb_has_high_up_probability(tmp_path: Path) -> None:
    decision = 1240 * MICROS
    with seed_store(tmp_path) as store:
        engine = CausalBtcFeatureEngine(store)
        features = engine.build(market=market(), decision_time_us=decision)
        low_sigma_features = replace(features, realized_volatility={60: 1e-6}, price_to_beat=85000.0)
        model = TwapSettlementModel(engine, SettlementModelConfig(paths=3000, volatility_floor=1e-8))
        result = model.evaluate(market=market(), decision_time_us=decision, features=low_sigma_features)
        assert result.p_up > 0.99


def test_critical_down_signal_cannot_force_buy_down(tmp_path: Path) -> None:
    # Phase 4 has no trading action at all. It only returns settlement probabilities.
    decision = 1240 * MICROS
    with seed_store(tmp_path) as store:
        engine = CausalBtcFeatureEngine(store)
        features = engine.build(market=market(), decision_time_us=decision)
        features = replace(
            features,
            predictor_price=86200.0,
            official_twap=86190.0,
            price_to_beat=86000.0,
            realized_volatility={60: 0.00005},
        )
        model = TwapSettlementModel(engine, SettlementModelConfig(paths=3000))
        result = model.evaluate(market=market(), decision_time_us=decision, features=features)
        assert result.p_down < 0.5
        assert not hasattr(result, "action")


def test_more_volatility_increases_remote_crossing_probability(tmp_path: Path) -> None:
    decision = 1240 * MICROS
    with seed_store(tmp_path) as store:
        engine = CausalBtcFeatureEngine(store)
        base = engine.build(market=market(), decision_time_us=decision)
        base = replace(base, predictor_price=86200.0, official_twap=86200.0, price_to_beat=86000.0)
        model = TwapSettlementModel(engine, SettlementModelConfig(paths=12000, volatility_floor=1e-8))
        low = model.evaluate(
            market=market(),
            decision_time_us=decision,
            features=replace(base, realized_volatility={60: 0.00005}),
        )
        high = model.evaluate(
            market=market(),
            decision_time_us=decision,
            features=replace(base, realized_volatility={60: 0.0010}),
        )
        assert high.p_down > low.p_down


def test_tiny_sigma_is_finite(tmp_path: Path) -> None:
    decision = 1240 * MICROS
    with seed_store(tmp_path) as store:
        engine = CausalBtcFeatureEngine(store)
        f = engine.build(market=market(), decision_time_us=decision)
        f = replace(f, realized_volatility={60: 0.0})
        result = TwapSettlementModel(
            engine,
            SettlementModelConfig(paths=1000, volatility_floor=1e-10),
        ).evaluate(market=market(), decision_time_us=decision, features=f)
        assert 0 <= result.p_up <= 1
        assert result.simulated_twap_std >= 0


def test_less_time_reduces_remote_down_probability(tmp_path: Path) -> None:
    decision = 1240 * MICROS
    with seed_store(tmp_path) as store:
        engine = CausalBtcFeatureEngine(store)
        f = engine.build(market=market(), decision_time_us=decision)
        f = replace(
            f,
            predictor_price=86200.0,
            official_twap=86200.0,
            price_to_beat=86000.0,
            realized_volatility={60: 0.0004},
        )
        long_market = market()
        short_market = replace(long_market, round_end_us=1270 * MICROS)
        model = TwapSettlementModel(engine, SettlementModelConfig(paths=16000))
        long_result = model.evaluate(market=long_market, decision_time_us=decision, features=f)
        short_result = model.evaluate(market=short_market, decision_time_us=decision, features=replace(f, seconds_remaining=30.0))
        assert short_result.p_down <= long_result.p_down


def test_monte_carlo_and_analytic_benchmark_are_reasonably_consistent(tmp_path: Path) -> None:
    decision = 1240 * MICROS
    with seed_store(tmp_path) as store:
        engine = CausalBtcFeatureEngine(store)
        f = engine.build(market=market(), decision_time_us=decision)
        f = replace(f, realized_volatility={60: 0.0002})
        result = TwapSettlementModel(engine, SettlementModelConfig(paths=30000)).evaluate(
            market=market(), decision_time_us=decision, features=f
        )
        assert abs(result.p_up - result.analytical_p_up) < 0.12


def test_sparse_secondary_exchange_still_allows_settlement_model(tmp_path: Path) -> None:
    from btc_v3.storage.db import initialize_project_databases

    decision = 1240 * MICROS
    initialize_project_databases(tmp_path)
    with ExternalStore.open_project(tmp_path) as store:
        seq = 0
        for sec in range(1150, 1241):
            seq += 1
            price = 86000.0 + (sec - 1150) * 0.25
            store.persist_coinbase_tick(
                source_ts_us=sec * MICROS,
                received_at_us=sec * MICROS + 1000,
                receive_sequence=seq,
                price=f"{price:.6f}",
                raw_payload={},
            )
            if sec % 11 == 0 or sec >= 1239:
                store.persist_kraken_tick(
                    source_ts_us=sec * MICROS,
                    received_at_us=sec * MICROS + 2000,
                    receive_sequence=seq,
                    price=f"{price + 1.0:.6f}",
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
            observed_at_us=1150 * MICROS,
            source="CHAINLINK_BTC_USD_TWAP_60S",
            source_stream="prices.crypto.twap:btcusd:60s",
            mapping_method="EXACT_ROUND_START_SOURCE_TIMESTAMP",
            raw_payload={},
        )
        engine = CausalBtcFeatureEngine(store)
        features = engine.build(market=market(), decision_time_us=decision)
        result = TwapSettlementModel(engine, SettlementModelConfig(paths=2000)).evaluate(
            market=market(), decision_time_us=decision, features=features
        )
        assert 0 <= result.p_up <= 1
        assert "AVAILABILITY_AWARE" in result.approximation
