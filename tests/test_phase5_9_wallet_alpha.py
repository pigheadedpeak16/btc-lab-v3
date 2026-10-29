from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from btc_v3.features.btc import CausalBtcFeatureEngine, FeatureUnavailable
from btc_v3.feeds.polymarket import DiscoveredMarket
from btc_v3.forensics.forward_lab import (
    ForwardLabConfig,
    ForwardPaperEngine,
    MakerOrder,
    STRATEGY_8,
    STRATEGY_9_EARLY_LARGE,
    STRATEGY_9_FIRST2,
    STRATEGY_9_FIRST3,
    STRATEGY_9_ONE_SIDED,
    crypto_taker_fee,
    verify_phase590_config,
)
from btc_v3.forensics.forward_store import ForwardWalletStore
from btc_v3.forensics.models import WalletTrade
from btc_v3.storage.db import initialize_project_databases
from btc_v3.storage.external_store import ExternalStore
from btc_v3.storage.market_store import MarketStore

MICROS = 1_000_000


def _market() -> DiscoveredMarket:
    return DiscoveredMarket(
        slug="btc-updown-5m-1000",
        event_id="1",
        market_id="m1",
        condition_id="0xabc",
        question="Bitcoin Up or Down",
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


def _trade(*, outcome: str, size: str = "48", price: str = "0.35", tx: str = "0x1") -> WalletTrade:
    token = "up" if outcome.upper() == "UP" else "down"
    return WalletTrade.from_api(
        "0x" + "1" * 40,
        {
            "timestamp": 1001,
            "event_slug": _market().slug,
            "token_id": token,
            "side": "BUY",
            "outcome": outcome.title(),
            "price": price,
            "size": size,
            "transaction_hash": tx,
        },
    )


def _save_signal(store: ForwardWalletStore, *, sid: str, signal_id: str, row: WalletTrade, seen_us: int) -> None:
    store.save_signal(
        signal_id=signal_id,
        session_id=sid,
        economic_key=signal_id,
        source="trades",
        source_first_seen_us=seen_us,
        trade_ts_us=row.timestamp_us,
        slug=row.slug,
        token_id=row.token_id,
        side=row.side,
        outcome=row.outcome,
        target_price=str(row.price),
        target_size=str(row.size),
        transaction_hash=row.transaction_hash,
        publication_lag_lower_us=0,
        publication_lag_upper_us=0,
        detection_sample_id=None,
        raw_json=row.raw_json,
    )


def test_phase590_verifier_passes() -> None:
    assert all(verify_phase590_config().values())


def test_phase590_schema_adds_counterfactual_decisions(tmp_path: Path) -> None:
    initialize_project_databases(tmp_path)
    with ForwardWalletStore.open_project(tmp_path) as store:
        names = {
            str(r[0])
            for r in store.conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
    assert "wallet_signal_strategy_decisions" in names


def test_wallet_sequence_filters_are_causal_and_frozen(tmp_path: Path) -> None:
    initialize_project_databases(tmp_path)
    market = _market()
    with MarketStore.open_project(tmp_path) as market_store, ForwardWalletStore.open_project(tmp_path) as store:
        market_store.persist_market(market, 1000 * MICROS)
        sid = "s590"
        store.start_session(session_id=sid, wallet="0x" + "1" * 40, started_at_us=1000 * MICROS, config={})
        engine = ForwardPaperEngine(session_id=sid, config=ForwardLabConfig(), store=store)

        rows = [
            _trade(outcome="UP", tx="0x1"),
            _trade(outcome="UP", tx="0x2"),
            _trade(outcome="DOWN", tx="0x3"),
            _trade(outcome="DOWN", tx="0x4"),
        ]
        decisions = []
        for i, row in enumerate(rows, start=1):
            signal_id = f"sig{i}"
            seen_us = (1010 + i) * MICROS  # >180s remaining and size=48.
            _save_signal(store, sid=sid, signal_id=signal_id, row=row, seen_us=seen_us)
            decisions.append(
                engine.schedule_phase590_wallet_variants(
                    signal_id=signal_id,
                    row=row,
                    first_seen_us=seen_us,
                    lower_lag_us=0,
                    market_store=market_store,
                )
            )

        assert decisions[0][STRATEGY_9_FIRST2] == "SCHEDULED"
        assert decisions[1][STRATEGY_9_FIRST2] == "SCHEDULED"
        assert decisions[2][STRATEGY_9_FIRST2] == "AFTER_FIRST2"
        assert decisions[2][STRATEGY_9_FIRST3] == "SCHEDULED"
        assert decisions[3][STRATEGY_9_FIRST3] == "AFTER_FIRST3"
        assert decisions[0][STRATEGY_9_ONE_SIDED] == "SCHEDULED"
        assert decisions[1][STRATEGY_9_ONE_SIDED] == "SCHEDULED"
        assert decisions[2][STRATEGY_9_ONE_SIDED] == "TARGET_ENTERED_TWO_SIDED_STATE"
        assert decisions[3][STRATEGY_9_ONE_SIDED] == "TARGET_ENTERED_TWO_SIDED_STATE"
        assert decisions[0][STRATEGY_9_EARLY_LARGE] == "SCHEDULED"


def test_s8_rejects_aggressor_received_at_order_creation(tmp_path: Path) -> None:
    initialize_project_databases(tmp_path)
    market = _market()
    with MarketStore.open_project(tmp_path) as market_store, ForwardWalletStore.open_project(tmp_path) as store:
        market_store.persist_market(market, 1000 * MICROS)
        sid = "s8audit"
        store.start_session(session_id=sid, wallet="0x" + "1" * 40, started_at_us=1000 * MICROS, config={})
        engine = ForwardPaperEngine(session_id=sid, config=ForwardLabConfig(), store=store)
        pos = engine.position(STRATEGY_8, market)
        created = 1100 * MICROS
        order = MakerOrder(
            "o1", market.slug, market.up_token_id, "UP", Decimal("0.35"), Decimal("48"),
            Decimal("48"), Decimal("0"), created
        )
        engine.maker_orders[f"{market.slug}|UP|0.35"] = order
        store.save_order(
            order_id="o1", session_id=sid, strategy=STRATEGY_8, signal_id=None,
            slug=market.slug, token_id=market.up_token_id, outcome="UP", side="BUY",
            style="MAKER_GTC_PAPER", price_limit="0.35", shares="48", created_at_us=created,
            scheduled_at_us=created, status="OPEN", reason=None, metadata={},
        )
        market_store.persist_trade(
            market,
            token_id=market.up_token_id,
            source_ts_us=created,
            received_at_us=created,
            price="0.35",
            size="48",
            taker_side="SELL",
            source_trade_id="equal-time",
            raw_payload={},
        )
        assert engine.process_market_trades(market_store, now_us=created) == 0
        assert pos.up_shares == 0

        market_store.persist_trade(
            market,
            token_id=market.up_token_id,
            source_ts_us=created + 1,
            received_at_us=created + 1,
            price="0.35",
            size="48",
            taker_side="SELL",
            source_trade_id="later",
            raw_payload={},
        )
        assert engine.process_market_trades(market_store, now_us=created + 1) == 1
        assert pos.up_shares == Decimal("48")


def test_single_source_fallback_preserves_causality_and_marks_source(tmp_path: Path) -> None:
    initialize_project_databases(tmp_path)
    market = _market()
    decision = 1240 * MICROS
    with ExternalStore.open_project(tmp_path) as store:
        seq = 0
        for sec in range(1160, 1241):
            seq += 1
            price = 86000 + (sec - 1160) * 0.5
            store.persist_coinbase_tick(
                source_ts_us=sec * MICROS,
                received_at_us=sec * MICROS + 1000,
                receive_sequence=seq,
                price=str(price),
                raw_payload={},
            )
            # Kraken exists historically but is deliberately stale at decision time.
            if sec <= 1230:
                store.persist_kraken_tick(
                    source_ts_us=sec * MICROS,
                    received_at_us=sec * MICROS + 2000,
                    receive_sequence=seq,
                    price=str(price + 1),
                    raw_payload={},
                )
            store.persist_settlement_observation(
                source_ts_us=sec * MICROS,
                received_at_us=sec * MICROS + 3000,
                receive_sequence=seq,
                value=str(price + 0.5),
                source="CHAINLINK_BTC_USD_TWAP_60S",
                stream_id="prices.crypto.twap:btcusd:60s",
                raw_payload={},
            )
        store.persist_price_reference(
            slug=market.slug,
            round_start_us=market.round_start_us,
            round_end_us=market.round_end_us,
            price_to_beat="86010",
            reference_ts_us=market.round_start_us,
            observed_at_us=1160 * MICROS,
            source="CHAINLINK_BTC_USD_TWAP_60S",
            source_stream="prices.crypto.twap:btcusd:60s",
            mapping_method="EXACT_ROUND_START_SOURCE_TIMESTAMP",
            raw_payload={},
        )
        engine = CausalBtcFeatureEngine(store)
        with pytest.raises(FeatureUnavailable, match="STALE_KRAKEN_HISTORY"):
            engine.build(market=market, decision_time_us=decision)
        features = engine.build_available_sources(market=market, decision_time_us=decision)
        assert features.current_sources == ("coinbase",)
        assert features.predictor_price == features.coinbase_price


def test_fee_schedule_round_trip(tmp_path: Path) -> None:
    initialize_project_databases(tmp_path)
    with MarketStore.open_project(tmp_path) as store:
        store.persist_fee_schedule(
            slug=_market().slug,
            observed_at_us=123,
            rate="0.07",
            exponent=1,
            source="test",
            raw_payload={"fd": {"r": 0.07, "e": 1}},
        )
        row = store.latest_fee_schedule(_market().slug)
        assert row is not None
        assert row[0] == Decimal("0.07")
        assert row[1] == 1


def test_generalized_fee_exponent_matches_current_sdk_formula() -> None:
    fee = crypto_taker_fee(
        shares=Decimal("100"),
        price=Decimal("0.5"),
        rate=Decimal("0.25"),
        exponent=2,
    )
    assert fee == Decimal("1.56250")


def test_phase592_model_book_arms_and_router_are_pre_registered(tmp_path: Path) -> None:
    from btc_v3.forensics.forward_lab import (
        STRATEGY_9_BOOK_MODEL30,
        STRATEGY_9_BOOK_MODEL40,
        STRATEGY_9_FIRST3_LARGE_DET7_5,
        STRATEGY_9_LARGE_DET7_5,
        STRATEGY_9_ROUTER_A,
    )
    initialize_project_databases(tmp_path)
    market = _market()
    with MarketStore.open_project(tmp_path) as market_store, ForwardWalletStore.open_project(tmp_path) as store:
        market_store.persist_market(market, 1000 * MICROS)
        sid = "s592-model"
        store.start_session(session_id=sid, wallet="0x" + "1" * 40, started_at_us=1000 * MICROS, config={})
        engine = ForwardPaperEngine(session_id=sid, config=ForwardLabConfig(), store=store)
        row = _trade(outcome="UP", size="48", price="0.35", tx="0x592")
        seen_us = 1011 * MICROS
        _save_signal(store, sid=sid, signal_id="sig592", row=row, seen_us=seen_us)
        decisions = engine.schedule_phase590_wallet_variants(
            signal_id="sig592",
            row=row,
            first_seen_us=seen_us,
            lower_lag_us=0,
            market_store=market_store,
            detection_p_up_mc=0.35,
        )
        assert decisions[STRATEGY_9_BOOK_MODEL30] == "SCHEDULED"
        assert decisions[STRATEGY_9_BOOK_MODEL40] == "MODEL_TARGET_PROB_BELOW_0_40"
        assert decisions[STRATEGY_9_FIRST3_LARGE_DET7_5] == "SCHEDULED"
        assert decisions[STRATEGY_9_LARGE_DET7_5] == "SCHEDULED"
        assert decisions[STRATEGY_9_ROUTER_A] == "SCHEDULED"
        router = next(p for p in engine.pending_copies if p.strategy == STRATEGY_9_ROUTER_A)
        assert router.router_stage == "SPECIALIST_PROBE"
        assert router.model_target_probability == pytest.approx(0.35)
        assert router.scheduled_at_us == seen_us + 25_000


def test_phase592_down_model_probability_is_inverted(tmp_path: Path) -> None:
    from btc_v3.forensics.forward_lab import STRATEGY_9_BOOK_MODEL30, STRATEGY_9_BOOK_MODEL40
    initialize_project_databases(tmp_path)
    market = _market()
    with MarketStore.open_project(tmp_path) as market_store, ForwardWalletStore.open_project(tmp_path) as store:
        market_store.persist_market(market, 1000 * MICROS)
        sid = "s592-down"
        store.start_session(session_id=sid, wallet="0x" + "1" * 40, started_at_us=1000 * MICROS, config={})
        engine = ForwardPaperEngine(session_id=sid, config=ForwardLabConfig(), store=store)
        row = _trade(outcome="DOWN", size="5", price="0.35", tx="0x593")
        seen_us = 1011 * MICROS
        _save_signal(store, sid=sid, signal_id="sig593", row=row, seen_us=seen_us)
        decisions = engine.schedule_phase590_wallet_variants(
            signal_id="sig593",
            row=row,
            first_seen_us=seen_us,
            lower_lag_us=0,
            market_store=market_store,
            detection_p_up_mc=0.75,
        )
        # DOWN target probability is 1 - P(UP) = 0.25.
        assert decisions[STRATEGY_9_BOOK_MODEL30] == "MODEL_TARGET_PROB_BELOW_0_30"
        assert decisions[STRATEGY_9_BOOK_MODEL40] == "MODEL_TARGET_PROB_BELOW_0_40"


def test_phase592_router_specialist_probe_respects_7_5c_cap(tmp_path: Path) -> None:
    from btc_v3.forensics.forward_lab import PendingCopy, STRATEGY_9_ROUTER_A
    from btc_v3.state.book_state import OrderBookState

    initialize_project_databases(tmp_path)
    with ForwardWalletStore.open_project(tmp_path) as store:
        sid = "s592-router-probe"
        store.start_session(session_id=sid, wallet="0x" + "1" * 40, started_at_us=1000 * MICROS, config={})
        engine = ForwardPaperEngine(session_id=sid, config=ForwardLabConfig(), store=store)
        now_us = 1011 * MICROS + 25_000
        up = OrderBookState(slug=_market().slug, token_id="up", outcome="UP")
        up.apply_snapshot(
            bids=[{"price": "0.39", "size": "100"}],
            asks=[{"price": "0.40", "size": "100"}],
            received_at_us=now_us,
            server_ts_us=now_us,
            book_hash="h",
        )
        class Collector:
            states = {"up": up}
        pending = PendingCopy(
            order_id="r1", strategy=STRATEGY_9_ROUTER_A, signal_id="sig", slug=_market().slug,
            token_id="up", outcome="UP", side="BUY", target_price=Decimal("0.35"),
            target_size=Decimal("48"), lower_lag_us=0, created_at_us=1011 * MICROS,
            scheduled_at_us=now_us, model_target_probability=0.50, router_stage="SPECIALIST_PROBE",
        )
        allowed, reason, ctx = engine._router_specialist_probe(pending, collector=Collector(), now_us=now_us)
        assert allowed is True
        assert reason == "ROUTER_SPECIALIST_EXECUTE"
        assert ctx["deterioration"] == "0.05"

        up.asks = {Decimal("0.44"): Decimal("100")}
        allowed, reason, ctx = engine._router_specialist_probe(pending, collector=Collector(), now_us=now_us)
        assert allowed is False
        assert reason == "ROUTER_SPECIALIST_PRICE_DETERIORATED"
        assert ctx["deterioration"] == "0.09"


def test_phase592_router_falls_back_to_book_when_specialist_price_deteriorates(tmp_path: Path) -> None:
    from btc_v3.forensics.forward_lab import STRATEGY_9_ROUTER_A
    from btc_v3.state.book_state import OrderBookState

    initialize_project_databases(tmp_path)
    market = _market()
    with MarketStore.open_project(tmp_path) as market_store, ForwardWalletStore.open_project(tmp_path) as store:
        market_store.persist_market(market, 1000 * MICROS)
        sid = "s592-router-fallback"
        store.start_session(session_id=sid, wallet="0x" + "1" * 40, started_at_us=1000 * MICROS, config={})
        engine = ForwardPaperEngine(session_id=sid, config=ForwardLabConfig(), store=store)
        row = _trade(outcome="UP", size="48", price="0.35", tx="0x594")
        seen_us = 1011 * MICROS
        _save_signal(store, sid=sid, signal_id="sig594", row=row, seen_us=seen_us)
        engine.schedule_phase590_wallet_variants(
            signal_id="sig594", row=row, first_seen_us=seen_us, lower_lag_us=0,
            market_store=market_store, detection_p_up_mc=0.50,
        )
        up = OrderBookState(slug=market.slug, token_id="up", outcome="UP")
        up.apply_snapshot(
            bids=[{"price": "0.43", "size": "100"}],
            asks=[{"price": "0.44", "size": "100"}],
            received_at_us=seen_us + 25_000, server_ts_us=seen_us + 25_000, book_hash="h",
        )
        down = OrderBookState(slug=market.slug, token_id="down", outcome="DOWN")
        down.apply_snapshot(
            bids=[{"price": "0.55", "size": "100"}],
            asks=[{"price": "0.56", "size": "100"}],
            received_at_us=seen_us + 25_000, server_ts_us=seen_us + 25_000, book_hash="d",
        )
        class Collector:
            states = {"up": up, "down": down}
        class Pool:
            def collector_for_slug(self, slug):
                assert slug == market.slug
                return Collector()
        stats = engine.execute_due_copies(
            collector_pool=Pool(), market_store=market_store, now_us=seen_us + 25_000
        )
        assert stats[f"{STRATEGY_9_ROUTER_A}:ROUTE_FALLBACK"] == 1
        router = next(p for p in engine.pending_copies if p.strategy == STRATEGY_9_ROUTER_A)
        assert router.router_stage == "BOOK_FALLBACK"
        assert router.require_book_confirmation is True
        assert router.scheduled_at_us == seen_us + 75_000


def test_phase592_router_low_model_specialist_failure_stops_instead_of_fallback(tmp_path: Path) -> None:
    from btc_v3.forensics.forward_lab import STRATEGY_9_ROUTER_A
    from btc_v3.state.book_state import OrderBookState

    initialize_project_databases(tmp_path)
    market = _market()
    with MarketStore.open_project(tmp_path) as market_store, ForwardWalletStore.open_project(tmp_path) as store:
        market_store.persist_market(market, 1000 * MICROS)
        sid = "s592-router-veto"
        store.start_session(session_id=sid, wallet="0x" + "1" * 40, started_at_us=1000 * MICROS, config={})
        engine = ForwardPaperEngine(session_id=sid, config=ForwardLabConfig(), store=store)
        row = _trade(outcome="UP", size="48", price="0.35", tx="0x595")
        seen_us = 1011 * MICROS
        _save_signal(store, sid=sid, signal_id="sig595", row=row, seen_us=seen_us)
        engine.schedule_phase590_wallet_variants(
            signal_id="sig595", row=row, first_seen_us=seen_us, lower_lag_us=0,
            market_store=market_store, detection_p_up_mc=0.20,
        )
        up = OrderBookState(slug=market.slug, token_id="up", outcome="UP")
        up.apply_snapshot(
            bids=[{"price": "0.43", "size": "100"}],
            asks=[{"price": "0.44", "size": "100"}],
            received_at_us=seen_us + 25_000, server_ts_us=seen_us + 25_000, book_hash="h",
        )
        class Collector:
            states = {"up": up}
        class Pool:
            def collector_for_slug(self, slug):
                return Collector()
        stats = engine.execute_due_copies(
            collector_pool=Pool(), market_store=market_store, now_us=seen_us + 25_000
        )
        assert stats[f"{STRATEGY_9_ROUTER_A}:ROUTER_MODEL_VETO"] == 1
        assert not any(p.strategy == STRATEGY_9_ROUTER_A for p in engine.pending_copies)
