from __future__ import annotations

from decimal import Decimal
from pathlib import Path

from btc_v3.forensics.forward_lab import (
    ForwardLabConfig,
    RoundPosition,
    crypto_taker_fee,
    economic_key,
    protected_copy_reason,
    publication_lag_bounds_us,
    verify_forward_lab_config,
    walk_taker_fak,
)
from btc_v3.forensics.models import WalletActivity, WalletTrade
from btc_v3.state.book_state import OrderBookState
from btc_v3.storage.db import initialize_project_databases
from btc_v3.storage.schemas import RESEARCH_SCHEMA


def _trade() -> WalletTrade:
    return WalletTrade.from_api(
        "0x" + "1" * 40,
        {
            "timestamp": 1700000000,
            "event_slug": "btc-updown-5m-1699999800",
            "token_id": "tok-up",
            "side": "BUY",
            "outcome": "Up",
            "price": "0.35",
            "size": "48",
            "transaction_hash": "0xabc",
        },
    )


def _activity() -> WalletActivity:
    return WalletActivity.from_api(
        "0x" + "1" * 40,
        {
            "type": "TRADE",
            "timestamp": 1700000000,
            "event_slug": "btc-updown-5m-1699999800",
            "token_id": "tok-up",
            "side": "BUY",
            "outcome": "Up",
            "price": "0.35",
            "size": "48",
            "transaction_hash": "0xabc",
        },
    )


def _state() -> OrderBookState:
    state = OrderBookState("btc-updown-5m-1699999800", "tok-up", "UP")
    state.apply_snapshot(
        bids=[{"price": "0.34", "size": "5"}, {"price": "0.33", "size": "10"}],
        asks=[{"price": "0.36", "size": "3"}, {"price": "0.37", "size": "4"}],
        received_at_us=1_700_000_000_000_000,
        server_ts_us=1_700_000_000_000_000,
        book_hash="h",
        tick_size="0.01",
        min_order_size="1",
    )
    return state


def test_economic_key_collapses_trade_activity_aliases() -> None:
    assert economic_key(_trade()) == economic_key(_activity())


def test_publication_bounds_for_coarse_second_clock() -> None:
    lower, upper = publication_lag_bounds_us(10_000_000, 12_500_000)
    assert lower == 1_500_001
    assert upper == 2_500_000


def test_crypto_taker_fee_matches_documented_curve() -> None:
    fee = crypto_taker_fee(shares=Decimal("100"), price=Decimal("0.50"), rate=Decimal("0.07"))
    assert fee == Decimal("1.75000")


def test_fak_buy_walk_allows_partial_fill() -> None:
    plan = walk_taker_fak(
        _state(),
        side="BUY",
        shares=Decimal("10"),
        fee_rate=Decimal("0"),
        max_price=Decimal("0.37"),
    )
    assert plan.filled_shares == Decimal("7")
    assert plan.vwap == (Decimal("3") * Decimal("0.36") + Decimal("4") * Decimal("0.37")) / Decimal("7")


def test_fak_sell_walk_uses_bids() -> None:
    plan = walk_taker_fak(
        _state(),
        side="SELL",
        shares=Decimal("8"),
        fee_rate=Decimal("0"),
        min_price=Decimal("0.33"),
    )
    assert plan.filled_shares == Decimal("8")
    assert plan.levels[0].price == Decimal("0.34")
    assert plan.levels[-1].price == Decimal("0.33")


def test_protected_copy_rejects_stale_publication_and_bad_price() -> None:
    cfg = ForwardLabConfig()
    reason = protected_copy_reason(
        side="BUY",
        target_price=Decimal("0.35"),
        executable_price=Decimal("0.39"),
        spread=Decimal("0.02"),
        top_depth=Decimal("20"),
        lower_lag_us=1_000_000,
        config=cfg,
    )
    assert reason == "PRICE_DETERIORATED"
    reason = protected_copy_reason(
        side="BUY",
        target_price=Decimal("0.35"),
        executable_price=Decimal("0.35"),
        spread=Decimal("0.02"),
        top_depth=Decimal("20"),
        lower_lag_us=6_000_000,
        config=cfg,
    )
    assert reason == "PUBLICATION_TOO_LATE"


def test_round_position_pair_and_settlement() -> None:
    pos = RoundPosition("slug", 0, 300_000_000)
    pos.record_fill(outcome="UP", side="BUY", shares=Decimal("10"), notional=Decimal("3.5"), fee=Decimal("0"))
    pos.record_fill(outcome="DOWN", side="BUY", shares=Decimal("10"), notional=Decimal("5.5"), fee=Decimal("0"))
    assert pos.avg_pair_cost == Decimal("0.9")
    pnl = pos.settle(resolved_side="UP", at_us=400_000_000)
    assert pnl == Decimal("1.0")


def test_phase56_schema_tables_initialize(tmp_path: Path) -> None:
    initialize_project_databases(tmp_path)
    import sqlite3

    con = sqlite3.connect(tmp_path / "research" / "v3_research.sqlite")
    names = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    con.close()
    for table in {
        "wallet_forward_sessions",
        "wallet_forward_source_observations",
        "wallet_forward_signals",
        "wallet_forward_orders",
        "wallet_forward_fills",
        "wallet_forward_rounds",
    }:
        assert table in names


def test_verify_forward_config_all_pass() -> None:
    assert all(verify_forward_lab_config().values())
