from decimal import Decimal

from btc_v3.state.book_state import OrderBookState


def make_state() -> OrderBookState:
    state = OrderBookState("slug", "token", "UP")
    state.apply_snapshot(
        bids=[{"price": "0.40", "size": "10"}, {"price": "0.42", "size": "5"}],
        asks=[{"price": "0.60", "size": "10"}, {"price": "0.55", "size": "7"}],
        received_at_us=2_000_000,
        server_ts_us=1_000_000,
        book_hash="h1",
        tick_size="0.01",
        min_order_size="5",
    )
    return state


def test_snapshot_preserves_full_depth_and_best_quotes() -> None:
    state = make_state()
    assert state.synchronized
    assert len(state.bids) == 2
    assert len(state.asks) == 2
    assert state.best_bid.price == Decimal("0.42")
    assert state.best_ask.price == Decimal("0.55")
    assert state.spread == Decimal("0.13")
    assert state.min_order_size == Decimal("5")


def test_old_source_snapshot_is_retained_for_audit_without_refreshing_execution_age() -> None:
    state = OrderBookState("slug", "token", "UP")
    state.apply_snapshot(
        bids=[{"price": "0.4", "size": "1"}],
        asks=[{"price": "0.6", "size": "1"}],
        received_at_us=100_000_000,
        server_ts_us=1_000_000,
        book_hash="quiet-book",
    )
    assert state.synchronized
    assert state.last_verified_at_us == 100_000_000
    assert state.age_seconds(101_000_000) == 100.0


def test_price_change_updates_level_and_zero_deletes() -> None:
    state = make_state()
    assert state.apply_price_change(
        order_side="BUY",
        price="0.43",
        size="2",
        received_at_us=3_000_000,
        server_ts_us=2_000_000,
    )
    assert state.best_bid.price == Decimal("0.43")
    assert state.apply_price_change(
        order_side="BUY",
        price="0.43",
        size="0",
        received_at_us=4_000_000,
        server_ts_us=3_000_000,
    )
    assert state.best_bid.price == Decimal("0.42")


def test_older_delta_is_not_applied_after_newer_state() -> None:
    state = make_state()
    state.apply_price_change(
        order_side="BUY",
        price="0.43",
        size="2",
        received_at_us=3_000_000,
        server_ts_us=5_000_000,
    )
    applied = state.apply_price_change(
        order_side="BUY",
        price="0.99",
        size="100",
        received_at_us=4_000_000,
        server_ts_us=4_000_000,
    )
    assert not applied
    assert state.best_bid.price == Decimal("0.43")
