from __future__ import annotations

from decimal import Decimal

from btc_v3.features.books import (
    boundary_completed_market_book_features,
    market_book_features,
)
from btc_v3.state.book_state import OrderBookState


def _state(outcome: str, *, bids, asks) -> OrderBookState:
    st = OrderBookState("slug", f"tok-{outcome.lower()}", outcome)
    st.apply_snapshot(
        bids=[{"price": p, "size": s} for p, s in bids],
        asks=[{"price": p, "size": s} for p, s in asks],
        received_at_us=1_000_000,
        server_ts_us=1_000_000,
        book_hash=f"h-{outcome}",
        tick_size="0.001",
        min_order_size="1",
    )
    return st


def test_strict_book_remains_strict_for_one_sided_boundary_market() -> None:
    up = _state("UP", bids=[("0.999", "20")], asks=[])
    down = _state("DOWN", bids=[], asks=[("0.001", "20")])
    try:
        market_book_features(up, down, depth_band=0.02)
    except ValueError as exc:
        assert str(exc) == "MISSING_BOOK"
    else:
        raise AssertionError("strict L2 features must not fabricate missing sides")


def test_boundary_completion_is_conservative_and_preserves_real_executable_quotes() -> None:
    up = _state("UP", bids=[("0.999", "20")], asks=[])
    down = _state("DOWN", bids=[], asks=[("0.001", "20")])
    book = boundary_completed_market_book_features(up, down, depth_band=0.02)
    assert book.up.best_bid == 0.999
    assert book.up.best_ask == 1.0
    assert book.down.best_bid == 0.0
    assert book.down.best_ask == 0.001
    assert book.up.spread == 0.0010000000000000009 or abs(book.up.spread - 0.001) < 1e-12
    assert abs(book.down.spread - 0.001) < 1e-12


def test_boundary_completion_refuses_unsynchronized_state() -> None:
    up = _state("UP", bids=[("0.999", "20")], asks=[])
    down = _state("DOWN", bids=[], asks=[("0.001", "20")])
    down.mark_unsynchronized()
    try:
        boundary_completed_market_book_features(up, down, depth_band=0.02)
    except ValueError as exc:
        assert str(exc) == "MISSING_BOOK"
    else:
        raise AssertionError("unsynchronized books must still fail closed")
