from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from ..state.book_state import OrderBookState


@dataclass(frozen=True)
class BookSideFeatures:
    outcome: str
    best_bid: float
    best_ask: float
    midpoint: float
    spread: float
    near_bid_depth: float
    near_ask_depth: float
    imbalance: float
    total_near_depth: float


@dataclass(frozen=True)
class MarketBookFeatures:
    up: BookSideFeatures
    down: BookSideFeatures
    book_direction_score: float


def _bounded(value: float) -> float:
    return max(-1.0, min(1.0, value))


def side_features(state: OrderBookState, *, depth_band: float) -> BookSideFeatures:
    if depth_band < 0:
        raise ValueError("depth_band must be non-negative")
    if not state.depth_reliable:
        raise ValueError("MISSING_BOOK")
    bid = state.best_bid
    ask = state.best_ask
    if bid is None or ask is None:
        raise ValueError("MISSING_BOOK")
    best_bid = float(bid.price)
    best_ask = float(ask.price)
    if best_ask < best_bid:
        raise ValueError("CROSSED_BOOK")

    band = Decimal(str(depth_band))
    near_bid = sum(
        (size for price, size in state.bids.items() if price >= bid.price - band),
        Decimal("0"),
    )
    near_ask = sum(
        (size for price, size in state.asks.items() if price <= ask.price + band),
        Decimal("0"),
    )
    bid_depth = float(near_bid)
    ask_depth = float(near_ask)
    total = bid_depth + ask_depth
    imbalance = 0.0 if total <= 0 else (bid_depth - ask_depth) / total
    return BookSideFeatures(
        outcome=state.outcome.upper(),
        best_bid=best_bid,
        best_ask=best_ask,
        midpoint=(best_bid + best_ask) / 2.0,
        spread=best_ask - best_bid,
        near_bid_depth=bid_depth,
        near_ask_depth=ask_depth,
        imbalance=_bounded(imbalance),
        total_near_depth=total,
    )


def market_book_features(
    up_state: OrderBookState,
    down_state: OrderBookState,
    *,
    depth_band: float,
) -> MarketBookFeatures:
    up = side_features(up_state, depth_band=depth_band)
    down = side_features(down_state, depth_band=depth_band)
    if up.outcome != "UP" or down.outcome != "DOWN":
        raise ValueError("BOOK_SIDE_MISMATCH")
    # If UP bids dominate its asks, that supports UP. If DOWN bids dominate
    # its asks, that supports DOWN. Their difference is naturally in [-2, 2],
    # so divide by two to retain a bounded signed score.
    direction = _bounded((up.imbalance - down.imbalance) / 2.0)
    return MarketBookFeatures(up=up, down=down, book_direction_score=direction)


def boundary_completed_side_features(state: OrderBookState, *, depth_band: float) -> BookSideFeatures:
    """Build conservative bounded features for a synchronized one-sided book.

    Missing bids are completed at the binary floor 0.0 and missing asks at the
    binary ceiling 1.0. This is decision-only scaffolding: callers must still
    require the actual executable side to exist before scheduling an order.
    It allows model-only strategy families to make an explicit skip/decision
    instead of treating legitimate boundary liquidity as a broken collector.
    """
    if depth_band < 0:
        raise ValueError("depth_band must be non-negative")
    if not state.depth_reliable:
        raise ValueError("MISSING_BOOK")

    bid = state.best_bid
    ask = state.best_ask
    best_bid = float(bid.price) if bid is not None else 0.0
    best_ask = float(ask.price) if ask is not None else 1.0
    if best_ask < best_bid:
        raise ValueError("CROSSED_BOOK")

    band = Decimal(str(depth_band))
    if bid is None:
        near_bid = Decimal("0")
    else:
        near_bid = sum(
            (size for price, size in state.bids.items() if price >= bid.price - band),
            Decimal("0"),
        )
    if ask is None:
        near_ask = Decimal("0")
    else:
        near_ask = sum(
            (size for price, size in state.asks.items() if price <= ask.price + band),
            Decimal("0"),
        )

    bid_depth = float(near_bid)
    ask_depth = float(near_ask)
    total = bid_depth + ask_depth
    imbalance = 0.0 if total <= 0 else (bid_depth - ask_depth) / total
    return BookSideFeatures(
        outcome=state.outcome.upper(),
        best_bid=best_bid,
        best_ask=best_ask,
        midpoint=(best_bid + best_ask) / 2.0,
        spread=best_ask - best_bid,
        near_bid_depth=bid_depth,
        near_ask_depth=ask_depth,
        imbalance=_bounded(imbalance),
        total_near_depth=total,
    )


def boundary_completed_market_book_features(
    up_state: OrderBookState,
    down_state: OrderBookState,
    *,
    depth_band: float,
) -> MarketBookFeatures:
    """Conservative features for synchronized books that may be one-sided.

    This must never be used as proof that missing liquidity is executable.
    Actual order scheduling/execution must inspect the raw OrderBookState side.
    """
    up = boundary_completed_side_features(up_state, depth_band=depth_band)
    down = boundary_completed_side_features(down_state, depth_band=depth_band)
    if up.outcome != "UP" or down.outcome != "DOWN":
        raise ValueError("BOOK_SIDE_MISMATCH")
    direction = _bounded((up.imbalance - down.imbalance) / 2.0)
    return MarketBookFeatures(up=up, down=down, book_direction_score=direction)
