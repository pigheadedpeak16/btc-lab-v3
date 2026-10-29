from decimal import Decimal

import pytest

from btc_v3.features.books import market_book_features, side_features
from btc_v3.state.book_state import OrderBookState


def _state(outcome: str, bids: dict[str, str], asks: dict[str, str]) -> OrderBookState:
    s = OrderBookState(slug="btc-updown-5m-test", token_id=outcome, outcome=outcome)
    s.apply_snapshot(bids=[{"price": k, "size": v} for k,v in bids.items()],
        asks=[{"price": k, "size": v} for k,v in asks.items()],
        received_at_us=1, server_ts_us=1, book_hash="fixture")
    return s


def test_near_touch_depth_and_imbalance_are_full_l2():
    up = _state("UP", {"0.50": "20", "0.49": "10", "0.40": "100"}, {"0.51": "5", "0.52": "5", "0.70": "100"})
    f = side_features(up, depth_band=0.02)
    assert f.best_bid == 0.50
    assert f.best_ask == 0.51
    assert f.near_bid_depth == 30.0
    assert f.near_ask_depth == 10.0
    assert f.imbalance == pytest.approx(0.5)


def test_combined_direction_score_is_signed_up_support():
    up = _state("UP", {"0.50": "30"}, {"0.51": "10"})
    down = _state("DOWN", {"0.49": "10"}, {"0.50": "30"})
    f = market_book_features(up, down, depth_band=0.02)
    assert f.book_direction_score > 0


def test_unsynchronized_book_fails_closed():
    up = _state("UP", {"0.50": "10"}, {"0.51": "10"})
    up.synchronized = False
    with pytest.raises(ValueError, match="MISSING_BOOK"):
        side_features(up, depth_band=0.02)
