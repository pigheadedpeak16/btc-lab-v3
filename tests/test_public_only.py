from btc_v3.feeds.polymarket import PolymarketPublicRest
from btc_v3.feeds.polymarket_collector import PolymarketBookCollector


def test_public_rest_exposes_no_execution_methods() -> None:
    names = {name.lower() for name in dir(PolymarketPublicRest)}
    forbidden = ("place_order", "cancel_order", "sign_order", "withdraw", "deposit")
    assert all(name not in names for name in forbidden)


def test_collector_exposes_no_execution_methods() -> None:
    names = {name.lower() for name in dir(PolymarketBookCollector)}
    forbidden = ("place_order", "cancel_order", "sign_order", "withdraw", "deposit")
    assert all(name not in names for name in forbidden)
