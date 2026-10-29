from btc_v3.config import (
    BookImbalanceParams,
    CrossExchangeParams,
    MeanReversionParams,
    MicrostructureParams,
    MomentumParams,
)
from btc_v3.features.books import BookSideFeatures, MarketBookFeatures
from btc_v3.features.btc import CausalBtcFeatures
from btc_v3.storage.external_store import PriceReference
from btc_v3.strategies.signals import (
    MicrostructureInput,
    book_imbalance_signal,
    cross_exchange_signal,
    mean_reversion_signal,
    microstructure_signal,
    momentum_signal,
)


def _btc(**changes):
    values = dict(
        slug="x",
        decision_time_us=1_000_000,
        seconds_remaining=60.0,
        price_reference=PriceReference("x", 0, 300_000_000, "86000", 0, 0, "CHAINLINK", "x", "EXACT"),
        price_to_beat=86000.0,
        coinbase_price=86100.0,
        kraken_price=86101.0,
        predictor_price=86100.5,
        official_twap=86090.0,
        official_twap_source_ts_us=1_000_000,
        distance_dollars=100.5,
        distance_pct=100.5 / 86000.0,
        log_distance=0.001,
        returns={5: 0.0005, 10: 0.0008, 15: 0.0010, 30: 0.0015, 60: 0.0018},
        coinbase_returns={5: 0.0005, 10: 0.0008, 15: 0.0010, 30: 0.0015, 60: 0.0018},
        kraken_returns={5: 0.00045, 10: 0.00075, 15: 0.00095, 30: 0.00145, 60: 0.00175},
        coinbase_kraken_basis=-0.00001,
        realized_volatility={30: 0.00008, 60: 0.00009, 120: 0.00010},
    )
    values.update(changes)
    return CausalBtcFeatures(**values)


def _side(outcome, bid, ask, bid_depth, ask_depth):
    total = bid_depth + ask_depth
    imb = (bid_depth - ask_depth) / total if total else 0.0
    return BookSideFeatures(outcome, bid, ask, (bid + ask) / 2, ask - bid, bid_depth, ask_depth, imb, total)


def test_momentum_positive_supports_up():
    sig = momentum_signal(_btc(), MomentumParams())
    assert sig.active
    assert sig.score > 0


def test_mean_reversion_opposes_extension():
    f = _btc(returns={5: -0.0003, 10: 0.0004, 15: 0.0007, 30: 0.0015, 60: 0.0018})
    sig = mean_reversion_signal(f, MeanReversionParams())
    assert sig.active
    assert sig.score < 0


def test_book_imbalance_signed_up():
    book = MarketBookFeatures(
        up=_side("UP", .50, .51, 50, 10),
        down=_side("DOWN", .49, .50, 10, 50),
        book_direction_score=0.666,
    )
    sig = book_imbalance_signal(book, BookImbalanceParams())
    assert sig.active
    assert sig.score > 0


def test_microstructure_combines_move_and_pressure():
    prev = MarketBookFeatures(
        up=_side("UP", .48, .49, 20, 20),
        down=_side("DOWN", .51, .52, 20, 20),
        book_direction_score=0.0,
    )
    cur = MarketBookFeatures(
        up=_side("UP", .50, .51, 40, 10),
        down=_side("DOWN", .49, .50, 10, 40),
        book_direction_score=0.6,
    )
    sig = microstructure_signal(MicrostructureInput(cur, prev), MicrostructureParams())
    assert sig.active
    assert sig.score > 0


def test_cross_exchange_requires_agreement():
    f = _btc(
        coinbase_returns={10: 0.0006},
        kraken_returns={10: -0.0006},
        coinbase_kraken_basis=0.0,
    )
    sig = cross_exchange_signal(f, CrossExchangeParams())
    assert not sig.active
    assert sig.reason in {"EXCHANGE_DISAGREEMENT", "SIGNAL_DISAGREES"}
