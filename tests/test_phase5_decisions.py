import pytest

from btc_v3.config import (
    CommonExperimentConfig,
    FairValueParams,
    MomentumParams,
    StrategyConfig,
)
from btc_v3.features.books import BookSideFeatures, MarketBookFeatures
from btc_v3.features.btc import CausalBtcFeatures
from btc_v3.storage.external_store import PriceReference
from btc_v3.strategies.engine import StrategyDecisionEngine, StrategyInputs, adjust_probability_logit
from btc_v3.types import Action, SignalIntegrationMode, StrategyFamily


def _btc(upward=True):
    sign = 1 if upward else -1
    return CausalBtcFeatures(
        slug="x",
        decision_time_us=1_000_000,
        seconds_remaining=30.0,
        price_reference=PriceReference("x", 0, 300_000_000, "86000", 0, 0, "CHAINLINK", "x", "EXACT"),
        price_to_beat=86000.0,
        coinbase_price=86200.0,
        kraken_price=86200.0,
        predictor_price=86200.0,
        official_twap=86180.0,
        official_twap_source_ts_us=1_000_000,
        distance_dollars=200.0,
        distance_pct=200/86000,
        log_distance=0.0023,
        returns={5: sign*.0006, 10: sign*.0008, 15: sign*.0010, 30: sign*.0015, 60: sign*.0018},
        coinbase_returns={5: sign*.0006, 10: sign*.0008, 15: sign*.0010, 30: sign*.0015, 60: sign*.0018},
        kraken_returns={5: sign*.00055, 10: sign*.00075, 15: sign*.00095, 30: sign*.00145, 60: sign*.00175},
        coinbase_kraken_basis=0.0,
        realized_volatility={30: .00008, 60: .00009, 120: .00010},
    )


def _side(outcome, bid, ask):
    return BookSideFeatures(outcome, bid, ask, (bid+ask)/2, ask-bid, 100, 100, 0.0, 200)


def _book(up_ask=.70, down_ask=.31):
    return MarketBookFeatures(
        up=_side("UP", up_ask-.01, up_ask),
        down=_side("DOWN", down_ask-.01, down_ask),
        book_direction_score=0.0,
    )


def _cfg(family, params, **common_changes):
    values = dict(decision_seconds_before_close=30, min_edge=.03, max_spread=.05)
    values.update(common_changes)
    return StrategyConfig(
        strategy_family=family,
        common=CommonExperimentConfig(**values),
        params=params,
    )


def test_logit_adjustment_bounded_and_directional():
    p = adjust_probability_logit(.8, -1.0, .35)
    assert 0 < p < .8
    assert adjust_probability_logit(.8, 1.0, .35) > .8


def test_fair_value_uses_executable_asks_not_midpoint():
    cfg = _cfg(StrategyFamily.FAIR_VALUE, FairValueParams())
    d = StrategyDecisionEngine().evaluate(
        cfg, StrategyInputs(btc=_btc(), base_p_up=.80, book=_book(up_ask=.70, down_ask=.31))
    )
    assert d.action is Action.BUY_UP
    assert d.up_edge == pytest.approx(.10)


def test_directional_down_signal_cannot_directly_buy_down_when_settlement_probability_is_low():
    # Critical scenario: BTC is far above PTB, base P(DOWN)=5%, while momentum points DOWN.
    # The signal only adjusts the logit; it never becomes an action by itself.
    cfg = _cfg(StrategyFamily.MOMENTUM, MomentumParams(signal_weight_beta=.35))
    d = StrategyDecisionEngine().evaluate(
        cfg,
        StrategyInputs(
            btc=_btc(upward=False),
            base_p_up=.95,
            book=_book(up_ask=.96, down_ask=.20),
        ),
    )
    assert d.signal_score < 0
    assert d.final_p_up is not None and d.final_p_up > .90
    assert d.action is Action.SKIP
    assert d.reason == "EDGE_TOO_SMALL"


def test_agreement_filter_does_not_change_probability():
    cfg = _cfg(
        StrategyFamily.MOMENTUM,
        MomentumParams(),
        signal_integration_mode=SignalIntegrationMode.AGREEMENT_FILTER,
    )
    d = StrategyDecisionEngine().evaluate(
        cfg,
        StrategyInputs(btc=_btc(upward=True), base_p_up=.75, book=_book(up_ask=.68, down_ask=.33)),
    )
    assert d.final_p_up == pytest.approx(.75)


def test_spread_gate_applies_before_trade_intent():
    book = MarketBookFeatures(
        up=BookSideFeatures("UP", .60, .70, .65, .10, 100, 100, 0, 200),
        down=_side("DOWN", .29, .30),
        book_direction_score=0,
    )
    cfg = _cfg(StrategyFamily.FAIR_VALUE, FairValueParams(), max_spread=.05)
    d = StrategyDecisionEngine().evaluate(cfg, StrategyInputs(_btc(), .80, book))
    assert d.action is Action.SKIP
    assert d.reason == "SPREAD_TOO_WIDE"
