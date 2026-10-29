import pytest

from btc_v3.config import CommonExperimentConfig, MomentumParams, StrategyConfig
from btc_v3.features.books import BookSideFeatures, MarketBookFeatures
from btc_v3.features.btc import CausalBtcFeatures
from btc_v3.features.lab import build_strategy_lab_features
from btc_v3.storage.external_store import PriceReference
from btc_v3.strategies.engine import StrategyDecisionEngine, StrategyInputs
from btc_v3.types import StrategyFamily


def _btc():
    return CausalBtcFeatures(
        slug="x", decision_time_us=1_000_000, seconds_remaining=25,
        price_reference=PriceReference("x", 0, 300_000_000, "86000", 0, 0, "CHAINLINK", "x", "EXACT"),
        price_to_beat=86000, coinbase_price=86200, kraken_price=86200,
        predictor_price=86200, official_twap=86190, official_twap_source_ts_us=1_000_000,
        distance_dollars=200, distance_pct=200/86000, log_distance=.0023,
        returns={5:.0007, 15:.0011, 30:.0017, 60:.0020},
        coinbase_returns={5:.00075, 15:.00115, 30:.00175},
        kraken_returns={5:.00065, 15:.00105, 30:.00165},
        coinbase_kraken_basis=0,
        realized_volatility={30:.00008, 60:.00009},
    )


def _side(outcome, bid, ask):
    return BookSideFeatures(outcome, bid, ask, (bid+ask)/2, ask-bid, 100, 100, 0, 200)


def test_strategy_decision_exposes_signal_attribution_without_regime_adjustment():
    btc = _btc()
    lab = build_strategy_lab_features(btc)
    book = MarketBookFeatures(_side("UP", .64, .65), _side("DOWN", .34, .35), 0)
    cfg = StrategyConfig(
        strategy_family=StrategyFamily.MOMENTUM,
        common=CommonExperimentConfig(decision_seconds_before_close=30, min_edge=.01),
        params=MomentumParams(signal_weight_beta=.35),
    )
    decision = StrategyDecisionEngine().evaluate(cfg, StrategyInputs(btc, .72, book, lab=lab))
    assert decision.signal_beta == pytest.approx(.35)
    assert decision.logit_delta == pytest.approx(decision.signal_score * .35)
    assert decision.signal_components
    assert decision.time_regime == lab.time_regime.value
    assert decision.volatility_regime == lab.volatility_regime.value
