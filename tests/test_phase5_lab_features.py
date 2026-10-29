import pytest

from btc_v3.features.btc import CausalBtcFeatures
from btc_v3.features.lab import build_strategy_lab_features, classify_time_regime
from btc_v3.storage.external_store import PriceReference
from btc_v3.types import TimeToExpiryRegime, VolatilityRegime


def _features(**changes):
    values = dict(
        slug="x",
        decision_time_us=100_000_000,
        seconds_remaining=45.0,
        price_reference=PriceReference("x", 0, 300_000_000, "86000", 0, 0, "CHAINLINK", "x", "EXACT"),
        price_to_beat=86000.0,
        coinbase_price=86100.0,
        kraken_price=86090.0,
        predictor_price=86095.0,
        official_twap=86110.0,
        official_twap_source_ts_us=100_000_000,
        distance_dollars=95.0,
        distance_pct=95.0/86000,
        log_distance=0.001,
        returns={5:.0005, 30:.0012, 60:.0015},
        coinbase_returns={5:.0006, 30:.0014},
        kraken_returns={5:.0004, 30:.0010},
        coinbase_kraken_basis=(86100-86090)/86095,
        realized_volatility={60:.00008},
    )
    values.update(changes)
    return CausalBtcFeatures(**values)


def test_time_regime_boundaries():
    assert classify_time_regime(240) is TimeToExpiryRegime.EARLY
    assert classify_time_regime(120) is TimeToExpiryRegime.MID
    assert classify_time_regime(45) is TimeToExpiryRegime.FINAL_WINDOW_EARLY
    assert classify_time_regime(20) is TimeToExpiryRegime.FINAL_WINDOW_LATE
    assert classify_time_regime(5) is TimeToExpiryRegime.FINAL_WINDOW_CRITICAL


def test_lab_features_include_twap_residual_and_cross_exchange_diagnostics():
    lab = build_strategy_lab_features(_features())
    assert lab.volatility_regime is VolatilityRegime.NORMAL
    assert lab.twap_residual_dollars == pytest.approx(15.0)
    assert lab.twap_residual_pct > 0
    assert lab.twap_residual_sigma_units is not None
    assert -1 <= lab.cross_exchange_consensus_score <= 1
    assert lab.cross_exchange_return_spread == pytest.approx(.0002)
    assert lab.lead_lag_proxy is not None


def test_lab_feature_builder_rejects_invalid_cutoffs():
    with pytest.raises(ValueError):
        build_strategy_lab_features(_features(), low_volatility_cutoff=.001, high_volatility_cutoff=.0001)
