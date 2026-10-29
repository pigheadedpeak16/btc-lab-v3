from btc_v3.forensics.forward_lab import (
    ForwardLabConfig,
    TOURNAMENT_HORIZONS,
    full_tournament_specs,
    verify_full_tournament_config,
)
from btc_v3.config import MeanReversionParams, MomentumParams


def test_full_tournament_variant_count_and_uniqueness():
    names = []
    for horizon in TOURNAMENT_HORIZONS:
        names.extend(name for name, _ in full_tournament_specs(horizon))
        names.append(f"CTRL_WALLET_CHEAP_CONTRA_H{horizon:03d}")
    assert len(names) == 84
    assert len(set(names)) == 84
    assert sum("_BASE_" in n for n in names) == 42
    assert sum("_WALLET_" in n and not n.startswith("CTRL_") for n in names) == 36


def test_wallet_informed_momentum_is_deliberately_stricter_than_base():
    specs = dict(full_tournament_specs(60))
    base = specs["S3_BASE_H060_MOMENTUM"].params
    wallet = specs["S3_WALLET_H060_MOMENTUM"].params
    assert isinstance(base, MomentumParams)
    assert isinstance(wallet, MomentumParams)
    assert wallet.minimum_return > base.minimum_return
    assert wallet.signal_weight_beta < base.signal_weight_beta


def test_wallet_informed_mean_reversion_targets_short_contrarian_structure():
    specs = dict(full_tournament_specs(60))
    params = specs["S4_WALLET_H060_MEAN_REVERSION"].params
    assert isinstance(params, MeanReversionParams)
    assert params.extension_lookback_seconds == 15
    assert params.reversal_lookback_seconds == 5
    assert params.signal_weight_beta == 0.45


def test_full_tournament_config_stays_paper_only_and_valid():
    cfg = ForwardLabConfig(enable_full_tournament=True)
    cfg.validate()
    checks = verify_full_tournament_config()
    assert all(checks.values())


def test_active_pool_market_rejects_expired_discovery_fallback():
    from types import SimpleNamespace
    from btc_v3.forensics.forward_lab import _active_pool_market

    live = SimpleNamespace(round_start_us=1_000_000, round_end_us=301_000_000, slug="live")
    expired = SimpleNamespace(round_start_us=1_000_000, round_end_us=301_000_000, slug="expired")
    collector = object()

    class Pool:
        def __init__(self, market):
            self.market = market
        def current(self, asset):
            assert asset == "BTC"
            return self.market, collector

    market, got_collector = _active_pool_market(Pool(live), asset="BTC", now_us=200_000_000)
    assert market is live
    assert got_collector is collector

    market, got_collector = _active_pool_market(Pool(expired), asset="BTC", now_us=301_000_000)
    assert market is None
    assert got_collector is None
