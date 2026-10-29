from __future__ import annotations

import math
from dataclasses import dataclass

from ..types import TimeToExpiryRegime, VolatilityRegime
from .btc import CausalBtcFeatures


@dataclass(frozen=True)
class StrategyLabFeatures:
    """Derived, causal diagnostics available to strategy research.

    These features do not change any baseline strategy by themselves. They are
    recorded so Phase 8/9 can test whether regime-conditioned variants genuinely
    improve out-of-sample results.
    """

    time_regime: TimeToExpiryRegime
    volatility_regime: VolatilityRegime
    volatility_reference: float | None
    twap_residual_dollars: float
    twap_residual_pct: float
    twap_residual_log: float
    twap_residual_sigma_units: float | None
    coinbase_kraken_basis: float
    cross_exchange_consensus_score: float
    cross_exchange_return_spread: float | None
    lead_lag_proxy: float | None


def classify_time_regime(seconds_remaining: float) -> TimeToExpiryRegime:
    if seconds_remaining > 180:
        return TimeToExpiryRegime.EARLY
    if seconds_remaining > 60:
        return TimeToExpiryRegime.MID
    if seconds_remaining > 30:
        return TimeToExpiryRegime.FINAL_WINDOW_EARLY
    if seconds_remaining > 10:
        return TimeToExpiryRegime.FINAL_WINDOW_LATE
    return TimeToExpiryRegime.FINAL_WINDOW_CRITICAL


def classify_volatility_regime(
    sigma_per_sqrt_second: float | None,
    *,
    low_cutoff: float,
    high_cutoff: float,
) -> VolatilityRegime:
    if sigma_per_sqrt_second is None:
        return VolatilityRegime.UNKNOWN
    if sigma_per_sqrt_second < low_cutoff:
        return VolatilityRegime.LOW
    if sigma_per_sqrt_second >= high_cutoff:
        return VolatilityRegime.HIGH
    return VolatilityRegime.NORMAL


def build_strategy_lab_features(
    features: CausalBtcFeatures,
    *,
    volatility_window_seconds: int = 60,
    low_volatility_cutoff: float = 0.00004,
    high_volatility_cutoff: float = 0.00012,
    fast_exchange_window_seconds: int = 5,
    slow_exchange_window_seconds: int = 30,
) -> StrategyLabFeatures:
    if low_volatility_cutoff <= 0 or high_volatility_cutoff <= low_volatility_cutoff:
        raise ValueError("invalid volatility regime cutoffs")
    if features.predictor_price <= 0 or features.official_twap <= 0:
        raise ValueError("prices must be positive")

    sigma = features.realized_volatility.get(volatility_window_seconds)
    residual_dollars = features.official_twap - features.predictor_price
    residual_pct = residual_dollars / features.predictor_price
    residual_log = math.log(features.official_twap / features.predictor_price)
    sigma_units: float | None = None
    if sigma is not None and sigma > 0:
        # A 60-second TWAP-vs-spot residual is normalized by a one-minute
        # diffusion scale only as a diagnostic; Phase 9 may replace this model.
        scale = sigma * math.sqrt(60.0)
        if scale > 0:
            sigma_units = residual_log / scale

    common_windows = sorted(set(features.coinbase_returns) & set(features.kraken_returns))
    consensus_values: list[float] = []
    for window in common_windows:
        cb = features.coinbase_returns[window]
        kr = features.kraken_returns[window]
        denom = abs(cb) + abs(kr)
        if denom > 0:
            consensus_values.append((cb + kr) / denom)
    consensus = sum(consensus_values) / len(consensus_values) if consensus_values else 0.0
    consensus = max(-1.0, min(1.0, consensus))

    spread: float | None = None
    if fast_exchange_window_seconds in features.coinbase_returns and fast_exchange_window_seconds in features.kraken_returns:
        spread = (
            features.coinbase_returns[fast_exchange_window_seconds]
            - features.kraken_returns[fast_exchange_window_seconds]
        )

    lead_lag: float | None = None
    needed = (fast_exchange_window_seconds, slow_exchange_window_seconds)
    if all(w in features.coinbase_returns and w in features.kraken_returns for w in needed):
        cb_fast = features.coinbase_returns[fast_exchange_window_seconds]
        kr_fast = features.kraken_returns[fast_exchange_window_seconds]
        cb_slow = features.coinbase_returns[slow_exchange_window_seconds]
        kr_slow = features.kraken_returns[slow_exchange_window_seconds]
        # Signed proxy: positive means Coinbase's recent acceleration leads Kraken
        # in the UP direction; negative is the symmetric DOWN case. It is not
        # interpreted as causal until validated out-of-sample.
        cb_accel = cb_fast - cb_slow * (fast_exchange_window_seconds / slow_exchange_window_seconds)
        kr_accel = kr_fast - kr_slow * (fast_exchange_window_seconds / slow_exchange_window_seconds)
        denom = max(abs(cb_accel) + abs(kr_accel), 1e-12)
        lead_lag = max(-1.0, min(1.0, (cb_accel - kr_accel) / denom))

    return StrategyLabFeatures(
        time_regime=classify_time_regime(features.seconds_remaining),
        volatility_regime=classify_volatility_regime(
            sigma,
            low_cutoff=low_volatility_cutoff,
            high_cutoff=high_volatility_cutoff,
        ),
        volatility_reference=sigma,
        twap_residual_dollars=residual_dollars,
        twap_residual_pct=residual_pct,
        twap_residual_log=residual_log,
        twap_residual_sigma_units=sigma_units,
        coinbase_kraken_basis=features.coinbase_kraken_basis,
        cross_exchange_consensus_score=consensus,
        cross_exchange_return_spread=spread,
        lead_lag_proxy=lead_lag,
    )
