from __future__ import annotations

import math
from dataclasses import dataclass

from ..config import (
    BookImbalanceParams,
    CrossExchangeParams,
    MeanReversionParams,
    MicrostructureParams,
    MomentumParams,
)
from ..features.books import MarketBookFeatures
from ..features.btc import CausalBtcFeatures
from ..types import StrategyFamily
from .base import SignalComponent, StrategySignal


@dataclass(frozen=True)
class MicrostructureInput:
    current: MarketBookFeatures
    previous: MarketBookFeatures


def _clip(value: float) -> float:
    return max(-1.0, min(1.0, value))


def _sign(value: float, eps: float = 0.0) -> int:
    if value > eps:
        return 1
    if value < -eps:
        return -1
    return 0


def momentum_signal(features: CausalBtcFeatures, params: MomentumParams) -> StrategySignal:
    windows = (
        params.momentum_short_seconds,
        params.momentum_medium_seconds,
        params.momentum_long_seconds,
    )
    try:
        values = [features.returns[w] for w in windows]
    except KeyError:
        return StrategySignal(StrategyFamily.MOMENTUM, 0.0, False, "MISSING_BTC_HISTORY")

    components = tuple(
        SignalComponent(f"return_{window}s", value) for window, value in zip(windows, values)
    )
    signs = [_sign(v, params.minimum_return) for v in values]
    nonzero = [s for s in signs if s != 0]
    if not nonzero:
        return StrategySignal(StrategyFamily.MOMENTUM, 0.0, False, "SIGNAL_TOO_SMALL", components)
    if params.require_direction_agreement and (
        len(nonzero) != len(signs) or len(set(nonzero)) != 1
    ):
        return StrategySignal(StrategyFamily.MOMENTUM, 0.0, False, "SIGNAL_DISAGREES", components)

    vol = features.realized_volatility.get(params.momentum_long_seconds)
    if vol is None:
        vol = features.realized_volatility.get(60)
    if vol is None or vol <= 0:
        return StrategySignal(StrategyFamily.MOMENTUM, 0.0, False, "VOLATILITY_UNAVAILABLE", components)

    representative = sum(values) / len(values)
    horizon = sum(windows) / len(windows)
    denom = max(1e-12, vol * math.sqrt(max(1.0, horizon)))
    normalized = representative / denom
    components += (
        SignalComponent("representative_return", representative),
        SignalComponent("normalized_return", normalized),
        SignalComponent("volatility_per_sqrt_second", vol),
    )
    if abs(representative) < params.minimum_return or abs(normalized) < params.minimum_normalized_return:
        return StrategySignal(StrategyFamily.MOMENTUM, 0.0, False, "SIGNAL_TOO_SMALL", components)
    score = _clip(math.tanh(normalized / 2.0))
    return StrategySignal(StrategyFamily.MOMENTUM, score, True, components=components)


def mean_reversion_signal(features: CausalBtcFeatures, params: MeanReversionParams) -> StrategySignal:
    try:
        extension = features.returns[params.extension_lookback_seconds]
        reversal = features.returns[params.reversal_lookback_seconds]
    except KeyError:
        return StrategySignal(StrategyFamily.MEAN_REVERSION, 0.0, False, "MISSING_BTC_HISTORY")

    components = (
        SignalComponent("extension_return", extension),
        SignalComponent("reversal_return", reversal),
    )
    if abs(extension) < params.extension_min_return or abs(reversal) < params.reversal_min_return:
        return StrategySignal(StrategyFamily.MEAN_REVERSION, 0.0, False, "SIGNAL_TOO_SMALL", components)
    if extension * reversal >= 0:
        return StrategySignal(StrategyFamily.MEAN_REVERSION, 0.0, False, "SIGNAL_DISAGREES", components)

    scale = params.extension_min_return + params.reversal_min_return
    raw = abs(extension) + abs(reversal)
    if params.vol_normalization:
        vol = features.realized_volatility.get(params.extension_lookback_seconds)
        if vol is None:
            vol = features.realized_volatility.get(60)
        if vol is None or vol <= 0:
            return StrategySignal(StrategyFamily.MEAN_REVERSION, 0.0, False, "VOLATILITY_UNAVAILABLE", components)
        raw /= max(1e-12, vol * math.sqrt(params.extension_lookback_seconds))
        scale = 1.0
        components += (SignalComponent("volatility_per_sqrt_second", vol),)
    score = -_sign(extension) * _clip(math.tanh(raw / max(scale, 1e-12)))
    return StrategySignal(StrategyFamily.MEAN_REVERSION, score, True, components=components)


def book_imbalance_signal(book: MarketBookFeatures, params: BookImbalanceParams) -> StrategySignal:
    total = book.up.total_near_depth + book.down.total_near_depth
    components = (
        SignalComponent("up_imbalance", book.up.imbalance),
        SignalComponent("down_imbalance", book.down.imbalance),
        SignalComponent("book_direction_score", book.book_direction_score),
        SignalComponent("total_near_depth", total),
    )
    if total < params.minimum_total_depth:
        return StrategySignal(StrategyFamily.BOOK_IMBALANCE, 0.0, False, "INSUFFICIENT_BOOK_DEPTH", components)
    score = _clip(book.book_direction_score)
    if abs(score) < params.minimum_direction_score:
        return StrategySignal(StrategyFamily.BOOK_IMBALANCE, score, False, "SIGNAL_TOO_SMALL", components)
    return StrategySignal(StrategyFamily.BOOK_IMBALANCE, score, True, components=components)


def microstructure_signal(value: MicrostructureInput, params: MicrostructureParams) -> StrategySignal:
    cur, prev = value.current, value.previous
    up_move = cur.up.midpoint - prev.up.midpoint
    down_move = cur.down.midpoint - prev.down.midpoint
    signed_move = (up_move - down_move) / 2.0
    move_component = 0.0
    if params.pm_min_midpoint_move > 0:
        move_component = _clip(signed_move / params.pm_min_midpoint_move)

    current_pressure = cur.book_direction_score
    previous_pressure = prev.book_direction_score
    pressure_change = _clip(current_pressure - previous_pressure)
    score = _clip(0.50 * move_component + 0.35 * current_pressure + 0.15 * pressure_change)
    components = (
        SignalComponent("signed_midpoint_move", signed_move, 0.50),
        SignalComponent("current_book_pressure", current_pressure, 0.35),
        SignalComponent("pressure_change", pressure_change, 0.15),
    )
    if abs(score) < params.minimum_pressure_score:
        return StrategySignal(StrategyFamily.MICROSTRUCTURE, score, False, "SIGNAL_TOO_SMALL", components)
    return StrategySignal(StrategyFamily.MICROSTRUCTURE, score, True, components=components)


def cross_exchange_signal(features: CausalBtcFeatures, params: CrossExchangeParams) -> StrategySignal:
    w = params.exchange_lookback_seconds
    try:
        cb = features.coinbase_returns[w]
        kr = features.kraken_returns[w]
    except KeyError:
        return StrategySignal(StrategyFamily.CROSS_EXCHANGE, 0.0, False, "MISSING_EXTERNAL_HISTORY")

    components = (
        SignalComponent("coinbase_return", cb),
        SignalComponent("kraken_return", kr),
        SignalComponent("coinbase_kraken_basis", features.coinbase_kraken_basis),
        SignalComponent("return_disagreement", cb - kr),
    )
    if abs(features.coinbase_kraken_basis) > params.basis_limit:
        return StrategySignal(StrategyFamily.CROSS_EXCHANGE, 0.0, False, "EXCHANGE_BASIS_TOO_WIDE", components)
    if abs(cb - kr) > params.max_exchange_disagreement:
        return StrategySignal(StrategyFamily.CROSS_EXCHANGE, 0.0, False, "EXCHANGE_DISAGREEMENT", components)
    if abs(cb) < params.coinbase_min_return or abs(kr) < params.kraken_min_return:
        return StrategySignal(StrategyFamily.CROSS_EXCHANGE, 0.0, False, "SIGNAL_TOO_SMALL", components)
    if cb * kr <= 0:
        return StrategySignal(StrategyFamily.CROSS_EXCHANGE, 0.0, False, "SIGNAL_DISAGREES", components)

    magnitude = (abs(cb) + abs(kr)) / 2.0
    threshold = max(params.coinbase_min_return, params.kraken_min_return, 1e-12)
    score = _sign(cb + kr) * _clip(math.tanh(magnitude / threshold - 1.0))
    return StrategySignal(StrategyFamily.CROSS_EXCHANGE, score, True, components=components)
