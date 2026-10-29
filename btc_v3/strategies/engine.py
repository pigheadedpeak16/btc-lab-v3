from __future__ import annotations

import math
from dataclasses import dataclass

from ..config import (
    BookImbalanceParams,
    CrossExchangeParams,
    FairValueParams,
    FavouriteParams,
    MeanReversionParams,
    MicrostructureParams,
    MomentumParams,
    StrategyConfig,
)
from ..features.books import MarketBookFeatures
from ..features.btc import CausalBtcFeatures
from ..features.lab import StrategyLabFeatures
from ..types import Action, SignalIntegrationMode, StrategyFamily
from .base import StrategyDecision, StrategySignal
from .signals import (
    MicrostructureInput,
    book_imbalance_signal,
    cross_exchange_signal,
    mean_reversion_signal,
    microstructure_signal,
    momentum_signal,
)


@dataclass(frozen=True)
class StrategyInputs:
    btc: CausalBtcFeatures
    base_p_up: float
    book: MarketBookFeatures
    previous_book: MarketBookFeatures | None = None
    lab: StrategyLabFeatures | None = None


def _clamp_probability(value: float) -> float:
    return max(1e-9, min(1.0 - 1e-9, value))


def probability_to_logit(p_up: float) -> float:
    p = _clamp_probability(p_up)
    return math.log(p / (1.0 - p))


def adjust_probability_logit(base_p_up: float, signal: float, beta: float) -> float:
    adjusted = probability_to_logit(base_p_up) + beta * max(-1.0, min(1.0, signal))
    if adjusted >= 0:
        z = math.exp(-adjusted)
        return 1.0 / (1.0 + z)
    z = math.exp(adjusted)
    return z / (1.0 + z)


class StrategyDecisionEngine:
    """Convert settlement fair value + one incremental signal into a research intent.

    Phase 5 is intentionally not an execution simulator. Fees, delayed book walking,
    AON/FOK behavior, fills, positions, and P&L belong to Phase 6. Regime and
    relative-value diagnostics are attached for attribution but do not silently
    alter baseline strategy probabilities.
    """

    def _signal(self, config: StrategyConfig, inputs: StrategyInputs) -> StrategySignal:
        family = config.strategy_family
        params = config.params
        if family is StrategyFamily.FAIR_VALUE:
            assert isinstance(params, FairValueParams)
            return StrategySignal(family, 0.0, True)
        if family is StrategyFamily.MOMENTUM:
            assert isinstance(params, MomentumParams)
            return momentum_signal(inputs.btc, params)
        if family is StrategyFamily.MEAN_REVERSION:
            assert isinstance(params, MeanReversionParams)
            return mean_reversion_signal(inputs.btc, params)
        if family is StrategyFamily.BOOK_IMBALANCE:
            assert isinstance(params, BookImbalanceParams)
            return book_imbalance_signal(inputs.book, params)
        if family is StrategyFamily.MICROSTRUCTURE:
            assert isinstance(params, MicrostructureParams)
            if inputs.previous_book is None:
                return StrategySignal(family, 0.0, False, "MISSING_BOOK_HISTORY")
            return microstructure_signal(
                MicrostructureInput(current=inputs.book, previous=inputs.previous_book),
                params,
            )
        if family is StrategyFamily.CROSS_EXCHANGE:
            assert isinstance(params, CrossExchangeParams)
            return cross_exchange_signal(inputs.btc, params)
        if family is StrategyFamily.FAVOURITE:
            return StrategySignal(family, 0.0, True)
        raise ValueError(f"unsupported strategy family: {family}")

    @staticmethod
    def _selected_spread(action: Action, book: MarketBookFeatures) -> float:
        return book.up.spread if action is Action.BUY_UP else book.down.spread

    @staticmethod
    def _ask(action: Action, book: MarketBookFeatures) -> float:
        return book.up.best_ask if action is Action.BUY_UP else book.down.best_ask

    @staticmethod
    def _regimes(inputs: StrategyInputs) -> tuple[str | None, str | None]:
        if inputs.lab is None:
            return None, None
        return inputs.lab.time_regime.value, inputs.lab.volatility_regime.value

    def evaluate(self, config: StrategyConfig, inputs: StrategyInputs) -> StrategyDecision:
        common = config.common
        family = config.strategy_family
        base_p = _clamp_probability(inputs.base_p_up)
        signal = self._signal(config, inputs)
        time_regime, vol_regime = self._regimes(inputs)

        if family is StrategyFamily.FAVOURITE:
            assert isinstance(config.params, FavouriteParams)
            up_mid = inputs.book.up.midpoint
            down_mid = inputs.book.down.midpoint
            if config.params.price_selection_method == "best_ask":
                favorite = Action.BUY_UP if inputs.book.up.best_ask >= inputs.book.down.best_ask else Action.BUY_DOWN
            else:
                total_mid = up_mid + down_mid
                if total_mid <= 0:
                    return self._skip(family, "MISSING_BOOK", None, None, signal, inputs.book, time_regime=time_regime, volatility_regime=vol_regime)
                normalized_up = up_mid / total_mid
                favorite = Action.BUY_UP if normalized_up >= 0.5 else Action.BUY_DOWN
            ask = self._ask(favorite, inputs.book)
            spread = self._selected_spread(favorite, inputs.book)
            if spread > common.max_spread:
                return self._skip(family, "SPREAD_TOO_WIDE", None, None, signal, inputs.book, time_regime=time_regime, volatility_regime=vol_regime)
            if ask > common.max_price:
                return self._skip(family, "ASK_TOO_HIGH", None, None, signal, inputs.book, time_regime=time_regime, volatility_regime=vol_regime)
            if ask < common.min_price:
                return self._skip(family, "ASK_TOO_LOW", None, None, signal, inputs.book, time_regime=time_regime, volatility_regime=vol_regime)
            return StrategyDecision(
                family=family,
                action=favorite,
                reason="FAVOURITE_CONTROL",
                base_p_up=None,
                final_p_up=None,
                signal_score=0.0,
                up_ask=inputs.book.up.best_ask,
                down_ask=inputs.book.down.best_ask,
                up_edge=None,
                down_edge=None,
                signal_components=signal.components,
                time_regime=time_regime,
                volatility_regime=vol_regime,
            )

        if not signal.active:
            return self._skip(
                family,
                signal.reason or "SIGNAL_TOO_SMALL",
                base_p,
                base_p,
                signal,
                inputs.book,
                time_regime=time_regime,
                volatility_regime=vol_regime,
            )

        beta: float | None = None
        logit_delta = 0.0
        if family is StrategyFamily.FAIR_VALUE:
            final_p = base_p
        else:
            beta = float(getattr(config.params, "signal_weight_beta"))
            if common.signal_integration_mode is SignalIntegrationMode.LOGIT_ADJUSTMENT:
                logit_delta = beta * signal.score
                final_p = adjust_probability_logit(base_p, signal.score, beta)
            else:
                final_p = base_p

        up_edge = final_p - inputs.book.up.best_ask - common.safety_margin
        down_p = 1.0 - final_p
        down_edge = down_p - inputs.book.down.best_ask - common.safety_margin
        action = Action.BUY_UP if up_edge >= down_edge else Action.BUY_DOWN

        if (
            family is not StrategyFamily.FAIR_VALUE
            and common.signal_integration_mode is SignalIntegrationMode.AGREEMENT_FILTER
        ):
            desired = 1 if action is Action.BUY_UP else -1
            if signal.score * desired <= 0:
                return self._skip(
                    family,
                    "SIGNAL_DISAGREES",
                    base_p,
                    final_p,
                    signal,
                    inputs.book,
                    up_edge,
                    down_edge,
                    beta=beta,
                    logit_delta=logit_delta,
                    time_regime=time_regime,
                    volatility_regime=vol_regime,
                )

        ask = self._ask(action, inputs.book)
        spread = self._selected_spread(action, inputs.book)
        selected_edge = up_edge if action is Action.BUY_UP else down_edge
        if spread > common.max_spread:
            return self._skip(family, "SPREAD_TOO_WIDE", base_p, final_p, signal, inputs.book, up_edge, down_edge, beta=beta, logit_delta=logit_delta, time_regime=time_regime, volatility_regime=vol_regime)
        if ask > common.max_price:
            return self._skip(family, "ASK_TOO_HIGH", base_p, final_p, signal, inputs.book, up_edge, down_edge, beta=beta, logit_delta=logit_delta, time_regime=time_regime, volatility_regime=vol_regime)
        if ask < common.min_price:
            return self._skip(family, "ASK_TOO_LOW", base_p, final_p, signal, inputs.book, up_edge, down_edge, beta=beta, logit_delta=logit_delta, time_regime=time_regime, volatility_regime=vol_regime)
        if selected_edge < common.min_edge:
            return self._skip(family, "EDGE_TOO_SMALL", base_p, final_p, signal, inputs.book, up_edge, down_edge, beta=beta, logit_delta=logit_delta, time_regime=time_regime, volatility_regime=vol_regime)

        return StrategyDecision(
            family=family,
            action=action,
            reason="POSITIVE_GROSS_EDGE",
            base_p_up=base_p,
            final_p_up=final_p,
            signal_score=signal.score,
            up_ask=inputs.book.up.best_ask,
            down_ask=inputs.book.down.best_ask,
            up_edge=up_edge,
            down_edge=down_edge,
            signal_beta=beta,
            logit_delta=logit_delta,
            signal_components=signal.components,
            time_regime=time_regime,
            volatility_regime=vol_regime,
        )

    @staticmethod
    def _skip(
        family: StrategyFamily,
        reason: str,
        base_p: float | None,
        final_p: float | None,
        signal: StrategySignal,
        book: MarketBookFeatures,
        up_edge: float | None = None,
        down_edge: float | None = None,
        *,
        beta: float | None = None,
        logit_delta: float = 0.0,
        time_regime: str | None = None,
        volatility_regime: str | None = None,
    ) -> StrategyDecision:
        return StrategyDecision(
            family=family,
            action=Action.SKIP,
            reason=reason,
            base_p_up=base_p,
            final_p_up=final_p,
            signal_score=signal.score,
            up_ask=book.up.best_ask,
            down_ask=book.down.best_ask,
            up_edge=up_edge,
            down_edge=down_edge,
            signal_beta=beta,
            logit_delta=logit_delta,
            signal_components=signal.components,
            time_regime=time_regime,
            volatility_regime=volatility_regime,
        )
