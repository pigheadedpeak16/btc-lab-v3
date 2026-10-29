from __future__ import annotations

from dataclasses import dataclass

from ..types import Action, StrategyFamily


@dataclass(frozen=True)
class SignalComponent:
    name: str
    value: float
    weight: float = 1.0


@dataclass(frozen=True)
class StrategySignal:
    family: StrategyFamily
    score: float
    active: bool
    reason: str | None = None
    components: tuple[SignalComponent, ...] = ()

    def __post_init__(self) -> None:
        if not -1.0 <= self.score <= 1.0:
            raise ValueError("signal score must be in [-1, 1]")


@dataclass(frozen=True)
class StrategyDecision:
    family: StrategyFamily
    action: Action
    reason: str
    base_p_up: float | None
    final_p_up: float | None
    signal_score: float
    up_ask: float | None
    down_ask: float | None
    up_edge: float | None
    down_edge: float | None
    signal_beta: float | None = None
    logit_delta: float = 0.0
    signal_components: tuple[SignalComponent, ...] = ()
    time_regime: str | None = None
    volatility_regime: str | None = None
