from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from ..types import StrategyFamily


@dataclass(frozen=True)
class SearchDimension:
    name: str
    kind: Literal["float", "int", "choice"]
    low: float | int | None = None
    high: float | int | None = None
    choices: tuple[object, ...] = ()
    log_scale: bool = False

    def __post_init__(self) -> None:
        if self.kind == "choice":
            if not self.choices:
                raise ValueError("choice dimension requires choices")
        else:
            if self.low is None or self.high is None or self.low >= self.high:
                raise ValueError("numeric dimension requires low < high")


@dataclass(frozen=True)
class StrategySearchSpace:
    family: StrategyFamily
    dimensions: tuple[SearchDimension, ...]


def baseline_search_spaces() -> tuple[StrategySearchSpace, ...]:
    """Candidate ranges for Phase 9, not claims of optimal parameters."""
    return (
        StrategySearchSpace(
            StrategyFamily.FAVOURITE,
            (SearchDimension("price_selection_method", "choice", choices=("normalized_midpoint", "best_ask")),),
        ),
        StrategySearchSpace(
            StrategyFamily.FAIR_VALUE,
            (
                SearchDimension("common.min_edge", "float", 0.005, 0.10),
                SearchDimension("common.safety_margin", "float", 0.0, 0.05),
            ),
        ),
        StrategySearchSpace(
            StrategyFamily.MOMENTUM,
            (
                SearchDimension("momentum_short_seconds", "int", 3, 12),
                SearchDimension("momentum_medium_seconds", "int", 10, 30),
                SearchDimension("momentum_long_seconds", "int", 20, 60),
                SearchDimension("minimum_return", "float", 0.00005, 0.002, log_scale=True),
                SearchDimension("minimum_normalized_return", "float", 0.05, 1.5),
                SearchDimension("signal_weight_beta", "float", 0.05, 1.5),
            ),
        ),
        StrategySearchSpace(
            StrategyFamily.MEAN_REVERSION,
            (
                SearchDimension("extension_lookback_seconds", "int", 15, 90),
                SearchDimension("reversal_lookback_seconds", "int", 3, 20),
                SearchDimension("extension_min_return", "float", 0.0001, 0.004, log_scale=True),
                SearchDimension("reversal_min_return", "float", 0.00003, 0.0015, log_scale=True),
                SearchDimension("signal_weight_beta", "float", 0.05, 1.5),
            ),
        ),
        StrategySearchSpace(
            StrategyFamily.BOOK_IMBALANCE,
            (
                SearchDimension("depth_band", "float", 0.005, 0.10),
                SearchDimension("minimum_direction_score", "float", 0.05, 0.80),
                SearchDimension("minimum_total_depth", "float", 1.0, 500.0, log_scale=True),
                SearchDimension("signal_weight_beta", "float", 0.05, 1.5),
            ),
        ),
        StrategySearchSpace(
            StrategyFamily.MICROSTRUCTURE,
            (
                SearchDimension("pm_move_lookback_seconds", "int", 2, 30),
                SearchDimension("pm_min_midpoint_move", "float", 0.002, 0.05, log_scale=True),
                SearchDimension("depth_band", "float", 0.005, 0.10),
                SearchDimension("minimum_pressure_score", "float", 0.02, 0.70),
                SearchDimension("signal_weight_beta", "float", 0.05, 1.5),
            ),
        ),
        StrategySearchSpace(
            StrategyFamily.CROSS_EXCHANGE,
            (
                SearchDimension("exchange_lookback_seconds", "int", 3, 30),
                SearchDimension("coinbase_min_return", "float", 0.00003, 0.002, log_scale=True),
                SearchDimension("kraken_min_return", "float", 0.00003, 0.002, log_scale=True),
                SearchDimension("max_exchange_disagreement", "float", 0.0001, 0.003, log_scale=True),
                SearchDimension("basis_limit", "float", 0.0001, 0.004, log_scale=True),
                SearchDimension("signal_weight_beta", "float", 0.05, 1.5),
            ),
        ),
    )
