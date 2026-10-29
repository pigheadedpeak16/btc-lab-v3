from __future__ import annotations

from ..config import (
    BookImbalanceParams,
    CommonExperimentConfig,
    CrossExchangeParams,
    FairValueParams,
    FavouriteParams,
    MeanReversionParams,
    MicrostructureParams,
    MomentumParams,
    StrategyConfig,
    StrategyLabConfig,
    TournamentConfig,
)
from ..types import StrategyFamily


def comparison_cohort(
    *,
    horizon_seconds: int = 60,
    min_edge: float = 0.03,
    experiment_name: str | None = None,
) -> TournamentConfig:
    common = CommonExperimentConfig(
        decision_seconds_before_close=horizon_seconds,
        min_edge=min_edge,
    )
    strategies = (
        StrategyConfig(strategy_family=StrategyFamily.FAVOURITE, common=common, params=FavouriteParams()),
        StrategyConfig(strategy_family=StrategyFamily.FAIR_VALUE, common=common, params=FairValueParams()),
        StrategyConfig(strategy_family=StrategyFamily.MOMENTUM, common=common, params=MomentumParams()),
        StrategyConfig(strategy_family=StrategyFamily.MEAN_REVERSION, common=common, params=MeanReversionParams()),
        StrategyConfig(strategy_family=StrategyFamily.BOOK_IMBALANCE, common=common, params=BookImbalanceParams()),
        StrategyConfig(strategy_family=StrategyFamily.MICROSTRUCTURE, common=common, params=MicrostructureParams()),
        StrategyConfig(strategy_family=StrategyFamily.CROSS_EXCHANGE, common=common, params=CrossExchangeParams()),
    )
    return TournamentConfig(
        experiment_name=experiment_name or f"comparison_{horizon_seconds}s_edge_{min_edge:.3f}",
        strategies=strategies,
    )


def comparison_matrix(
    *,
    lab: StrategyLabConfig | None = None,
    min_edge: float = 0.03,
) -> tuple[TournamentConfig, ...]:
    """Separate fair-comparison cohorts for each decision horizon.

    Results from different horizons must not be pooled into a single head-to-head
    comparison because they observe different information sets.
    """
    cfg = lab or StrategyLabConfig()
    return tuple(
        comparison_cohort(
            horizon_seconds=horizon,
            min_edge=min_edge,
            experiment_name=f"strategy_lab_{horizon}s_edge_{min_edge:.3f}",
        )
        for horizon in cfg.decision_horizons_seconds
    )
