import pytest
from pydantic import ValidationError

from btc_v3.config import (
    CommonExperimentConfig,
    FairValueParams,
    MomentumParams,
    StrategyConfig,
    TournamentConfig,
)
from btc_v3.types import StrategyFamily


def test_config_is_frozen() -> None:
    cfg = CommonExperimentConfig(decision_seconds_before_close=60)
    with pytest.raises(ValidationError):
        cfg.min_edge = 0.50  # type: ignore[misc]


def test_invalid_price_range_rejected() -> None:
    with pytest.raises(ValidationError):
        CommonExperimentConfig(
            decision_seconds_before_close=60,
            min_price=0.90,
            max_price=0.20,
        )


def test_wrong_strategy_params_rejected() -> None:
    common = CommonExperimentConfig(decision_seconds_before_close=60)
    with pytest.raises(ValidationError):
        StrategyConfig(
            strategy_family=StrategyFamily.MOMENTUM,
            common=common,
            params=FairValueParams(),
        )


def test_tournament_requires_strategy() -> None:
    with pytest.raises(ValidationError):
        TournamentConfig(experiment_name="empty", strategies=())


def test_valid_momentum_config() -> None:
    cfg = StrategyConfig(
        strategy_family=StrategyFamily.MOMENTUM,
        common=CommonExperimentConfig(decision_seconds_before_close=60),
        params=MomentumParams(),
    )
    assert cfg.strategy_family is StrategyFamily.MOMENTUM
