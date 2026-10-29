from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .types import (
    PredictorPriceSource,
    SettlementModelType,
    SignalIntegrationMode,
    StrategyFamily,
    VolatilityModel,
)

Probability = Annotated[float, Field(ge=0.0, le=1.0)]
PositiveFloat = Annotated[float, Field(gt=0.0)]
NonNegativeFloat = Annotated[float, Field(ge=0.0)]
PositiveInt = Annotated[int, Field(gt=0)]


class FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", validate_assignment=True)


class CommonExperimentConfig(FrozenModel):
    decision_seconds_before_close: PositiveInt
    shares: PositiveFloat = 5.0
    execution_delay_seconds: NonNegativeFloat = 1.0
    max_quote_age_seconds: PositiveFloat = 3.0
    max_decision_gap_seconds: PositiveFloat = 5.0
    slippage_limit: NonNegativeFloat = 0.01
    min_edge: Probability = 0.03
    safety_margin: Probability = 0.0
    min_price: Probability = 0.01
    max_price: Probability = 0.99
    max_spread: Probability = 0.10
    minimum_executable_shares: PositiveFloat = 1.0
    settlement_model_type: SettlementModelType = SettlementModelType.TWAP_MONTE_CARLO
    volatility_model: VolatilityModel = VolatilityModel.REALIZED_STD
    volatility_lookback_seconds: PositiveInt = 60
    volatility_floor: PositiveFloat = 1e-8
    volatility_cap: PositiveFloat = 0.02
    predictor_price_source: PredictorPriceSource = PredictorPriceSource.MEDIAN_COMPOSITE
    signal_integration_mode: SignalIntegrationMode = SignalIntegrationMode.LOGIT_ADJUSTMENT
    monte_carlo_paths: PositiveInt = 10_000
    monte_carlo_step_seconds: PositiveFloat = 1.0
    seed_version: PositiveInt = 1

    @model_validator(mode="after")
    def validate_ranges(self) -> "CommonExperimentConfig":
        if self.min_price >= self.max_price:
            raise ValueError("min_price must be less than max_price")
        if self.volatility_floor >= self.volatility_cap:
            raise ValueError("volatility_floor must be less than volatility_cap")
        return self


class FavouriteParams(FrozenModel):
    price_selection_method: Literal["normalized_midpoint", "best_ask"] = "normalized_midpoint"


class FairValueParams(FrozenModel):
    pass


class MomentumParams(FrozenModel):
    momentum_short_seconds: PositiveInt = 5
    momentum_medium_seconds: PositiveInt = 15
    momentum_long_seconds: PositiveInt = 30
    minimum_return: NonNegativeFloat = 0.0003
    minimum_normalized_return: NonNegativeFloat = 0.25
    require_direction_agreement: bool = True
    signal_weight_beta: NonNegativeFloat = 0.35

    @model_validator(mode="after")
    def validate_windows(self) -> "MomentumParams":
        if not (self.momentum_short_seconds < self.momentum_medium_seconds < self.momentum_long_seconds):
            raise ValueError("momentum windows must be strictly increasing")
        return self


class MeanReversionParams(FrozenModel):
    extension_lookback_seconds: PositiveInt = 30
    extension_min_return: NonNegativeFloat = 0.001
    reversal_lookback_seconds: PositiveInt = 5
    reversal_min_return: NonNegativeFloat = 0.00015
    vol_normalization: bool = True
    signal_weight_beta: NonNegativeFloat = 0.35


class BookImbalanceParams(FrozenModel):
    depth_band: Probability = 0.02
    minimum_direction_score: Probability = 0.30
    minimum_total_depth: NonNegativeFloat = 10.0
    signal_weight_beta: NonNegativeFloat = 0.35


class MicrostructureParams(FrozenModel):
    pm_move_lookback_seconds: PositiveInt = 10
    pm_min_midpoint_move: NonNegativeFloat = 0.01
    depth_band: Probability = 0.02
    minimum_pressure_score: Probability = 0.15
    signal_weight_beta: NonNegativeFloat = 0.35


class CrossExchangeParams(FrozenModel):
    exchange_lookback_seconds: PositiveInt = 10
    coinbase_min_return: NonNegativeFloat = 0.00035
    kraken_min_return: NonNegativeFloat = 0.00035
    max_exchange_disagreement: NonNegativeFloat = 0.0006
    basis_limit: NonNegativeFloat = 0.001
    signal_weight_beta: NonNegativeFloat = 0.35


StrategyParams = (
    FavouriteParams
    | FairValueParams
    | MomentumParams
    | MeanReversionParams
    | BookImbalanceParams
    | MicrostructureParams
    | CrossExchangeParams
)


class StrategyConfig(FrozenModel):
    strategy_family: StrategyFamily
    common: CommonExperimentConfig
    params: StrategyParams

    @staticmethod
    def _expected_params() -> dict[StrategyFamily, type[FrozenModel]]:
        return {
            StrategyFamily.FAVOURITE: FavouriteParams,
            StrategyFamily.FAIR_VALUE: FairValueParams,
            StrategyFamily.MOMENTUM: MomentumParams,
            StrategyFamily.MEAN_REVERSION: MeanReversionParams,
            StrategyFamily.BOOK_IMBALANCE: BookImbalanceParams,
            StrategyFamily.MICROSTRUCTURE: MicrostructureParams,
            StrategyFamily.CROSS_EXCHANGE: CrossExchangeParams,
        }

    @model_validator(mode="before")
    @classmethod
    def parse_params_for_family(cls, value: object) -> object:
        if not isinstance(value, dict):
            return value
        raw_family = value.get("strategy_family")
        raw_params = value.get("params")
        if raw_family is None or not isinstance(raw_params, dict):
            return value
        family = raw_family if isinstance(raw_family, StrategyFamily) else StrategyFamily(raw_family)
        expected_type = cls._expected_params()[family]
        parsed = dict(value)
        parsed["params"] = expected_type.model_validate(raw_params)
        return parsed

    @model_validator(mode="after")
    def validate_param_family(self) -> "StrategyConfig":
        expected_type = self._expected_params()[self.strategy_family]
        if not isinstance(self.params, expected_type):
            raise ValueError(
                f"{self.strategy_family.value} requires params type {expected_type.__name__}"
            )
        return self


class TournamentConfig(FrozenModel):
    experiment_name: Annotated[str, Field(min_length=1, max_length=120)]
    strategies: tuple[StrategyConfig, ...]
    schema_version: PositiveInt = 1

    @model_validator(mode="after")
    def require_strategies(self) -> "TournamentConfig":
        if not self.strategies:
            raise ValueError("at least one strategy config is required")
        return self


class StrategyLabConfig(FrozenModel):
    """Research-only feature/regime settings; defaults are not claimed optimal."""

    decision_horizons_seconds: tuple[PositiveInt, ...] = (240, 180, 120, 60, 30, 15)
    volatility_regime_window_seconds: PositiveInt = 60
    low_volatility_cutoff: PositiveFloat = 0.00004
    high_volatility_cutoff: PositiveFloat = 0.00012
    fast_exchange_window_seconds: PositiveInt = 5
    slow_exchange_window_seconds: PositiveInt = 30

    @model_validator(mode="after")
    def validate_lab(self) -> "StrategyLabConfig":
        if len(set(self.decision_horizons_seconds)) != len(self.decision_horizons_seconds):
            raise ValueError("decision horizons must be unique")
        if tuple(sorted(self.decision_horizons_seconds, reverse=True)) != self.decision_horizons_seconds:
            raise ValueError("decision horizons must be strictly descending")
        if self.low_volatility_cutoff >= self.high_volatility_cutoff:
            raise ValueError("low volatility cutoff must be below high cutoff")
        if self.fast_exchange_window_seconds >= self.slow_exchange_window_seconds:
            raise ValueError("fast exchange window must be shorter than slow exchange window")
        return self
