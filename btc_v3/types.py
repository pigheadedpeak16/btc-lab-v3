from __future__ import annotations

from enum import StrEnum


class StrategyFamily(StrEnum):
    FAVOURITE = "favourite"
    FAIR_VALUE = "fair_value"
    MOMENTUM = "fair_value_momentum"
    MEAN_REVERSION = "fair_value_mean_reversion"
    BOOK_IMBALANCE = "fair_value_book_imbalance"
    MICROSTRUCTURE = "fair_value_microstructure"
    CROSS_EXCHANGE = "fair_value_cross_exchange"


class SignalIntegrationMode(StrEnum):
    LOGIT_ADJUSTMENT = "logit_adjustment"
    AGREEMENT_FILTER = "agreement_filter"


class PredictorPriceSource(StrEnum):
    COINBASE = "coinbase"
    KRAKEN = "kraken"
    MEDIAN_COMPOSITE = "median_composite"


class SettlementModelType(StrEnum):
    TWAP_MONTE_CARLO = "twap_monte_carlo"


class VolatilityModel(StrEnum):
    REALIZED_STD = "realized_std"


class Action(StrEnum):
    BUY_UP = "BUY_UP"
    BUY_DOWN = "BUY_DOWN"
    SKIP = "SKIP"


class CausalityTier(StrEnum):
    STRICT_RECEIPT_CAUSAL = "STRICT_RECEIPT_CAUSAL"
    PUBLISH_TIMESTAMP_CAUSAL = "PUBLISH_TIMESTAMP_CAUSAL"
    APPROXIMATE_CAUSAL = "APPROXIMATE_CAUSAL"


class TimeToExpiryRegime(StrEnum):
    EARLY = "EARLY_GT_180S"
    MID = "MID_60_180S"
    FINAL_WINDOW_EARLY = "FINAL_WINDOW_30_60S"
    FINAL_WINDOW_LATE = "FINAL_WINDOW_10_30S"
    FINAL_WINDOW_CRITICAL = "FINAL_WINDOW_0_10S"


class VolatilityRegime(StrEnum):
    LOW = "LOW"
    NORMAL = "NORMAL"
    HIGH = "HIGH"
    UNKNOWN = "UNKNOWN"
