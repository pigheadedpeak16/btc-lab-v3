from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass

import numpy as np

from ..features.btc import CausalBtcFeatureEngine, CausalBtcFeatures, FeatureUnavailable
from ..feeds.polymarket import DiscoveredMarket
from .calibration_diagnostics import calibration_report

MICROS = 1_000_000
MODEL_POLICY_REVISION = "stage6_causal_twap_v1"


@dataclass(frozen=True)
class SettlementModelConfig:
    paths: int = 10_000
    volatility_lookback_seconds: int = 60
    volatility_floor: float = 1e-8
    volatility_cap: float = 0.02
    twap_window_seconds: int = 60
    step_seconds: int = 1
    seed_version: int = 1
    allow_single_source_known_samples: bool = False

    def __post_init__(self):
        if (type(self.paths) is not int or self.paths <= 0
                or type(self.volatility_lookback_seconds) is not int or self.volatility_lookback_seconds <= 0
                or not math.isfinite(self.volatility_floor) or not math.isfinite(self.volatility_cap)
                or not 0 < self.volatility_floor <= self.volatility_cap
                or self.twap_window_seconds != 60 or self.step_seconds != 1
                or type(self.seed_version) is not int or self.seed_version <= 0
                or type(self.allow_single_source_known_samples) is not bool):
            raise ValueError('MODEL_CONFIG_INVALID')


@dataclass(frozen=True)
class SettlementProbabilityResult:
    p_up: float
    p_down: float
    analytical_p_up: float
    simulated_twap_mean: float
    simulated_twap_std: float
    predictor_price: float
    official_twap: float
    price_to_beat: float
    volatility_per_sqrt_second: float
    oracle_scale: float
    known_final_window_samples: int
    future_final_window_samples: int
    seed: int
    approximation: str
    model_policy_revision: str = MODEL_POLICY_REVISION
    calibration_state: str = "UNCALIBRATED_RESEARCH_ONLY"
    current_sources: tuple[str, ...] = ("coinbase", "kraken")
    mc_standard_error: float = 0.0


def _normal_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def deterministic_seed(slug: str, decision_time_us: int, config: SettlementModelConfig) -> int:
    payload = {
        "model_policy_revision": MODEL_POLICY_REVISION,
        "slug": slug,
        "decision_time_us": decision_time_us,
        "config": config.__dict__,
    }
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).digest()
    return int.from_bytes(digest[:8], "big", signed=False)


class TwapSettlementModel:
    """Initial TWAP-aware benchmark.

    The official Chainlink TWAP and exact PTB are authoritative inputs. Future uncertainty is
    modeled from an availability-aware Coinbase/Kraken composite with zero arithmetic drift.
    Historical one-second proxy samples allow one fresh exchange to carry the composite when the
    other event-driven feed is temporarily quiet, within a bounded age limit. The final oracle TWAP
    is approximated by a 1-second arithmetic rolling window anchored to the latest official TWAP.
    This approximation is explicit; it is not represented as a byte-for-byte Chainlink reimplementation.
    """

    def __init__(self, feature_engine: CausalBtcFeatureEngine, config: SettlementModelConfig | None = None) -> None:
        self.features = feature_engine
        self.config = config or SettlementModelConfig()

    def _volatility(self, features: CausalBtcFeatures) -> float:
        sigma = features.realized_volatility.get(self.config.volatility_lookback_seconds)
        if sigma is None:
            raise FeatureUnavailable("VOLATILITY_WINDOW_UNAVAILABLE")
        if not math.isfinite(sigma) or sigma < 0:
            raise FeatureUnavailable("MODEL_INVALID_VOLATILITY")
        return min(self.config.volatility_cap, max(self.config.volatility_floor, sigma))

    def evaluate(
        self,
        *,
        market: DiscoveredMarket,
        decision_time_us: int,
        features: CausalBtcFeatures | None = None,
    ) -> SettlementProbabilityResult:
        cfg = self.config
        f = features or self.features.build(market=market, decision_time_us=decision_time_us)
        if (f.slug != market.slug or f.decision_time_us != decision_time_us
                or not market.round_start_us <= decision_time_us < market.round_end_us
                or f.seconds_remaining != (market.round_end_us - decision_time_us) / MICROS):
            raise FeatureUnavailable("MODEL_FEATURE_MARKET_OR_TIME_MISMATCH")
        if (not f.current_sources or len(set(f.current_sources)) != len(f.current_sources)
                or not set(f.current_sources) <= {"coinbase", "kraken"}
                or (len(f.current_sources) != 2 and not cfg.allow_single_source_known_samples)):
            raise FeatureUnavailable("MODEL_CURRENT_DUAL_SOURCE_REQUIRED")
        if any(not math.isfinite(x) or x <= 0 for x in
               (f.predictor_price, f.price_to_beat, f.official_twap)):
            raise FeatureUnavailable("MODEL_INVALID_PRICE")
        if not 0 <= decision_time_us - f.official_twap_source_ts_us <= self.features.max_age_us:
            raise FeatureUnavailable("MODEL_OFFICIAL_TWAP_TIME_INVALID")
        sigma = self._volatility(f)
        if cfg.step_seconds != 1 or cfg.twap_window_seconds != 60:
            raise ValueError("Phase-4 benchmark currently requires 1-second samples and a 60-second TWAP")
        if cfg.paths <= 0:
            raise ValueError("paths must be positive")

        window_start_us = market.round_end_us - cfg.twap_window_seconds * MICROS
        sample_times = [window_start_us + i * MICROS for i in range(1, cfg.twap_window_seconds + 1)]
        known_times = [t for t in sample_times if t <= decision_time_us]
        future_times = [t for t in sample_times if t > decision_time_us]

        # The scale must compare the official TWAP with the SAME source-time
        # window, not a newer exchange window observed while the oracle was delayed.
        anchor_us = (f.official_twap_source_ts_us // MICROS) * MICROS
        trailing_start = anchor_us - (cfg.twap_window_seconds - 1) * MICROS
        self.features._lookup_stage = "official_anchor_history"
        trailing = self.features.composite_grid(
            start_us=trailing_start,
            end_us=anchor_us,
            decision_us=decision_time_us,
        )
        if not trailing:
            raise FeatureUnavailable("MISSING_EXTERNAL_HISTORY")
        trailing_mean = sum(p for _, p in trailing) / len(trailing)
        if trailing_mean <= 0:
            raise FeatureUnavailable("INVALID_PRICE")
        oracle_scale = f.official_twap / trailing_mean

        known_values: list[float] = []
        self.features._lookup_stage = "known_final_twap_window"
        for ts_us in known_times:
            known_values.append(
                self.features.composite_price_at(
                    ts_us,
                    decision_time_us,
                    allow_single_source=cfg.allow_single_source_known_samples,
                )
                * oracle_scale
            )

        self.features._lookup_stage = "model_simulation"
        seed = deterministic_seed(market.slug, decision_time_us, cfg)
        rng = np.random.default_rng(seed)
        current = f.predictor_price
        future_offsets = [(t - decision_time_us) / MICROS for t in future_times]
        if future_offsets:
            # Simulate only settlement sample times. The first increment may be
            # fractional; ceil-to-seconds inflated uncertainty for delayed decisions.
            increments = np.diff(np.r_[0.0, future_offsets])
            z = rng.standard_normal((cfg.paths, len(future_offsets)))
            log_inc = (-0.5 * sigma * sigma * increments) + sigma * np.sqrt(increments) * z
            future_path = current * np.exp(np.cumsum(log_inc, axis=1))
        else:
            future_path = np.empty((cfg.paths, 0))

        known_sum = float(sum(known_values))
        if future_offsets:
            future_matrix = future_path * oracle_scale
            final_twap = (known_sum + future_matrix.sum(axis=1)) / cfg.twap_window_seconds
        else:
            final_twap = np.full(cfg.paths, known_sum / cfg.twap_window_seconds)

        p_up = float(np.mean(final_twap >= f.price_to_beat))
        p_up = min(1.0, max(0.0, p_up))

        analytical_mean = (known_sum + len(future_times) * current * oracle_scale) / cfg.twap_window_seconds
        if future_offsets:
            cov_sum = 0.0
            for a in future_offsets:
                for b in future_offsets:
                    cov_sum += min(a, b)
            analytical_sd = current * oracle_scale * sigma * math.sqrt(max(0.0, cov_sum)) / cfg.twap_window_seconds
        else:
            analytical_sd = 0.0
        if analytical_sd <= 1e-15:
            analytical_p = 1.0 if analytical_mean >= f.price_to_beat else 0.0
        else:
            zscore = (f.price_to_beat - analytical_mean) / analytical_sd
            analytical_p = 1.0 - _normal_cdf(zscore)
        analytical_p = min(1.0, max(0.0, analytical_p))

        return SettlementProbabilityResult(
            p_up=p_up,
            p_down=1.0 - p_up,
            analytical_p_up=analytical_p,
            simulated_twap_mean=float(np.mean(final_twap)),
            simulated_twap_std=float(np.std(final_twap)),
            predictor_price=f.predictor_price,
            official_twap=f.official_twap,
            price_to_beat=f.price_to_beat,
            volatility_per_sqrt_second=sigma,
            oracle_scale=oracle_scale,
            known_final_window_samples=len(known_times),
            future_final_window_samples=len(future_times),
            seed=seed,
            approximation="OFFICIAL_TWAP_ANCHORED_1S_AVAILABILITY_AWARE_COMPOSITE_PROXY",
            current_sources=f.current_sources,
            mc_standard_error=math.sqrt(p_up * (1.0 - p_up) / cfg.paths),
        )
