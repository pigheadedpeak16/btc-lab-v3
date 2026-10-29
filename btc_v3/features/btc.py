from __future__ import annotations

import bisect
import math
from dataclasses import dataclass
from decimal import Decimal

from ..feeds.polymarket import DiscoveredMarket
from ..storage.external_store import ExternalStore, PriceReference, positive_price

MICROS = 1_000_000


class FeatureUnavailable(RuntimeError):
    pass


@dataclass(frozen=True)
class CausalBtcFeatures:
    slug: str
    decision_time_us: int
    seconds_remaining: float
    price_reference: PriceReference
    price_to_beat: float
    coinbase_price: float
    kraken_price: float
    predictor_price: float
    official_twap: float
    official_twap_source_ts_us: int
    distance_dollars: float
    distance_pct: float
    log_distance: float
    returns: dict[int, float]
    coinbase_returns: dict[int, float]
    kraken_returns: dict[int, float]
    coinbase_kraken_basis: float
    realized_volatility: dict[int, float]
    current_sources: tuple[str, ...] = ("coinbase", "kraken")


class CausalBtcFeatureEngine:
    """Build strictly causal BTC features from locally observed external feeds.

    Point-in-time decision prices are strict: Coinbase and Kraken must each have a
    sufficiently recent source tick. Historical one-second grids are different. A
    trade-driven source can legitimately be unchanged for several seconds, so the
    grid uses the fresh sources available at each timestamp, with a bounded carry
    limit. This avoids treating a quiet Kraken trade stream as a dead feed while
    still failing closed when *all* predictor sources are stale.
    """

    def __init__(
        self,
        store: ExternalStore,
        *,
        max_source_age_seconds: float = 5.0,
        historical_grid_max_age_seconds: float = 15.0,
        live_prefetch_lookback_seconds: int = 180,
    ) -> None:
        self.store = store
        self.max_age_us = int(max_source_age_seconds * MICROS)
        self.grid_max_age_us = int(historical_grid_max_age_seconds * MICROS)
        self.live_prefetch_lookback_seconds = max(120, int(live_prefetch_lookback_seconds))
        # Decision-scoped bulk cache.  The old implementation issued hundreds of
        # SQLite point queries for every telemetry sample (volatility grids, return
        # grids, and TWAP-model grids).  On a multi-million-row external DB those
        # synchronous scans starved the asyncio feed collectors for 30-90 seconds.
        self._cache_decision_us: int | None = None
        self._cache_start_us: int | None = None
        self._cache_rows: dict[str, list[tuple[int, int, int, str]]] = {}
        self._cache_times: dict[str, list[int]] = {}
        self.last_unavailable_context: dict[str, object] | None = None
        self._lookup_stage: str = "unspecified"

    def prefetch(self, *, decision_us: int, lookback_seconds: int | None = None) -> None:
        """Bulk-load recent causal ticks once for a decision timestamp.

        The cache includes the historical carry allowance before the requested
        lookback so a 1-second grid can still use the latest sufficiently fresh
        trade-driven tick at its first timestamp.
        """
        lookback = max(self.live_prefetch_lookback_seconds, int(lookback_seconds or 0))
        start_us = decision_us - lookback * MICROS - self.grid_max_age_us - MICROS
        if (
            self._cache_decision_us == decision_us
            and self._cache_start_us is not None
            and self._cache_start_us <= start_us
        ):
            return
        rows: dict[str, list[tuple[int, int, int, str]]] = {}
        times: dict[str, list[int]] = {}
        for source in ("coinbase", "kraken"):
            data = self.store.tick_window(
                source=source,
                start_source_ts_us=start_us,
                end_source_ts_us=decision_us,
                decision_time_us=decision_us,
            )
            rows[source] = data
            times[source] = [r[0] for r in data]
        self._cache_decision_us = decision_us
        self._cache_start_us = start_us
        self._cache_rows = rows
        self._cache_times = times

    def _cached_tick(
        self,
        source: str,
        target_us: int,
        decision_us: int,
    ) -> tuple[int, int, str] | None:
        if self._cache_decision_us != decision_us or self._cache_start_us is None or target_us < self._cache_start_us:
            return None
        times = self._cache_times.get(source) or []
        rows = self._cache_rows.get(source) or []
        i = bisect.bisect_right(times, target_us) - 1
        if i < 0:
            return None
        ts_us, received_at_us, _row_id, price = rows[i]
        return ts_us, received_at_us, price

    def _price_at(
        self,
        source: str,
        target_us: int,
        decision_us: int,
        *,
        max_age_us: int | None = None,
    ) -> float:
        row = self._cached_tick(source, target_us, decision_us)
        if row is None:
            row = self.store.latest_tick(
                source=source,
                target_source_ts_us=target_us,
                decision_time_us=decision_us,
            )
        if row is None:
            self.last_unavailable_context = {
                "source": source, "stage": self._lookup_stage,
                "reason": "MISSING_HISTORY", "target_ts_us": target_us,
                "decision_ts_us": decision_us,
                "allowed_age_us": self.max_age_us if max_age_us is None else max_age_us,
            }
            raise FeatureUnavailable(f"MISSING_{source.upper()}_HISTORY")
        source_ts_us, _received_at_us, price = row
        allowed_age = self.max_age_us if max_age_us is None else max_age_us
        if target_us - source_ts_us > allowed_age:
            self.last_unavailable_context = {
                "source": source, "stage": self._lookup_stage,
                "reason": "STALE_HISTORY", "target_ts_us": target_us,
                "decision_ts_us": decision_us,
                "observed_source_ts_us": source_ts_us,
                "observed_receive_ts_us": _received_at_us,
                "age_us": target_us - source_ts_us,
                "allowed_age_us": allowed_age,
            }
            raise FeatureUnavailable(f"STALE_{source.upper()}_HISTORY")
        if not positive_price(price) or not math.isfinite(float(Decimal(price))):
            raise FeatureUnavailable(f"INVALID_{source.upper()}_PRICE")
        return float(Decimal(price))

    def _optional_price_at(
        self,
        source: str,
        target_us: int,
        decision_us: int,
        *,
        max_age_us: int,
    ) -> float | None:
        try:
            return self._price_at(
                source,
                target_us,
                decision_us,
                max_age_us=max_age_us,
            )
        except FeatureUnavailable:
            return None

    def composite_price_at(
        self,
        target_us: int,
        decision_us: int,
        *,
        allow_single_source: bool = False,
        max_age_us: int | None = None,
    ) -> float:
        if not allow_single_source:
            cb = self._price_at("coinbase", target_us, decision_us, max_age_us=max_age_us)
            kr = self._price_at("kraken", target_us, decision_us, max_age_us=max_age_us)
            return (cb + kr) / 2.0

        allowed_age = self.grid_max_age_us if max_age_us is None else max_age_us
        values: list[float] = []
        failures: dict[str, object] = {}
        for source in ("coinbase", "kraken"):
            value = self._optional_price_at(
                source,
                target_us,
                decision_us,
                max_age_us=allowed_age,
            )
            if value is not None:
                values.append(value)
            else:
                failures[source] = self.last_unavailable_context
        if not values:
            self.last_unavailable_context = {
                "reason": "MISSING_EXTERNAL_HISTORY", "stage": self._lookup_stage,
                "target_ts_us": target_us, "decision_ts_us": decision_us,
                "allowed_age_us": allowed_age, "sources": failures,
            }
            raise FeatureUnavailable("MISSING_EXTERNAL_HISTORY")
        return sum(values) / len(values)

    def composite_grid(
        self,
        *,
        start_us: int,
        end_us: int,
        decision_us: int,
        step_seconds: int = 1,
        allow_single_source: bool = True,
        max_age_seconds: float | None = None,
    ) -> list[tuple[int, float]]:
        if end_us < start_us:
            return []
        step_us = step_seconds * MICROS
        first = ((start_us + step_us - 1) // step_us) * step_us
        max_age_us = (
            self.grid_max_age_us
            if max_age_seconds is None
            else int(max_age_seconds * MICROS)
        )
        result: list[tuple[int, float]] = []
        for ts_us in range(first, end_us + 1, step_us):
            result.append(
                (
                    ts_us,
                    self.composite_price_at(
                        ts_us,
                        decision_us,
                        allow_single_source=allow_single_source,
                        max_age_us=max_age_us,
                    ),
                )
            )
        return result

    def realized_volatility(self, *, decision_us: int, lookback_seconds: int) -> float:
        start_us = decision_us - lookback_seconds * MICROS
        series = self.composite_grid(
            start_us=start_us,
            end_us=decision_us,
            decision_us=decision_us,
            allow_single_source=True,
        )
        if len(series) < max(6, lookback_seconds // 4):
            raise FeatureUnavailable("VOLATILITY_UNAVAILABLE")
        returns: list[float] = []
        for (_, p0), (_, p1) in zip(series, series[1:]):
            if p0 <= 0 or p1 <= 0:
                continue
            returns.append(math.log(p1 / p0))
        if len(returns) < 5:
            raise FeatureUnavailable("VOLATILITY_UNAVAILABLE")
        mean = sum(returns) / len(returns)
        variance = sum((r - mean) ** 2 for r in returns) / max(1, len(returns) - 1)
        return math.sqrt(max(0.0, variance))

    def build(
        self,
        *,
        market: DiscoveredMarket,
        decision_time_us: int,
        return_windows: tuple[int, ...] = (5, 10, 15, 30, 60),
        volatility_windows: tuple[int, ...] = (30, 60, 120),
    ) -> CausalBtcFeatures:
        self.last_unavailable_context = None
        self._lookup_stage = "current_and_feature_history"
        if decision_time_us < market.round_start_us:
            raise FeatureUnavailable("DECISION_BEFORE_ROUND_START")
        if decision_time_us >= market.round_end_us:
            raise FeatureUnavailable("DECISION_AFTER_ROUND_END")
        max_requested = max((0, *return_windows, *volatility_windows, 60))
        self.prefetch(decision_us=decision_time_us, lookback_seconds=max_requested)
        reference = self.store.causal_price_reference(market.slug, decision_time_us)
        if reference is None:
            raise FeatureUnavailable("NO_CAUSAL_PRICE_REFERENCE")
        if (reference.reference_ts_us != market.round_start_us
                or reference.round_start_us != market.round_start_us
                or reference.round_end_us != market.round_end_us):
            raise FeatureUnavailable("WRONG_BOUNDARY_PRICE_REFERENCE")

        # Current decision state remains strict: both exchanges must be fresh.
        cb = self._price_at("coinbase", decision_time_us, decision_time_us)
        kr = self._price_at("kraken", decision_time_us, decision_time_us)
        predictor = (cb + kr) / 2.0

        twap_row = self.store.latest_settlement_observation(
            target_source_ts_us=decision_time_us,
            decision_time_us=decision_time_us,
        )
        if twap_row is None:
            raise FeatureUnavailable("MISSING_OFFICIAL_TWAP")
        twap_source_ts_us, _twap_received_us, twap_value = twap_row
        if decision_time_us - twap_source_ts_us > self.max_age_us:
            raise FeatureUnavailable("STALE_OFFICIAL_TWAP")
        twap = float(Decimal(twap_value))

        ptb = float(Decimal(reference.price_to_beat))
        returns: dict[int, float] = {}
        coinbase_returns: dict[int, float] = {}
        kraken_returns: dict[int, float] = {}
        for window in return_windows:
            target_us = decision_time_us - window * MICROS
            old = self.composite_price_at(
                target_us,
                decision_time_us,
                allow_single_source=True,
            )
            returns[window] = math.log(predictor / old)

            cb_old = self._optional_price_at(
                "coinbase",
                target_us,
                decision_time_us,
                max_age_us=self.grid_max_age_us,
            )
            kr_old = self._optional_price_at(
                "kraken",
                target_us,
                decision_time_us,
                max_age_us=self.grid_max_age_us,
            )
            if cb_old is not None:
                coinbase_returns[window] = math.log(cb / cb_old)
            if kr_old is not None:
                kraken_returns[window] = math.log(kr / kr_old)

        vols: dict[int, float] = {}
        for window in volatility_windows:
            try:
                vols[window] = self.realized_volatility(
                    decision_us=decision_time_us,
                    lookback_seconds=window,
                )
            except FeatureUnavailable:
                continue

        if predictor <= 0 or ptb <= 0:
            raise FeatureUnavailable("INVALID_PRICE")
        distance = predictor - ptb
        return CausalBtcFeatures(
            slug=market.slug,
            decision_time_us=decision_time_us,
            seconds_remaining=(market.round_end_us - decision_time_us) / MICROS,
            price_reference=reference,
            price_to_beat=ptb,
            coinbase_price=cb,
            kraken_price=kr,
            predictor_price=predictor,
            official_twap=twap,
            official_twap_source_ts_us=twap_source_ts_us,
            distance_dollars=distance,
            distance_pct=distance / ptb,
            log_distance=math.log(predictor / ptb),
            returns=returns,
            coinbase_returns=coinbase_returns,
            kraken_returns=kraken_returns,
            coinbase_kraken_basis=(cb - kr) / predictor,
            realized_volatility=vols,
            current_sources=("coinbase", "kraken"),
        )

    def build_available_sources(
        self,
        *,
        market: DiscoveredMarket,
        decision_time_us: int,
        return_windows: tuple[int, ...] = (5, 10, 15, 30, 60),
        volatility_windows: tuple[int, ...] = (30, 60, 120),
    ) -> CausalBtcFeatures:
        """Build a causal fallback state from whichever current predictor is fresh.

        This method exists for Phase 5.9's strategy-specific readiness.  It never
        fabricates an external tick and it fails closed if both exchanges are stale.
        The missing current exchange is represented by the available predictor only
        to preserve the historical feature dataclass contract; ``current_sources``
        records the exact feeds that were actually fresh.  Cross-exchange strategies
        must therefore require both current sources and are never evaluated on this
        single-source fallback.
        """
        self.last_unavailable_context = None
        self._lookup_stage = "available_source_feature_history"
        if decision_time_us < market.round_start_us:
            raise FeatureUnavailable("DECISION_BEFORE_ROUND_START")
        if decision_time_us >= market.round_end_us:
            raise FeatureUnavailable("DECISION_AFTER_ROUND_END")
        max_requested = max((0, *return_windows, *volatility_windows, 60))
        self.prefetch(decision_us=decision_time_us, lookback_seconds=max_requested)
        reference = self.store.causal_price_reference(market.slug, decision_time_us)
        if reference is None:
            raise FeatureUnavailable("NO_CAUSAL_PRICE_REFERENCE")
        if (reference.reference_ts_us != market.round_start_us
                or reference.round_start_us != market.round_start_us
                or reference.round_end_us != market.round_end_us):
            raise FeatureUnavailable("WRONG_BOUNDARY_PRICE_REFERENCE")

        cb = self._optional_price_at(
            "coinbase", decision_time_us, decision_time_us, max_age_us=self.max_age_us
        )
        kr = self._optional_price_at(
            "kraken", decision_time_us, decision_time_us, max_age_us=self.max_age_us
        )
        available: list[tuple[str, float]] = []
        if cb is not None:
            available.append(("coinbase", cb))
        if kr is not None:
            available.append(("kraken", kr))
        if not available:
            raise FeatureUnavailable("MISSING_CURRENT_PREDICTOR")
        predictor = sum(v for _, v in available) / len(available)

        twap_row = self.store.latest_settlement_observation(
            target_source_ts_us=decision_time_us,
            decision_time_us=decision_time_us,
        )
        if twap_row is None:
            raise FeatureUnavailable("MISSING_OFFICIAL_TWAP")
        twap_source_ts_us, _twap_received_us, twap_value = twap_row
        if decision_time_us - twap_source_ts_us > self.max_age_us:
            raise FeatureUnavailable("STALE_OFFICIAL_TWAP")
        twap = float(Decimal(twap_value))

        ptb = float(Decimal(reference.price_to_beat))
        returns: dict[int, float] = {}
        coinbase_returns: dict[int, float] = {}
        kraken_returns: dict[int, float] = {}
        for window in return_windows:
            target_us = decision_time_us - window * MICROS
            old = self.composite_price_at(
                target_us,
                decision_time_us,
                allow_single_source=True,
            )
            returns[window] = math.log(predictor / old)
            if cb is not None:
                cb_old = self._optional_price_at(
                    "coinbase", target_us, decision_time_us, max_age_us=self.grid_max_age_us
                )
                if cb_old is not None:
                    coinbase_returns[window] = math.log(cb / cb_old)
            if kr is not None:
                kr_old = self._optional_price_at(
                    "kraken", target_us, decision_time_us, max_age_us=self.grid_max_age_us
                )
                if kr_old is not None:
                    kraken_returns[window] = math.log(kr / kr_old)

        vols: dict[int, float] = {}
        for window in volatility_windows:
            try:
                vols[window] = self.realized_volatility(
                    decision_us=decision_time_us,
                    lookback_seconds=window,
                )
            except FeatureUnavailable:
                continue

        if predictor <= 0 or ptb <= 0:
            raise FeatureUnavailable("INVALID_PRICE")
        cb_display = predictor if cb is None else cb
        kr_display = predictor if kr is None else kr
        basis = ((cb - kr) / predictor) if cb is not None and kr is not None else 0.0
        distance = predictor - ptb
        return CausalBtcFeatures(
            slug=market.slug,
            decision_time_us=decision_time_us,
            seconds_remaining=(market.round_end_us - decision_time_us) / MICROS,
            price_reference=reference,
            price_to_beat=ptb,
            coinbase_price=cb_display,
            kraken_price=kr_display,
            predictor_price=predictor,
            official_twap=twap,
            official_twap_source_ts_us=twap_source_ts_us,
            distance_dollars=distance,
            distance_pct=distance / ptb,
            log_distance=math.log(predictor / ptb),
            returns=returns,
            coinbase_returns=coinbase_returns,
            kraken_returns=kraken_returns,
            coinbase_kraken_basis=basis,
            realized_volatility=vols,
            current_sources=tuple(name for name, _ in available),
        )
