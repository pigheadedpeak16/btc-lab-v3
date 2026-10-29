from __future__ import annotations

import asyncio
import hashlib
import json
import math
import statistics
import sys
import time
import uuid
from contextlib import contextmanager
from copy import copy, deepcopy
from functools import wraps
from collections import deque,Counter
from dataclasses import asdict, dataclass, field, replace
from decimal import Decimal, ROUND_HALF_UP, ROUND_DOWN, ROUND_UP
from pathlib import Path
from typing import Any, Iterable, Literal

import httpx

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
)
from ..features.books import (
    MarketBookFeatures,
    boundary_completed_market_book_features,
    market_book_features,
)
from ..features.btc import CausalBtcFeatureEngine, FeatureUnavailable
from ..features.lab import build_strategy_lab_features
from ..models.settlement import SettlementModelConfig, TwapSettlementModel, MODEL_POLICY_REVISION
from ..feeds.official_resolution import RESOLUTION_POLICY_REVISION
from ..strategies.edge_gate import EdgeGateConfig, assess_executable_edge
from ..strategies.paper_bankroll import PaperSessionBudget
from ..strategies.engine import StrategyDecisionEngine, StrategyInputs
from ..types import Action, StrategyFamily
from ..feeds.coinbase import CoinbaseCollector
from ..feeds.kraken import KrakenCollector
from ..feeds.polymarket import DiscoveredMarket, PolymarketPublicRest
from ..feeds.polymarket_collector import FEED_POLICY_REVISION,EXECUTION_ADMISSION_REVISION
from ..feeds.clock_health import RuntimeClockHealth
from ..feeds.settlement_reference import AuthenticatedTwapCollector, PriceReferenceMapper
from ..storage.external_store import ExternalStore,REFERENCE_INTEGRITY_POLICY_REVISION
from ..storage.market_store import MarketStore,QUOTE_PROJECTION_POLICY_REVISION
from ..timeutil import epoch_us_to_iso, now_epoch_us
from .client import DATA_API_BASE, PolymarketDataClient, _loads_decimal
from .forward_store import ForwardWalletStore
# Independent shadow arms only; persistent account/selector operation is retired.
ACCOUNTING_POLICY_REVISION = 'research_shadow_ledger_v1'
from .latency import OverlapCollectorPool
from .live import _sample_from_state
from .models import WalletActivity, WalletTrade
from .store import ForensicsStore

MICROS = 1_000_000
FEE_QUANTUM = Decimal("0.00001")
PHASE_VERSION = "5.9.5"
EXECUTION_POLICY_REVISION = "stage4_execution_realism_v1"
SHARE_QUANTUM = Decimal("0.01")  # Share-denominated limit/FAK submissions, not partial fills.
SUPPORTED_TICKS = {Decimal(x) for x in (".1", ".01", ".005", ".0025", ".001", ".0001")}


def _tick_limit(state, price, side):
    tick = state.tick_size
    if tick not in SUPPORTED_TICKS:
        return price  # Legacy non-strict replay only; strict callers fail closed.
    rounded = (price / tick).to_integral_value(rounding=ROUND_DOWN if side == "BUY" else ROUND_UP) * tick
    return min(rounded, Decimal(1) - tick) if side == "BUY" else max(rounded, tick)


def _termination_state(error: BaseException | None) -> str:
    """Persist truthful terminal status without logging secrets or raw tracebacks."""
    if error is None or isinstance(error, (KeyboardInterrupt, asyncio.CancelledError, SystemExit)):
        return "STOPPED"
    if isinstance(error, RuntimeError) and str(error).startswith(("LOW_DISK_SPACE_GUARD", "CLOCK_HEALTH_LOST","CLOCK_SYNC_UNVERIFIED")):
        return "GUARD_STOPPED"
    return "CRASHED"

STORAGE_PROFILE = "compact_raw_ref_v1"
STRATEGY_8 = "S8_PAIRED_GRID"
STRATEGY_9A = "S9A_LITERAL_MIRROR"
STRATEGY_9B = "S9B_PROTECTED_MIRROR"
STRATEGY_9_FIRST2 = "S9_FIRST2"
STRATEGY_9_FIRST3 = "S9_FIRST3"
STRATEGY_9_ONE_SIDED = "S9_ONE_SIDED"
STRATEGY_9_EARLY = "S9_EARLY_GT180"
STRATEGY_9_EARLY_LARGE = "S9_EARLY_LARGE_GE24"
STRATEGY_9_DET3 = "S9_DET3"
STRATEGY_9_DET4 = "S9_DET4"
STRATEGY_9_DET5 = "S9_DET5"
STRATEGY_9_DET7_5 = "S9_DET7_5"
STRATEGY_9_BOOK_CONFIRM_75 = "S9_BOOK_CONFIRM_75"
STRATEGY_9_FIRST3_LARGE = "S9_FIRST3_LARGE_GE24"
STRATEGY_9_FIRST3_LARGE_DET7_5 = "S9_FIRST3_LARGE_DET7_5"
STRATEGY_9_LARGE_DET7_5 = "S9_LARGE_DET7_5"
STRATEGY_9_BOOK_MODEL30 = "S9_BOOK_MODEL30"
STRATEGY_9_BOOK_MODEL40 = "S9_BOOK_MODEL40"
STRATEGY_9_ROUTER_A = "S9_ROUTER_A"
STRATEGY_9_MODEL60 = "S9_MODEL60"
STRATEGY_9_MODEL65 = "S9_MODEL65"
STRATEGY_9_BOOK_MODEL60 = "S9_BOOK_MODEL60"
STRATEGY_9_BOOK_MODEL65 = "S9_BOOK_MODEL65"
STRATEGY_9_PRICE60_80 = "S9_PRICE60_80"
STRATEGY_9_ROUTER_V1 = "S9_ROUTER_V1"
STRATEGY_9_EDGE_BOOK_V1 = "S9_EDGE_BOOK_V1"  # frozen 5.9.3 control
STRATEGY_9_EDGE_BOOK_V2 = "S9_EDGE_BOOK_V2"  # separately sized, session-budgeted paper challenger

PHASE590_WALLET_STRATEGIES = (
    STRATEGY_9A,
    STRATEGY_9B,
    STRATEGY_9_FIRST2,
    STRATEGY_9_FIRST3,
    STRATEGY_9_ONE_SIDED,
    STRATEGY_9_EARLY,
    STRATEGY_9_EARLY_LARGE,
    STRATEGY_9_DET3,
    STRATEGY_9_DET4,
    STRATEGY_9_DET5,
    STRATEGY_9_DET7_5,
    STRATEGY_9_BOOK_CONFIRM_75,
    STRATEGY_9_FIRST3_LARGE,
    STRATEGY_9_FIRST3_LARGE_DET7_5,
    STRATEGY_9_LARGE_DET7_5,
    STRATEGY_9_BOOK_MODEL30,
    STRATEGY_9_BOOK_MODEL40,
    STRATEGY_9_ROUTER_A,
    STRATEGY_9_MODEL60,
    STRATEGY_9_MODEL65,
    STRATEGY_9_BOOK_MODEL60,
    STRATEGY_9_BOOK_MODEL65,
    STRATEGY_9_PRICE60_80,
    STRATEGY_9_ROUTER_V1,
)


def phase591_config_hash(config: "ForwardLabConfig") -> str:
    payload = {
        "phase": PHASE_VERSION,
        "execution_policy_revision": EXECUTION_POLICY_REVISION,
        "execution_admission_policy_revision": EXECUTION_ADMISSION_REVISION,
        "accounting_policy_revision": ACCOUNTING_POLICY_REVISION,
        "model_policy_revision": MODEL_POLICY_REVISION,
        "settlement_policy_revision": RESOLUTION_POLICY_REVISION,
        "feed_policy_revision": FEED_POLICY_REVISION,
        "quote_projection_policy_revision": QUOTE_PROJECTION_POLICY_REVISION,
        "reference_integrity_policy_revision": REFERENCE_INTEGRITY_POLICY_REVISION,
        "resource_policy_revision": "offline_durable_retention_v1",
        "storage_profile": STORAGE_PROFILE,
        "config": asdict(config),
        "wallet_strategies": list(PHASE590_WALLET_STRATEGIES) + [STRATEGY_9_EDGE_BOOK_V1, STRATEGY_9_EDGE_BOOK_V2],
        "tournament_horizons": list(TOURNAMENT_HORIZONS) if "TOURNAMENT_HORIZONS" in globals() else [240, 180, 120, 60, 30, 15],
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()


# Backwards-compatible name retained for older local tests/tools.
def phase590_config_hash(config: "ForwardLabConfig") -> str:
    return phase591_config_hash(config)


@dataclass(frozen=True)
class ForwardLabConfig:
    # Explicit opt-in to an existing account; never initialized by a session.
    trades_poll_ms: int = 100
    activity_poll_ms: int = 250
    telemetry_ms: int = 1000
    discovery_ms: int = 1000
    discovery_timeout_seconds: float = 2.5
    model_paths: int = 500
    recent_window_seconds: int = 30
    rollover_grace_seconds: float = 45.0
    copy_execution_delay_ms: int = 25

    # Strategy 8 -- inferred paired-grid maker.
    grid_prices: tuple[float, ...] = (0.05, 0.15, 0.25, 0.35, 0.45, 0.55, 0.65, 0.75, 0.85, 0.95)
    grid_clip_shares: float = 48.0
    grid_orders_per_side: int = 1
    min_pair_edge: float = 0.05
    min_directional_edge: float = 0.03
    unpaired_max_price: float = 0.45
    max_unpaired_shares: float = 48.0
    max_total_round_notional: float = 100.0
    stop_new_unpaired_seconds: float = 60.0
    stop_pairing_seconds: float = 15.0
    maker_order_max_age_seconds: float = 90.0

    # Strategy 9A/9B paper-copy controls.
    copy_max_order_notional: float = 50.0
    protected_max_deterioration: float = 0.03
    protected_max_publication_lag_lower_seconds: float = 5.0
    protected_max_spread: float = 0.10
    protected_min_top_depth: float = 1.0

    # Phase 5.9 prospectively frozen wallet-copy hypotheses.
    early_min_seconds_remaining: float = 180.0
    large_signal_min_shares: float = 24.0
    book_confirm_delay_ms: int = 75
    book_confirm_execution_delay_ms: int = 25
    book_confirm_min_direction_score: float = 0.20

    # Phase 5.9.2 -- frozen after the 5.9.1 out-of-sample forensic review.
    # These are challengers, not claims of optimized live profitability.
    book_model30_min_target_probability: float = 0.30
    book_model40_min_target_probability: float = 0.40
    router_model_min_target_probability: float = 0.30
    model60_min_target_probability: float = 0.60
    model65_min_target_probability: float = 0.65
    router_v1_min_target_probability: float = 0.60
    price_band_min: float = 0.60
    price_band_max: float = 0.80
    router_specialist_max_deterioration: float = 0.075

    # Phase 5.9.3 experimental paper-only challenger. Fixed prospective gates;
    # the MC probability is NOT yet empirically calibrated. The haircut is a
    # policy stress margin, not a fitted confidence bound.
    edge_v1_probability_haircut: float = 0.08
    edge_v1_slippage_allowance: float = 0.02
    edge_v1_min_net_edge: float = 0.03

    # False is retained for isolated legacy replay/test fixtures only.
    # New forward sessions require True before opening stores or clients.
    strict_market_minimum: bool = False
    # Legacy grid definition remains available for explicitly opted-in research.
    # Operational CLI excludes it until useful maker participation is established.
    s8_research_policy: str = 'LEGACY_GRID_V1'

    # Current documented crypto taker fee coefficient; configurable for replay.
    taker_fee_rate: float = 0.07
    max_book_age_seconds: float = 2.0

    # Phase 5.7 full forward tournament.  Signal profiles are frozen before the run.
    enable_full_tournament: bool = False
    tournament_execution_delay_ms: int = 100
    tournament_max_decision_gap_seconds: float = 5.0
    tournament_history_ms: int = 500

    def validate(self) -> None:
        if self.s8_research_policy not in {'LEGACY_GRID_V1','DISABLED_V1'}:
            raise ValueError('S8_RESEARCH_POLICY_UNSUPPORTED')
        if self.trades_poll_ms < 50:
            raise ValueError("trades_poll_ms must be >= 50")
        if self.tournament_execution_delay_ms < 0:
            raise ValueError("tournament_execution_delay_ms must be >= 0")
        if self.tournament_max_decision_gap_seconds <= 0:
            raise ValueError("tournament_max_decision_gap_seconds must be positive")
        if self.tournament_history_ms < 250:
            raise ValueError("tournament_history_ms must be >= 250")
        if self.activity_poll_ms < 100:
            raise ValueError("activity_poll_ms must be >= 100")
        if self.telemetry_ms < 250:
            raise ValueError("telemetry_ms must be >= 250")
        if self.discovery_ms < 500:
            raise ValueError("discovery_ms must be >= 500")
        if self.discovery_timeout_seconds <= 0:
            raise ValueError("discovery_timeout_seconds must be positive")
        if self.model_paths < 100:
            raise ValueError("model_paths must be >= 100")
        if self.recent_window_seconds < 5:
            raise ValueError("recent_window_seconds must be >= 5")
        if self.rollover_grace_seconds < 20:
            raise ValueError("rollover_grace_seconds must be >= 20 for delayed wallet attribution")
        if self.copy_execution_delay_ms < 0:
            raise ValueError("copy_execution_delay_ms must be >= 0")
        if self.book_confirm_delay_ms < 0 or self.book_confirm_execution_delay_ms < 0:
            raise ValueError("book-confirm delays must be >= 0")
        if self.early_min_seconds_remaining <= 0:
            raise ValueError("early_min_seconds_remaining must be positive")
        if self.large_signal_min_shares <= 0:
            raise ValueError("large_signal_min_shares must be positive")
        if not 0 <= self.book_confirm_min_direction_score <= 1:
            raise ValueError("book_confirm_min_direction_score must be in [0,1]")
        for name in (
            "book_model30_min_target_probability",
            "book_model40_min_target_probability",
            "router_model_min_target_probability",
            "model60_min_target_probability",
            "model65_min_target_probability",
            "router_v1_min_target_probability",
            "price_band_min",
            "price_band_max",
        ):
            value = float(getattr(self, name))
            if not 0 <= value <= 1:
                raise ValueError(f"{name} must be in [0,1]")
        if not 0 <= self.edge_v1_probability_haircut < 1:
            raise ValueError("edge_v1_probability_haircut must be in [0,1)")
        if not 0 <= self.edge_v1_slippage_allowance < 1:
            raise ValueError("edge_v1_slippage_allowance must be in [0,1)")
        if not 0 < self.edge_v1_min_net_edge < 1:
            raise ValueError("edge_v1_min_net_edge must be in (0,1)")
        if not self.price_band_min < self.price_band_max:
            raise ValueError("price band min must be below max")
        if self.router_specialist_max_deterioration < 0:
            raise ValueError("router_specialist_max_deterioration must be non-negative")
        if not self.grid_prices:
            raise ValueError("grid_prices cannot be empty")
        if tuple(sorted(set(self.grid_prices))) != self.grid_prices:
            raise ValueError("grid_prices must be strictly increasing and unique")
        if any(p <= 0 or p >= 1 for p in self.grid_prices):
            raise ValueError("grid prices must be strictly between 0 and 1")
        if self.grid_clip_shares <= 0 or self.grid_orders_per_side <= 0:
            raise ValueError("grid clip/order count must be positive")
        for name in (
            "min_pair_edge",
            "min_directional_edge",
            "unpaired_max_price",
            "protected_max_deterioration",
            "protected_max_spread",
            "taker_fee_rate",
        ):
            value = float(getattr(self, name))
            if value < 0:
                raise ValueError(f"{name} must be non-negative")
        if self.unpaired_max_price >= 1:
            raise ValueError("unpaired_max_price must be < 1")
        if self.max_unpaired_shares <= 0 or self.copy_max_order_notional <= 0:
            raise ValueError("risk caps must be positive")


@dataclass(frozen=True)
class PollBatch:
    source: str
    request_started_us: int
    response_received_us: int
    rtt_us: int
    status_code: int | None
    rows: tuple[WalletTrade | WalletActivity, ...]
    error_text: str | None = None


class FastAttributionPoller:
    """Two public wallet endpoints, no retry, persistent HTTP/2 when available.

    The goal is to race public attribution paths rather than assume /v2/trades is
    always the fastest.  429/503 responses remain visible measurements.
    """

    def __init__(self, *, timeout_seconds: float = 3.0) -> None:
        self.client = httpx.AsyncClient(
            timeout=timeout_seconds,
            headers={"Accept": "application/json", "User-Agent": "btc-lab-v3-forward-wallet/0.5.7.5"},
            follow_redirects=True,
            http2=True,
        )

    async def __aenter__(self) -> "FastAttributionPoller":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.client.aclose()

    async def poll(self, *, source: Literal["trades", "activity"], wallet: str, start_epoch_s: int) -> PollBatch:
        request_started_us = time.time_ns() // 1000
        mono = time.perf_counter_ns()
        rows: list[WalletTrade | WalletActivity] = []
        status: int | None = None
        error: str | None = None
        try:
            if source == "trades":
                path = "/v2/trades"
                params = {
                    "user": wallet,
                    "start": start_epoch_s,
                    "end": 0,
                    "limit": 100,
                    "taker_only": "false",
                }
            else:
                path = "/v2/activity"
                params = {
                    "user": wallet,
                    "start": start_epoch_s,
                    "end": 0,
                    "limit": 100,
                    "sort_direction": "DESC",
                }
            response = await self.client.get(f"{DATA_API_BASE}{path}", params=params)
            status = response.status_code
            if status == 200:
                payload = _loads_decimal(response.text)
                raw_rows = payload.get("data") if isinstance(payload, dict) else None
                if isinstance(raw_rows, list):
                    if source == "trades":
                        rows = [WalletTrade.from_api(wallet, r) for r in raw_rows if isinstance(r, dict)]
                    else:
                        rows = [WalletActivity.from_api(wallet, r) for r in raw_rows if isinstance(r, dict)]
            else:
                error = response.text[:400]
        except httpx.HTTPError as exc:
            error = f"{type(exc).__name__}: {exc}"
        received_us = time.time_ns() // 1000
        rtt_us = max(0, (time.perf_counter_ns() - mono) // 1000)
        return PollBatch(source, request_started_us, received_us, rtt_us, status, tuple(rows), error)


def _fmt_decimal(value: Decimal | float | int | None) -> str | None:
    if value is None:
        return None
    return format(Decimal(str(value)), "f")


def _row_id(row: WalletTrade | WalletActivity) -> str:
    return row.trade_id if isinstance(row, WalletTrade) else row.activity_id


def _row_is_trade(row: WalletTrade | WalletActivity) -> bool:
    return isinstance(row, WalletTrade) or row.is_trade


def _row_is_btc(row: WalletTrade | WalletActivity) -> bool:
    return row.is_btc_5m


def _row_slug(row: WalletTrade | WalletActivity) -> str | None:
    return row.slug or row.event_slug


def economic_key(row: WalletTrade | WalletActivity) -> str:
    """Conservative economic dedupe key used to avoid double-copying API aliases."""
    payload = {
        "tx": (row.transaction_hash or "").lower(),
        "slug": (_row_slug(row) or "").lower(),
        "token": row.token_id or "",
        "side": (row.side or "").upper(),
        "outcome": (row.outcome or "").upper(),
        "price": _fmt_decimal(row.price),
        "size": _fmt_decimal(row.size),
        "trade_second": None if row.timestamp_us is None else int(row.timestamp_us // MICROS),
    }
    text = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode()).hexdigest()


def publication_lag_bounds_us(trade_ts_us: int | None, first_seen_us: int) -> tuple[int | None, int | None]:
    if trade_ts_us is None:
        return None, None
    # Public rows observed in our capture use second-resolution timestamps. Treat
    # that second as [t, t+1s); the lower bound is therefore first_seen-(t+1s).
    lower = max(0, first_seen_us - (trade_ts_us + MICROS - 1))
    upper = max(0, first_seen_us - trade_ts_us)
    return lower, upper


def crypto_taker_fee(*, shares: Decimal, price: Decimal, rate: Decimal, exponent: int = 1) -> Decimal:
    if shares <= 0 or rate <= 0:
        return Decimal("0")
    if exponent < 0:
        raise ValueError("fee exponent must be non-negative")
    curve = price * (Decimal("1") - price)
    raw = shares * rate * (curve ** exponent)
    if raw < FEE_QUANTUM / 2:
        return Decimal("0")
    return raw.quantize(FEE_QUANTUM, rounding=ROUND_HALF_UP)


@dataclass(frozen=True)
class TakerFillLevel:
    price: Decimal
    shares: Decimal
    notional: Decimal
    fee: Decimal


@dataclass(frozen=True)
class TakerPlan:
    requested_shares: Decimal
    filled_shares: Decimal
    vwap: Decimal | None
    notional: Decimal
    fee: Decimal
    levels: tuple[TakerFillLevel, ...]


def walk_taker_fak(
    state: Any,
    *,
    side: str,
    shares: Decimal,
    fee_rate: Decimal,
    fee_exponent: int = 1,
    max_price: Decimal | None = None,
    min_price: Decimal | None = None,
) -> TakerPlan:
    """Walk current L2 as an FAK paper order. Partial fills are allowed."""
    if shares <= 0:
        raise ValueError("shares must be positive")
    if not state.depth_reliable:
        raise ValueError("BOOK_UNSYNCHRONIZED")
    order_side = side.upper()
    remaining = shares
    levels: list[TakerFillLevel] = []
    total = Decimal("0")
    fees = Decimal("0")
    if order_side == "BUY":
        book = [(p, state.asks[p]) for p in sorted(state.asks)]
        for price, available in book:
            if max_price is not None and price > max_price:
                break
            qty = min(remaining, available)
            if qty <= 0:
                continue
            notional = qty * price
            fee = crypto_taker_fee(shares=qty, price=price, rate=fee_rate, exponent=fee_exponent)
            levels.append(TakerFillLevel(price, qty, notional, fee))
            total += notional
            fees += fee
            remaining -= qty
            if remaining <= 0:
                break
    elif order_side == "SELL":
        book = [(p, state.bids[p]) for p in sorted(state.bids, reverse=True)]
        for price, available in book:
            if min_price is not None and price < min_price:
                break
            qty = min(remaining, available)
            if qty <= 0:
                continue
            notional = qty * price
            fee = crypto_taker_fee(shares=qty, price=price, rate=fee_rate, exponent=fee_exponent)
            levels.append(TakerFillLevel(price, qty, notional, fee))
            total += notional
            fees += fee
            remaining -= qty
            if remaining <= 0:
                break
    else:
        raise ValueError("side must be BUY or SELL")
    filled = shares - remaining
    vwap = (total / filled) if filled > 0 else None
    return TakerPlan(shares, filled, vwap, total, fees, tuple(levels))


def _sized_taker_plan(state, *, shares, side, fee_rate, fee_exponent=1,
                      max_price=None, min_price=None, max_notional=None, max_cost=None):
    """Size on executable L2 and rounded per-level fees, without spending a cap.

    Binary search uses integer share increments. No heuristic fee multiplier or
    best-price-only funding estimate; partial executions retain their true size.
    """
    if not shares.is_finite():
        return Decimal(0), None
    shares = shares.quantize(SHARE_QUANTUM, rounding=ROUND_DOWN)
    if shares <= 0:
        return shares, None

    def plan_for(qty):
        return walk_taker_fak(state, side=side, shares=qty, fee_rate=fee_rate,
            fee_exponent=fee_exponent, max_price=max_price, min_price=min_price)

    def affordable(plan):
        return ((max_notional is None or plan.notional <= max_notional)
                and (max_cost is None or plan.notional + plan.fee <= max_cost))

    plan = plan_for(shares)
    if affordable(plan):
        return shares, plan
    low, high = 0, int(shares / SHARE_QUANTUM)
    while low < high:
        middle = (low + high + 1) // 2
        if affordable(plan_for(middle * SHARE_QUANTUM)):
            low = middle
        else:
            high = middle - 1
    submitted = low * SHARE_QUANTUM
    return submitted, None if submitted <= 0 else plan_for(submitted)


@dataclass
class RoundPosition:
    slug: str
    round_start_us: int
    round_end_us: int
    up_shares: Decimal = Decimal("0")
    down_shares: Decimal = Decimal("0")
    up_buy_cost: Decimal = Decimal("0")
    down_buy_cost: Decimal = Decimal("0")
    sell_proceeds: Decimal = Decimal("0")
    fees: Decimal = Decimal("0")
    cash_flow: Decimal = Decimal("0")
    filled_orders: int = 0
    settled_side: str | None = None
    settled_at_us: int | None = None
    pnl: Decimal | None = None
    up_remaining_basis: Decimal = Decimal("0")
    down_remaining_basis: Decimal = Decimal("0")
    settlement_evidence: dict[str,Any] = field(default_factory=dict)

    def shares(self, outcome: str) -> Decimal:
        return self.up_shares if outcome.upper() == "UP" else self.down_shares

    def avg_buy(self, outcome: str) -> Decimal | None:
        shares = self.shares(outcome)
        if shares <= 0:
            return None
        cost = self.up_remaining_basis if outcome.upper() == "UP" else self.down_remaining_basis
        return cost / shares

    def record_fill(self, *, outcome: str, side: str, shares: Decimal, notional: Decimal, fee: Decimal) -> None:
        if self.pnl is not None or self.settled_at_us is not None:
            raise ValueError("POSITION_SETTLED")
        outcome = outcome.upper()
        side = side.upper()
        if side == "BUY":
            if outcome == "UP":
                self.up_shares += shares
                self.up_buy_cost += notional
                self.up_remaining_basis += notional
            else:
                self.down_shares += shares
                self.down_buy_cost += notional
                self.down_remaining_basis += notional
            self.cash_flow -= notional + fee
        elif side == "SELL":
            if outcome == "UP":
                if shares > self.up_shares:
                    raise ValueError("SELL_EXCEEDS_UP_POSITION")
                self.up_remaining_basis *= (self.up_shares - shares) / self.up_shares
                self.up_shares -= shares
            else:
                if shares > self.down_shares:
                    raise ValueError("SELL_EXCEEDS_DOWN_POSITION")
                self.down_remaining_basis *= (self.down_shares - shares) / self.down_shares
                self.down_shares -= shares
            self.sell_proceeds += notional
            self.cash_flow += notional - fee
        else:
            raise ValueError("unsupported fill side")
        self.fees += fee
        self.filled_orders += 1

    @property
    def total_buy_cost(self) -> Decimal:
        return self.up_buy_cost + self.down_buy_cost

    @property
    def matched_shares(self) -> Decimal:
        return min(self.up_shares, self.down_shares)

    @property
    def avg_pair_cost(self) -> Decimal | None:
        up = self.avg_buy("UP")
        down = self.avg_buy("DOWN")
        return None if up is None or down is None else up + down

    def settle(self, *, resolved_side: str, at_us: int) -> Decimal:
        if self.pnl is not None:
            return self.pnl
        payout = self.up_shares if resolved_side.upper() == "UP" else self.down_shares
        self.settled_side = resolved_side.upper()
        self.settled_at_us = at_us
        self.pnl = self.cash_flow + payout
        return self.pnl


@dataclass
class MakerOrder:
    order_id: str
    slug: str
    token_id: str
    outcome: str
    price: Decimal
    original_shares: Decimal
    remaining_shares: Decimal
    queue_ahead: Decimal
    created_at_us: int
    status: str = "OPEN"
    min_order_size: Decimal | None = None
    tick_size: Decimal | None = None


@dataclass
class PendingCopy:
    order_id: str
    strategy: str
    signal_id: str
    slug: str
    token_id: str
    outcome: str
    side: str
    target_price: Decimal | None
    target_size: Decimal
    lower_lag_us: int | None
    created_at_us: int
    scheduled_at_us: int
    deterioration_cap: Decimal | None = None
    require_book_confirmation: bool = False
    confirmed_at_us: int | None = None
    confirmation_score: float | None = None
    confirmation_context: dict[str, Any] | None = None
    model_target_probability: float | None = None
    router_stage: str | None = None
    executed_at_us: int | None = None


@dataclass
class WalletMarketSignalState:
    slug: str
    signal_count: int = 0
    first_outcome: str | None = None
    outcomes_seen: set[str] = field(default_factory=set)
    two_sided_started: bool = False
    cumulative_up_shares: Decimal = Decimal("0")
    cumulative_down_shares: Decimal = Decimal("0")

    def context_before(self, *, outcome: str, size: Decimal) -> dict[str, Any]:
        normalized = outcome.upper()
        ordinal = self.signal_count + 1
        opposite_seen = bool(self.outcomes_seen and normalized not in self.outcomes_seen)
        return {
            "signal_ordinal": ordinal,
            "first_outcome": self.first_outcome,
            "outcomes_seen_before": sorted(self.outcomes_seen),
            "opposite_seen_before": opposite_seen,
            "two_sided_started_before": self.two_sided_started,
            "target_cumulative_up_before": _fmt_decimal(self.cumulative_up_shares),
            "target_cumulative_down_before": _fmt_decimal(self.cumulative_down_shares),
            "signal_size": _fmt_decimal(size),
        }

    def observe(self, *, outcome: str, size: Decimal) -> None:
        normalized = outcome.upper()
        self.signal_count += 1
        if self.first_outcome is None:
            self.first_outcome = normalized
        elif normalized != self.first_outcome:
            self.two_sided_started = True
        self.outcomes_seen.add(normalized)
        if normalized == "UP":
            self.cumulative_up_shares += size
        elif normalized == "DOWN":
            self.cumulative_down_shares += size


def protected_copy_reason(
    *,
    side: str,
    target_price: Decimal | None,
    executable_price: Decimal | None,
    spread: Decimal | None,
    top_depth: Decimal | None,
    lower_lag_us: int | None,
    config: ForwardLabConfig,
) -> str | None:
    if target_price is None or executable_price is None:
        return "MISSING_PRICE"
    if lower_lag_us is not None and lower_lag_us > int(config.protected_max_publication_lag_lower_seconds * MICROS):
        return "PUBLICATION_TOO_LATE"
    if spread is None or spread > Decimal(str(config.protected_max_spread)):
        return "SPREAD_TOO_WIDE"
    if top_depth is None or top_depth < Decimal(str(config.protected_min_top_depth)):
        return "TOP_DEPTH_TOO_SMALL"
    max_det = Decimal(str(config.protected_max_deterioration))
    if side.upper() == "BUY" and executable_price - target_price > max_det:
        return "PRICE_DETERIORATED"
    if side.upper() == "SELL" and target_price - executable_price > max_det:
        return "PRICE_DETERIORATED"
    return None


def _state_for_outcome(collector: Any, outcome: str) -> Any | None:
    if collector is None:
        return None
    wanted = outcome.upper()
    return next((s for s in collector.states.values() if s.outcome.upper() == wanted), None)


def _clob_tx_clock(market_store: MarketStore, tx_hash: str | None, slug: str | None) -> tuple[int | None, int | None]:
    if not tx_hash:
        return None, None
    params: list[Any] = [tx_hash.lower()]
    query = "SELECT id,received_at_us FROM market_trades WHERE lower(source_trade_id)=?"
    if slug:
        query += " AND slug=?"
        params.append(slug)
    query += " ORDER BY received_at_us ASC LIMIT 1"
    row = market_store.conn.execute(query, params).fetchone()
    if row is None:
        return None, None
    return int(row[0]), int(row[1])


def _atomic_paper_update(method):
    """Roll back touched memory together with a synchronous SQLite unit of work."""
    @wraps(method)
    def wrapped(owner, *args, **kwargs):
        engine = getattr(owner, "paper_engine", owner)
        if engine._atomic_depth:
            return method(owner, *args, **kwargs)
        name = method.__name__
        tracked = []
        if name == "_execute_copy":
            tracked = [args[0] if args else kwargs["pending"]]
            keys = {(p.strategy, p.slug) for p in tracked}
        elif name in {"execute_due_copies", "execute_due"}:
            pending = owner.pending if name == "execute_due" else engine.pending_copies
            tracked = [p for p in pending if p.scheduled_at_us <= kwargs["now_us"]]
            keys = {(p.strategy, p.slug) for p in tracked}
        elif name == "refresh_strategy8":
            keys = {(STRATEGY_8, kwargs["market"].slug)}
        elif name == "process_market_trades":
            keys = {(STRATEGY_8, o.slug) for o in engine.maker_orders.values()}
        else:  # settle_ready: finalized historical positions need no snapshot.
            keys = {key for key, p in engine.positions.items()
                    if p.pnl is None and p.round_end_us <= kwargs["now_us"]}
        if not keys:
            return method(owner, *args, **kwargs)
        tournament_pending = list(owner.pending) if owner is not engine else None
        try:
            with engine.atomic_update(keys, tracked):
                return method(owner, *args, **kwargs)
        except BaseException:
            if tournament_pending is not None:
                owner.pending = tournament_pending
            raise
    return wrapped


class ForwardPaperEngine:
    def __init__(self, *, session_id: str, config: ForwardLabConfig, store: ForwardWalletStore, execution_clock=None) -> None:
        self.session_id = session_id
        self.config = config
        self.store = store
        self.positions: dict[tuple[str, str], RoundPosition] = {}
        self.maker_orders: dict[str, MakerOrder] = {}
        self.maker_cancellations = Counter()
        self.pending_copies: list[PendingCopy] = []
        self.last_market_trade_id = 0
        self.wallet_signal_state: dict[str, WalletMarketSignalState] = {}
        self.market_fee_rates: dict[str, tuple[Decimal, int, str]] = {}
        self.edge_v2_bankroll = PaperSessionBudget()
        self.edge_v2_costs_by_slug: dict[str, Decimal] = {}
        self.execution_clock = execution_clock
        self._atomic_depth = 0
        # Independent counterfactual arms consume separate virtual liquidity.
        # Equal snapshots do not replenish it; only observed size increases do.
        self._liquidity: dict[tuple, tuple[dict, dict]] = {}
        self._maker_prints: dict[str, set[tuple]] = {}

    @contextmanager
    def atomic_update(self, keys, tracked):
        # Snapshot only touched positions and small live queues, never history.
        positions = {key: self.positions.get(key) for key in keys}
        objects = [p for p in positions.values() if p is not None]
        objects += list(self.maker_orders.values()) + tracked + [self.edge_v2_bankroll]
        snapshots = [(obj, deepcopy(vars(obj))) for obj in objects]
        makers = dict(self.maker_orders)
        maker_cancellations = self.maker_cancellations.copy()
        pending = list(self.pending_copies)
        costs = dict(self.edge_v2_costs_by_slug)
        cursor = self.last_market_trade_id
        liquidity = {key: deepcopy(value) for key, value in self._liquidity.items()
                     if key[:2] in keys}
        maker_slugs = {slug for strategy, slug in keys if strategy == STRATEGY_8}
        prints = {slug: set(values) for slug, values in self._maker_prints.items() if slug in maker_slugs}
        self._atomic_depth += 1
        try:
            with self.store.transaction():
                yield
        except BaseException:
            for obj, state in snapshots:
                vars(obj).clear()
                vars(obj).update(state)
            for key, pos in positions.items():
                if pos is None:
                    self.positions.pop(key, None)
                else:
                    self.positions[key] = pos
            self.maker_orders = makers
            self.maker_cancellations = maker_cancellations
            self.pending_copies = pending
            self.edge_v2_costs_by_slug = costs
            self.last_market_trade_id = cursor
            for key in list(self._liquidity):
                if key[:2] in keys:
                    self._liquidity.pop(key)
            self._liquidity.update(liquidity)
            for slug in maker_slugs:
                self._maker_prints.pop(slug, None)
            self._maker_prints.update(prints)
            raise
        finally:
            self._atomic_depth -= 1

    def execution_time(self, simulated_us: int | None = None) -> int:
        """Live runner supplies a UTC clock; explicit times are an isolated replay/test seam."""
        if self.execution_clock is not None:
            return self.execution_clock()
        return now_epoch_us() if simulated_us is None else simulated_us

    @staticmethod
    def position_execution_reason(pos: RoundPosition | None, at_us: int) -> str | None:
        if pos is None:
            return "UNKNOWN_MARKET"
        if pos.pnl is not None or pos.settled_at_us is not None:
            return "POSITION_SETTLED"
        if at_us >= pos.round_end_us:
            return "MARKET_EXPIRED"
        if at_us < pos.round_start_us:
            return "MARKET_NOT_OPEN"
        return None

    @staticmethod
    def execution_identity_reason(*, slug, token_id, outcome, state, collector, market_store):
        row = market_store.conn.execute(
            "SELECT token_id FROM market_tokens WHERE slug=? AND side=?", (slug, outcome)
        ).fetchone()
        collected = getattr(collector, "market", None)
        if (outcome not in {"UP", "DOWN"} or row is None or row[0] != token_id
                or (state is not None and (state.slug != slug or state.token_id != token_id
                                           or state.outcome != outcome))
                or (collected is not None and (collected.slug != slug
                    or collected.outcome_for_token(token_id) != outcome))):
            return "EXECUTION_MARKET_MISMATCH"
        return None

    @staticmethod
    def paired_book_execution_reason(collector,at_us,max_age,*,market_store=None,slug=None):
        market=getattr(collector,'market',None)
        slug=slug or getattr(market,'slug',None)
        if slug is None:return 'PAIRED_BOOK_UNAVAILABLE'
        for outcome in ('UP','DOWN'):
            state=_state_for_outcome(collector,outcome)
            if (state is None or state.slug!=slug or state.outcome!=outcome or not state.depth_reliable
                or state.best_bid is None or state.best_ask is None):return 'PAIRED_BOOK_UNAVAILABLE'
            if market_store is not None:
                row=market_store.conn.execute('SELECT token_id FROM market_tokens WHERE slug=? AND side=?',(slug,outcome)).fetchone()
                if row is None or row[0]!=state.token_id:return 'PAIRED_BOOK_UNAVAILABLE'
            age=state.age_seconds(at_us)
            if age is None or age>max_age:return 'PAIRED_BOOK_STALE'
        return None

    def _liquidity_view(self, strategy, state, side):
        key = (strategy, state.slug, state.token_id, side)
        observed = dict(state.asks if side == "BUY" else state.bids)
        prior, remaining = self._liquidity.get(key, ({}, {}))
        available = {price: max(Decimal(0), min(qty,
            remaining.get(price, Decimal(0)) + qty - prior.get(price, Decimal(0))))
            for price, qty in observed.items()}
        view = copy(state)
        if side == "BUY": view.asks = available
        else: view.bids = available
        return key, observed, available, view

    def _consume_liquidity(self, preview, plan):
        key, observed, available, _ = preview
        remaining = dict(available)
        for level in plan.levels:
            remaining[level.price] -= level.shares
        self._liquidity[key] = (observed, remaining)

    def set_market_fee(self, *, slug: str, rate: Decimal, exponent: int, source: str) -> None:
        self.market_fee_rates[slug] = (rate, exponent, source)

    def taker_fee_rate_for_slug(self, slug: str) -> tuple[Decimal, int, str]:
        value = self.market_fee_rates.get(slug)
        if value is not None:
            return value
        return Decimal(str(self.config.taker_fee_rate)), 1, "CONFIG_FALLBACK"

    def position(self, strategy: str, market: DiscoveredMarket) -> RoundPosition:
        key = (strategy, market.slug)
        if key not in self.positions:
            self.positions[key] = self._recover_position(strategy,market.slug,market.round_start_us,market.round_end_us)
        return self.positions[key]

    def _recover_position(self,strategy,slug,start,end):
        row=self.store.conn.execute('''SELECT up_shares,down_shares,up_cost,down_cost,sell_proceeds,fees,
            settled_side,settled_at_us,pnl,details_json FROM wallet_forward_rounds
            WHERE session_id=? AND strategy=? AND slug=?''',(self.session_id,strategy,slug)).fetchone()
        if row is None:return RoundPosition(slug,start,end)
        # An open position needs complete order/liquidity reconciliation, never a
        # guessed zero balance. Final positions can safely be hydrated read-only.
        if row[8] is None or row[7] is None:raise ValueError('POSITION_STATE_RECOVERY_REQUIRED')
        details=json.loads(row[9]);up,down=Decimal(row[0]),Decimal(row[1])
        return RoundPosition(slug,start,end,up_shares=up,down_shares=down,
            up_buy_cost=Decimal(row[2]),down_buy_cost=Decimal(row[3]),sell_proceeds=Decimal(row[4]),fees=Decimal(row[5]),
            cash_flow=Decimal(details['cash_flow']),filled_orders=details['filled_orders'],settled_side=row[6],
            settled_at_us=row[7],pnl=Decimal(row[8]),
            up_remaining_basis=Decimal(details.get('up_remaining_basis') or '0') if 'up_remaining_basis' in details else Decimal(details.get('avg_up_buy') or '0')*up,
            down_remaining_basis=Decimal(details.get('down_remaining_basis') or '0') if 'down_remaining_basis' in details else Decimal(details.get('avg_down_buy') or '0')*down,
            settlement_evidence=details.get('settlement_evidence',{}))

    def prune_memory(self,*,now_us,limit=10000):
        if self._atomic_depth:raise RuntimeError('RETENTION_DURING_TRANSACTION')
        retired=set()
        for key,pos in list(self.positions.items()):
            if pos.pnl is None or pos.round_end_us+600_000_000>=now_us:continue
            final=self.store.conn.execute('''SELECT pnl,settled_at_us FROM wallet_forward_rounds
                WHERE session_id=? AND strategy=? AND slug=?''',(self.session_id,*key)).fetchone()
            if final is None or final[0] is None or Decimal(final[0])!=pos.pnl or final[1]!=pos.settled_at_us:continue
            self.positions.pop(key);retired.add(pos.slug)
            for liquidity_key in list(self._liquidity):
                if liquidity_key[:2]==key:self._liquidity.pop(liquidity_key)
        busy={p.slug for p in self.pending_copies}|{p.slug for p in self.positions.values()}|{o.slug for o in self.maker_orders.values() if o.status in {'OPEN','PARTIAL'}}
        for slug in retired-busy:
            self.wallet_signal_state.pop(slug,None);self.market_fee_rates.pop(slug,None);self._maker_prints.pop(slug,None)
            for order_id,order in list(self.maker_orders.items()):
                if order.slug==slug and order.status in {'CANCELLED','FILLED','EXPIRED','REJECTED'}:self.maker_orders.pop(order_id)
        sizes={name:len(getattr(self,name)) for name in ('positions','pending_copies','maker_orders','wallet_signal_state','market_fee_rates','_liquidity','_maker_prints')}
        if any(n>limit for n in sizes.values()) or sum(len(values) for values in self._maker_prints.values())>100000:
            raise RuntimeError('PAPER_RESOURCE_STATE_LIMIT')
        return dict(policy_revision='offline_durable_retention_v1',sizes=sizes,retired_slugs=len(retired))

    def position_by_slug(self, strategy: str, slug: str, market_store: MarketStore) -> RoundPosition | None:
        key = (strategy, slug)
        if key in self.positions:
            return self.positions[key]
        row = market_store.conn.execute(
            "SELECT round_start_us,round_end_us FROM markets WHERE slug=? LIMIT 1", (slug,)
        ).fetchone()
        if row is None:
            return None
        pos = self._recover_position(strategy,slug,int(row[0]),int(row[1]))
        self.positions[key] = pos
        return pos

    def _persist_position(self, strategy: str, pos: RoundPosition) -> None:
        self.store.upsert_round(
            session_id=self.session_id,
            strategy=strategy,
            slug=pos.slug,
            round_start_us=pos.round_start_us,
            round_end_us=pos.round_end_us,
            up_shares=_fmt_decimal(pos.up_shares) or "0",
            down_shares=_fmt_decimal(pos.down_shares) or "0",
            up_cost=_fmt_decimal(pos.up_buy_cost) or "0",
            down_cost=_fmt_decimal(pos.down_buy_cost) or "0",
            sell_proceeds=_fmt_decimal(pos.sell_proceeds) or "0",
            fees=_fmt_decimal(pos.fees) or "0",
            settled_side=pos.settled_side,
            settled_at_us=pos.settled_at_us,
            pnl=_fmt_decimal(pos.pnl),
            details={
                "cash_flow": _fmt_decimal(pos.cash_flow),
                "matched_shares": _fmt_decimal(pos.matched_shares),
                "avg_up_buy": _fmt_decimal(pos.avg_buy("UP")),
                "avg_down_buy": _fmt_decimal(pos.avg_buy("DOWN")),
                "avg_pair_cost": _fmt_decimal(pos.avg_pair_cost),
                "filled_orders": pos.filled_orders,
                "up_remaining_basis": _fmt_decimal(pos.up_remaining_basis),
                "down_remaining_basis": _fmt_decimal(pos.down_remaining_basis),
                "settlement_evidence": pos.settlement_evidence,
            },
        )

    def _maker_logical_key(self, slug: str, outcome: str, price: Decimal) -> str:
        return f"{slug}|{outcome}|{format(price, 'f')}"

    def _new_maker_order_id(self, slug: str, outcome: str, price: Decimal, now_us: int) -> str:
        digest = hashlib.sha256(
            f"{self.session_id}|S8|{slug}|{outcome}|{price}|{now_us}|{uuid.uuid4().hex}".encode()
        ).hexdigest()[:24]
        return f"s8_{digest}"

    @_atomic_paper_update
    def refresh_strategy8(self, *, market: DiscoveredMarket, collector: Any, sample: Any, now_us: int) -> None:
        if self.config.s8_research_policy=='DISABLED_V1':return
        now_us = self.execution_time(now_us)
        if collector is None or market.phase_at(now_us) != "LIVE":
            return
        pos = self.position(STRATEGY_8, market)
        if self.position_execution_reason(pos, now_us) is not None:
            return
        seconds_left = max(0.0, (market.round_end_us - now_us) / MICROS)
        fair_up = (sample.p_up_mc if sample is not None
                   and getattr(sample, "slug", None) == market.slug
                   and getattr(sample, "sample_ts_us", now_us + 1) <= now_us else None)
        desired: dict[str, tuple[str, Decimal, Decimal, Decimal]] = {}
        unavailable_outcomes = set()
        # token/outcome/price/qty keyed by deterministic order id.
        for outcome, token_id in (("UP", market.up_token_id), ("DOWN", market.down_token_id)):
            state = _state_for_outcome(collector, outcome)
            if (state is None or state.slug != market.slug or state.token_id != token_id
                    or not state.synchronized or state.best_bid is None):
                unavailable_outcomes.add(outcome)
                continue
            if state.age_seconds(now_us) is None or state.age_seconds(now_us) > self.config.max_book_age_seconds:
                unavailable_outcomes.add(outcome)
                continue
            if self.config.strict_market_minimum and (
                    state.min_order_size is None or not state.min_order_size.is_finite()
                    or state.min_order_size <= 0 or state.tick_size not in SUPPORTED_TICKS):
                unavailable_outcomes.add(outcome)
                continue
            own = pos.shares(outcome)
            opposite_outcome = "DOWN" if outcome == "UP" else "UP"
            opp = pos.shares(opposite_outcome)
            opp_avg = pos.avg_buy(opposite_outcome)
            underweight = own < opp
            if seconds_left <= self.config.stop_pairing_seconds:
                continue
            if underweight and opp_avg is not None:
                cap = Decimal("1") - opp_avg - Decimal(str(self.config.min_pair_edge))
                share_budget = min(Decimal(str(self.config.grid_clip_shares)), opp - own)
            else:
                if seconds_left <= self.config.stop_new_unpaired_seconds:
                    continue
                unpaired = max(Decimal("0"), own - opp)
                remaining_unpaired = Decimal(str(self.config.max_unpaired_shares)) - unpaired
                if remaining_unpaired <= 0:
                    continue
                fair = fair_up if outcome == "UP" else (None if fair_up is None else 1.0 - fair_up)
                cap = Decimal(str(self.config.unpaired_max_price))
                if fair is not None:
                    cap = min(cap, Decimal(str(max(0.0, fair - self.config.min_directional_edge))))
                share_budget = min(Decimal(str(self.config.grid_clip_shares)), remaining_unpaired)
            if cap <= 0 or share_budget <= 0:
                continue
            # Rest only; never cross the ask.  Restrict to levels at/below current best bid.
            book_cap = state.best_bid.price
            max_price = min(cap, book_cap)
            levels = [Decimal(str(p)) for p in self.config.grid_prices if Decimal(str(p)) <= max_price]
            if not levels:
                continue
            for price in sorted(levels, reverse=True)[: self.config.grid_orders_per_side]:
                if state.tick_size is not None and price % state.tick_size != 0:
                    continue
                logical_key = self._maker_logical_key(market.slug, outcome, price)
                desired[logical_key] = (outcome, price, share_budget, state.bids.get(price, Decimal("0")))

        # Cancel quotes that are no longer desired or are too old.
        for logical_key, order in list(self.maker_orders.items()):
            if order.slug != market.slug or order.status not in {"OPEN", "PARTIAL"}:
                continue
            too_old = now_us - order.created_at_us > int(self.config.maker_order_max_age_seconds * MICROS)
            if logical_key not in desired or too_old:
                order.status = "CANCELLED"
                cancel_reason = ("MAKER_BOOK_UNRELIABLE" if order.outcome in unavailable_outcomes
                                 else "MAKER_QUOTE_MAX_AGE" if too_old else "MAKER_QUOTE_NO_LONGER_DESIRED")
                self.store.update_order_status(order.order_id, status="CANCELLED", reason=cancel_reason)
                self.maker_cancellations[cancel_reason]+=1
                self.maker_orders.pop(logical_key, None)

        # Respect total round notional using filled-cost plus hypothetical new quote value.
        live_orders = [o for o in self.maker_orders.values() if o.slug == market.slug
                       and o.status in {"OPEN", "PARTIAL"}]
        round_spent = pos.total_buy_cost + sum(o.price * o.remaining_shares for o in live_orders)
        reserved_shares = {side: sum(o.remaining_shares for o in live_orders if o.outcome == side)
                           for side in ("UP", "DOWN")}
        for logical_key, (outcome, price, qty, queue_ahead) in desired.items():
            if logical_key in self.maker_orders:
                continue
            remaining_notional = Decimal(str(self.config.max_total_round_notional)) - round_spent
            if remaining_notional <= 0:
                break
            state = _state_for_outcome(collector, outcome)
            qty = min(qty - reserved_shares[outcome], remaining_notional / price)
            qty = qty.quantize(SHARE_QUANTUM, rounding=ROUND_DOWN)
            if qty <= 0:
                continue
            if self.config.strict_market_minimum and qty < state.min_order_size:
                continue
            token = market.up_token_id if outcome == "UP" else market.down_token_id
            now_us = self.execution_time(now_us)
            if self.position_execution_reason(pos, now_us) is not None:
                return
            age = state.age_seconds(now_us)
            if age is None or age > self.config.max_book_age_seconds:
                continue
            order_id = self._new_maker_order_id(market.slug, outcome, price, now_us)
            order = MakerOrder(order_id, market.slug, token, outcome, price, qty, qty, queue_ahead, now_us,
                               min_order_size=state.min_order_size, tick_size=state.tick_size)
            self.maker_orders[logical_key] = order
            reserved_shares[outcome] += qty
            round_spent += price * qty
            self.store.save_order(
                order_id=order_id,
                session_id=self.session_id,
                strategy=STRATEGY_8,
                signal_id=None,
                slug=market.slug,
                token_id=token,
                outcome=outcome,
                side="BUY",
                style="MAKER_GTC_PAPER",
                price_limit=_fmt_decimal(price),
                shares=_fmt_decimal(qty) or "0",
                created_at_us=now_us,
                scheduled_at_us=now_us,
                status="OPEN",
                reason=None,
                metadata={"queue_ahead": _fmt_decimal(queue_ahead), "maker_rebate_excluded": True,
                          "market_min_order_size": _fmt_decimal(state.min_order_size),
                          "tick_size": _fmt_decimal(state.tick_size),
                          "minimum_submission_eligible": state.min_order_size is not None and qty >= state.min_order_size,
                          "execution_policy_revision": EXECUTION_POLICY_REVISION},
            )

    @_atomic_paper_update
    def process_market_trades(self, market_store: MarketStore, *, now_us: int | None = None,
                              collector_pool: OverlapCollectorPool | None = None) -> int:
        # Retire quotes even when no new aggressor event arrives after rollover.
        for key, order in list(self.maker_orders.items()):
            pos = self.positions.get((STRATEGY_8, order.slug))
            reason = self.position_execution_reason(pos, self.execution_time(now_us))
            if reason is None:
                collector = None if collector_pool is None else collector_pool.collector_for_slug(order.slug)
                state = _state_for_outcome(collector, order.outcome)
                reason = self.execution_identity_reason(slug=order.slug, token_id=order.token_id,
                    outcome=order.outcome, state=state, collector=collector, market_store=market_store)
            if reason is None and collector_pool is not None:
                collector = collector_pool.collector_for_slug(order.slug)
                state = _state_for_outcome(collector, order.outcome)
                age = None if state is None else state.age_seconds(self.execution_time(now_us))
                if age is None or age > self.config.max_book_age_seconds:
                    reason = "MAKER_BOOK_UNRELIABLE"
            if reason is not None:
                order.status = "CANCELLED"
                self.store.update_order_status(order.order_id, status="CANCELLED", reason=reason)
                self.maker_cancellations[reason]+=1
                self.maker_orders.pop(key, None)
        rows = market_store.conn.execute(
            """
            SELECT id,slug,token_id,received_at_us,price,size,taker_side,source_trade_id,source_ts_us
            FROM market_trades WHERE id>? ORDER BY id ASC LIMIT 5000
            """,
            (self.last_market_trade_id,),
        ).fetchall()
        fills = 0
        for row in rows:
            trade_id = int(row[0])
            self.last_market_trade_id = max(self.last_market_trade_id, trade_id)
            slug, token_id = str(row[1]), (str(row[2]) if row[2] is not None else None)
            received_at_us = int(row[3])
            price = Decimal(str(row[4]))
            size = Decimal(str(row[5]))
            taker_side = str(row[6] or "").upper()
            if taker_side != "SELL" or token_id is None:
                continue
            source_us = None if row[8] is None else int(row[8])
            if (source_us is None or source_us > received_at_us
                    or received_at_us > self.execution_time(now_us)):
                continue
            # A tx hash is not a unique execution ID. Identical public prints
            # are ambiguous: credit once, even if this undercounts real fills.
            available = size
            matching = [
                order for order in self.maker_orders.values()
                if order.status in {"OPEN", "PARTIAL"}
                and order.slug == slug and order.token_id == token_id and order.price == price
            ]
            if not matching:
                continue
            fingerprint = (token_id, row[7], source_us, price, size, taker_side)
            seen = self._maker_prints.setdefault(slug, set())
            if fingerprint in seen:
                continue
            seen.add(fingerprint)
            matching.sort(key=lambda o: o.created_at_us)
            for order in matching:
                pos = self.positions.get((STRATEGY_8, slug))
                execution_us = self.execution_time(now_us)
                reason = self.position_execution_reason(pos, execution_us)
                if reason is None and collector_pool is not None:
                    state = _state_for_outcome(collector_pool.collector_for_slug(slug), order.outcome)
                    age = None if state is None else state.age_seconds(execution_us)
                    if age is None or age > self.config.max_book_age_seconds:
                        reason = "MAKER_BOOK_UNRELIABLE"
                if reason is None and received_at_us >= pos.round_end_us:
                    reason = "MARKET_EXPIRED"
                if reason is None and row[8] is not None and int(row[8]) >= pos.round_end_us:
                    reason = "MARKET_EXPIRED"
                if reason is not None:
                    order.status = "CANCELLED"
                    self.store.update_order_status(order.order_id, status="CANCELLED", reason=reason)
                    self.maker_cancellations[reason]+=1
                    continue
                if available <= 0:
                    break
                # Strict causal invariant: an aggressor event already received at or
                # before order creation cannot consume queue or fill a quote that did
                # not yet exist.  Equal timestamps fail closed because microsecond
                # ordering cannot be proven from the export.
                if received_at_us <= order.created_at_us or source_us <= order.created_at_us:
                    continue
                queue_before = order.queue_ahead
                available_before = available
                queue_consumed = Decimal("0")
                if order.queue_ahead > 0:
                    consumed = min(order.queue_ahead, available)
                    order.queue_ahead -= consumed
                    available -= consumed
                    queue_consumed = consumed
                    if available <= 0:
                        continue
                qty = min(order.remaining_shares, available)
                if qty <= 0:
                    continue
                notional = qty * price
                pos = self.positions.get((STRATEGY_8, slug))
                if pos is None:
                    # Market metadata should have created it when quoting; fail closed otherwise.
                    continue
                pos.record_fill(outcome=order.outcome, side="BUY", shares=qty, notional=notional, fee=Decimal("0"))
                self._persist_position(STRATEGY_8, pos)
                fill_id = f"ff_{uuid.uuid4().hex}"
                self.store.save_fill(
                    fill_id=fill_id,
                    order_id=order.order_id,
                    session_id=self.session_id,
                    strategy=STRATEGY_8,
                    slug=slug,
                    token_id=token_id,
                    outcome=order.outcome,
                    side="BUY",
                    fill_ts_us=execution_us,
                    shares=_fmt_decimal(qty) or "0",
                    price=_fmt_decimal(price) or "0",
                    notional=_fmt_decimal(notional) or "0",
                    fee="0",
                    liquidity_role="MAKER_PAPER",
                    metadata={
                        "market_trade_id": trade_id,
                        "source_trade_id": row[7],
                        "aggressor_source_ts_us": row[8],
                        "aggressor_received_at_us": received_at_us,
                        "order_created_at_us": order.created_at_us,
                        "queue_before": _fmt_decimal(queue_before),
                        "aggressor_available_before": _fmt_decimal(available_before),
                        "queue_consumed": _fmt_decimal(queue_consumed),
                        "queue_after": _fmt_decimal(order.queue_ahead),
                        "quantity_applied_to_us": _fmt_decimal(qty),
                        "queue_model": "FIFO_DISPLAYED_AHEAD_ONLY_STRICT_CAUSAL",
                        "execution_certainty": "CONDITIONAL_PUBLIC_PRINT_QUEUE_ESTIMATE",
                        "execution_policy_revision": EXECUTION_POLICY_REVISION,
                        "submitted_shares": _fmt_decimal(order.original_shares),
                        "market_min_order_size": _fmt_decimal(order.min_order_size),
                        "tick_size": _fmt_decimal(order.tick_size),
                        "minimum_submission_eligible": (order.min_order_size is not None
                            and order.original_shares >= order.min_order_size),
                    },
                )
                order.remaining_shares -= qty
                available -= qty
                fills += 1
                if order.remaining_shares <= 0:
                    order.status = "FILLED"
                    self.store.update_order_status(order.order_id, status="FILLED")
                    for logical_key, current in list(self.maker_orders.items()):
                        if current is order:
                            self.maker_orders.pop(logical_key, None)
                            break
                else:
                    order.status = "PARTIAL"
                    self.store.update_order_status(order.order_id, status="PARTIAL")
        return fills

    def schedule_copy(
        self,
        *,
        strategy: str,
        signal_id: str,
        row: WalletTrade | WalletActivity,
        first_seen_us: int,
        lower_lag_us: int | None,
        delay_ms: int | None = None,
        deterioration_cap: Decimal | None = None,
        require_book_confirmation: bool = False,
        model_target_probability: float | None = None,
        router_stage: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> PendingCopy | None:
        slug = _row_slug(row)
        if not slug or not row.token_id or not row.outcome or not row.side or row.size is None:
            return None
        order_id = f"{strategy.lower()}_{hashlib.sha256(f'{self.session_id}|{strategy}|{signal_id}'.encode()).hexdigest()[:24]}"
        pending = PendingCopy(
            order_id=order_id,
            strategy=strategy,
            signal_id=signal_id,
            slug=slug,
            token_id=row.token_id,
            outcome=row.outcome.upper(),
            side=row.side.upper(),
            target_price=row.price,
            target_size=row.size,
            lower_lag_us=lower_lag_us,
            created_at_us=first_seen_us,
            scheduled_at_us=first_seen_us + int(
                self.config.copy_execution_delay_ms if delay_ms is None else delay_ms
            ) * 1000,
            deterioration_cap=deterioration_cap,
            require_book_confirmation=require_book_confirmation,
            model_target_probability=model_target_probability,
            router_stage=router_stage,
        )
        self.pending_copies.append(pending)
        self.store.save_order(
            order_id=order_id,
            session_id=self.session_id,
            strategy=strategy,
            signal_id=signal_id,
            slug=slug,
            token_id=row.token_id,
            outcome=row.outcome.upper(),
            side=row.side.upper(),
            style="FAK_PAPER",
            price_limit=None,
            shares=_fmt_decimal(row.size) or "0",
            created_at_us=first_seen_us,
            scheduled_at_us=pending.scheduled_at_us,
            status="SCHEDULED",
            reason=None,
            metadata={
                "target_price": _fmt_decimal(row.price),
                "lower_publication_lag_us": lower_lag_us,
                "deterioration_cap": _fmt_decimal(deterioration_cap),
                "require_book_confirmation": require_book_confirmation,
                "model_target_probability": model_target_probability,
                "router_stage": router_stage,
                **(metadata or {}),
            },
        )
        return pending

    def schedule_phase590_wallet_variants(
        self,
        *,
        signal_id: str,
        row: WalletTrade | WalletActivity,
        first_seen_us: int,
        lower_lag_us: int | None,
        market_store: MarketStore,
        detection_p_up_mc: float | None = None,
    ) -> dict[str, str]:
        """Apply the frozen Phase-5.9 causal wallet hypotheses to one signal.

        Every strategy gets an explicit accept/reject record, including strategies
        that do not create an order.  This is the forward counterfactual ledger used
        after the session to quantify which filters helped or hurt.
        """
        slug = _row_slug(row)
        outcome = (row.outcome or "").upper()
        if not slug or not outcome or row.size is None:
            return {}
        state = self.wallet_signal_state.setdefault(slug, WalletMarketSignalState(slug=slug))
        context = state.context_before(outcome=outcome, size=row.size)
        round_row = market_store.conn.execute(
            "SELECT round_end_us FROM markets WHERE slug=? LIMIT 1", (slug,)
        ).fetchone()
        seconds_remaining = None if round_row is None else (int(round_row[0]) - first_seen_us) / MICROS
        ordinal = int(context["signal_ordinal"])
        large = row.size >= Decimal(str(self.config.large_signal_min_shares))
        early = seconds_remaining is not None and seconds_remaining > self.config.early_min_seconds_remaining
        first_side_only = (
            not bool(context["two_sided_started_before"])
            and (state.first_outcome is None or outcome == state.first_outcome)
        )
        target_model_probability = None
        if detection_p_up_mc is not None:
            try:
                p_up = float(detection_p_up_mc)
                if 0.0 <= p_up <= 1.0:
                    target_model_probability = p_up if outcome == "UP" else 1.0 - p_up
            except (TypeError, ValueError):
                target_model_probability = None
        base_context = {
            **context,
            "seconds_remaining_at_publication": seconds_remaining,
            "large_signal": large,
            "early_signal": early,
            "target_price": _fmt_decimal(row.price),
            "target_size": _fmt_decimal(row.size),
            "target_side": (row.side or "").upper(),
            "target_outcome": outcome,
            "source_first_seen_us": first_seen_us,
            "lower_publication_lag_us": lower_lag_us,
            "detection_p_up_mc": detection_p_up_mc,
            "target_model_probability": target_model_probability,
        }

        # strategy, eligible, filter-reject reason, deterioration cap, delay, book-confirm, router-stage
        specs: list[tuple[str, bool, str | None, Decimal | None, int | None, bool, str | None]] = [
            (STRATEGY_9A, True, None, None, None, False, None),
            (STRATEGY_9B, True, None, None, None, False, None),
            (STRATEGY_9_FIRST2, ordinal <= 2, "AFTER_FIRST2", None, None, False, None),
            (STRATEGY_9_FIRST3, ordinal <= 3, "AFTER_FIRST3", None, None, False, None),
            (STRATEGY_9_ONE_SIDED, first_side_only, "TARGET_ENTERED_TWO_SIDED_STATE", None, None, False, None),
            (STRATEGY_9_EARLY, early, "NOT_EARLY", None, None, False, None),
            (STRATEGY_9_EARLY_LARGE, early and large, "NOT_EARLY_LARGE", None, None, False, None),
            (STRATEGY_9_DET3, True, None, Decimal("0.03"), None, False, None),
            (STRATEGY_9_DET4, True, None, Decimal("0.04"), None, False, None),
            (STRATEGY_9_DET5, True, None, Decimal("0.05"), None, False, None),
            (STRATEGY_9_DET7_5, True, None, Decimal("0.075"), None, False, None),
            (STRATEGY_9_BOOK_CONFIRM_75, True, None, None, self.config.book_confirm_delay_ms, True, None),
            (STRATEGY_9_FIRST3_LARGE, ordinal <= 3 and large, "NOT_FIRST3_LARGE", None, None, False, None),
            (
                STRATEGY_9_FIRST3_LARGE_DET7_5,
                ordinal <= 3 and large,
                "NOT_FIRST3_LARGE",
                Decimal("0.075"),
                None,
                False,
                None,
            ),
            (STRATEGY_9_LARGE_DET7_5, large, "NOT_LARGE", Decimal("0.075"), None, False, None),
            (
                STRATEGY_9_BOOK_MODEL30,
                target_model_probability is not None
                and target_model_probability >= self.config.book_model30_min_target_probability,
                "MODEL_TARGET_PROB_BELOW_0_30",
                None,
                self.config.book_confirm_delay_ms,
                True,
                None,
            ),
            (
                STRATEGY_9_BOOK_MODEL40,
                target_model_probability is not None
                and target_model_probability >= self.config.book_model40_min_target_probability,
                "MODEL_TARGET_PROB_BELOW_0_40",
                None,
                self.config.book_confirm_delay_ms,
                True,
                None,
            ),
            (
                STRATEGY_9_ROUTER_A,
                (ordinal <= 3 and large)
                or (
                    target_model_probability is not None
                    and target_model_probability >= self.config.router_model_min_target_probability
                ),
                "ROUTER_MODEL_VETO",
                None,
                self.config.copy_execution_delay_ms if (ordinal <= 3 and large) else self.config.book_confirm_delay_ms,
                not (ordinal <= 3 and large),
                "SPECIALIST_PROBE" if (ordinal <= 3 and large) else "BOOK_DIRECT",
            ),
        ]

        # New 5.9.2 arms are pre-registered once and remain frozen for both
        # upcoming nights; detection-time MC probability is never backfilled.
        model60 = target_model_probability is not None and target_model_probability >= self.config.model60_min_target_probability
        model65 = target_model_probability is not None and target_model_probability >= self.config.model65_min_target_probability
        price_band = row.price is not None and Decimal(str(self.config.price_band_min)) <= row.price < Decimal(str(self.config.price_band_max))
        specs.extend([
            (STRATEGY_9_MODEL60, model60, "MODEL_BELOW_60_OR_MISSING", None, None, False, None),
            (STRATEGY_9_MODEL65, model65, "MODEL_BELOW_65_OR_MISSING", None, None, False, None),
            (STRATEGY_9_BOOK_MODEL60, model60, "MODEL_BELOW_60_OR_MISSING", None, self.config.book_confirm_delay_ms, True, None),
            (STRATEGY_9_BOOK_MODEL65, model65, "MODEL_BELOW_65_OR_MISSING", None, self.config.book_confirm_delay_ms, True, None),
            (STRATEGY_9_PRICE60_80, price_band, "PRICE_OUTSIDE_60_80", None, None, False, None),
            (STRATEGY_9_ROUTER_V1, model60 or price_band, "ROUTER_NO_PRIMARY_OR_FALLBACK", None, self.config.book_confirm_delay_ms, True, "V1_DECIDE"),
        ])

        result: dict[str, str] = {}
        for strategy, eligible, reject_reason, cap, delay_ms, require_confirm, router_stage in specs:
            if not eligible:
                result[strategy] = reject_reason or "FILTERED"
                self.store.save_signal_strategy_decision(
                    session_id=self.session_id,
                    signal_id=signal_id,
                    strategy=strategy,
                    decided_at_us=first_seen_us,
                    decision="REJECT",
                    reason=reject_reason or "FILTERED",
                    context=base_context,
                )
                continue
            pending = self.schedule_copy(
                strategy=strategy,
                signal_id=signal_id,
                row=row,
                first_seen_us=first_seen_us,
                lower_lag_us=lower_lag_us,
                delay_ms=delay_ms,
                deterioration_cap=cap,
                require_book_confirmation=require_confirm,
                model_target_probability=target_model_probability,
                router_stage=router_stage,
                metadata=base_context,
            )
            if pending is None:
                result[strategy] = "UNSCHEDULABLE_SIGNAL"
                self.store.save_signal_strategy_decision(
                    session_id=self.session_id,
                    signal_id=signal_id,
                    strategy=strategy,
                    decided_at_us=first_seen_us,
                    decision="REJECT",
                    reason="UNSCHEDULABLE_SIGNAL",
                    context=base_context,
                )
                continue
            result[strategy] = "SCHEDULED"
            self.store.save_signal_strategy_decision(
                session_id=self.session_id,
                signal_id=signal_id,
                strategy=strategy,
                decided_at_us=first_seen_us,
                decision="ACCEPT",
                reason=None,
                context=base_context,
            )

        state.observe(outcome=outcome, size=row.size)
        return result

    def schedule_phase593_edge(
        self,
        *,
        signal_id: str,
        row: WalletTrade | WalletActivity,
        first_seen_us: int,
        lower_lag_us: int | None,
        detection_p_up_mc: float | None,
    ) -> str:
        """Independent Phase 5.9.3 arm; legacy frozen scheduler is untouched.

        Only the causal probability captured at first public detection can enter
        this order. Missing/invalid features are explicit rejections, not guesses.
        """
        strategy = STRATEGY_9_EDGE_BOOK_V1
        outcome = (row.outcome or "").upper()
        p = None
        if detection_p_up_mc is not None and outcome in {"UP", "DOWN"}:
            try:
                candidate = float(detection_p_up_mc)
                if math.isfinite(candidate) and 0 <= candidate <= 1:
                    p = candidate if outcome == "UP" else 1 - candidate
            except (TypeError, ValueError):
                pass
        reason = None
        if (row.side or "").upper() != "BUY":
            reason = "EDGE_BUY_ONLY"
        elif p is None:
            reason = "EDGE_CAUSAL_MODEL_MISSING"
        context = {
            "detection_p_up_mc": detection_p_up_mc,
            "model_target_probability": p,
            "source_first_seen_us": first_seen_us,
            "phase593": True,
            "model_not_empirically_calibrated": True,
        }
        if reason is None:
            pending = self.schedule_copy(
                strategy=strategy, signal_id=signal_id, row=row,
                first_seen_us=first_seen_us, lower_lag_us=lower_lag_us,
                delay_ms=self.config.book_confirm_delay_ms,
                require_book_confirmation=True,
                model_target_probability=p,
                metadata=context,
            )
            if pending is None:
                reason = "EDGE_UNSCHEDULABLE_SIGNAL"
        self.store.save_signal_strategy_decision(
            session_id=self.session_id,
            signal_id=signal_id,
            strategy=strategy,
            decided_at_us=first_seen_us,
            decision="REJECT" if reason else "ACCEPT",
            reason=reason,
            context=context,
        )
        return reason or "SCHEDULED"

    def schedule_phase594_edge_v2(
        self, *, signal_id: str, row: WalletTrade | WalletActivity,
        first_seen_us: int, lower_lag_us: int | None,
        detection_p_up_mc: float | None,
    ) -> str:
        """Independent causal candidate; may independently size to market minimum."""
        strategy = STRATEGY_9_EDGE_BOOK_V2
        outcome = (row.outcome or "").upper()
        p = None
        if detection_p_up_mc is not None and outcome in {"UP", "DOWN"}:
            try:
                value = float(detection_p_up_mc)
                if math.isfinite(value) and 0 <= value <= 1:
                    p = value if outcome == "UP" else 1 - value
            except (ValueError, TypeError):
                pass
        if (row.side or "").upper() != "BUY":
            reason = "EDGE_BUY_ONLY"
        elif p is None:
            reason = "EDGE_CAUSAL_MODEL_MISSING"
        else:
            reason = None
        context = {"phase594": True, "detection_p_up_mc": detection_p_up_mc,
                   "model_target_probability": p, "v2_risk_scope": "SESSION_ONLY",
                   "model_not_empirically_calibrated": True}
        if reason is None:
            pending = self.schedule_copy(
                strategy=strategy, signal_id=signal_id, row=row,
                first_seen_us=first_seen_us, lower_lag_us=lower_lag_us,
                delay_ms=self.config.book_confirm_delay_ms,
                require_book_confirmation=True, model_target_probability=p,
                metadata=context,
            )
            if pending is None:
                reason = "EDGE_UNSCHEDULABLE_SIGNAL"
        self.store.save_signal_strategy_decision(
            session_id=self.session_id, signal_id=signal_id, strategy=strategy,
            decided_at_us=first_seen_us,
            decision="REJECT" if reason else "ACCEPT", reason=reason, context=context,
        )
        return reason or "SCHEDULED"

    def _router_specialist_probe(self, pending: PendingCopy, *, collector: Any, now_us: int) -> tuple[bool, str, dict[str, Any]]:
        """Causally decide whether Router-A may take the immediate specialist route.

        This does not fill anything.  It inspects only the executable book available
        at the +25 ms specialist decision point.  If the FIRST3+LARGE signal cannot
        be executed within the frozen 7.5c deterioration cap, Router-A may fall back
        to its later BOOK_CONFIRM_75 branch (subject to the frozen model veto).
        """
        state = _state_for_outcome(collector, pending.outcome)
        context: dict[str, Any] = {
            "route": "FIRST3_LARGE_DET7_5",
            "probed_at_us": now_us,
            "target_price": _fmt_decimal(pending.target_price),
            "model_target_probability": pending.model_target_probability,
        }
        if state is None or not state.synchronized:
            return False, "ROUTER_SPECIALIST_BOOK_UNAVAILABLE", context
        age = state.age_seconds(now_us)
        context["book_age_s"] = age
        if age is None or age > self.config.max_book_age_seconds:
            return False, "ROUTER_SPECIALIST_STALE_BOOK", context
        best = state.best_ask if pending.side == "BUY" else state.best_bid
        if best is None:
            return False, "ROUTER_SPECIALIST_EMPTY_BOOK", context
        context["executable_price"] = _fmt_decimal(best.price)
        if pending.target_price is None:
            return False, "ROUTER_SPECIALIST_TARGET_PRICE_MISSING", context
        deterioration = (
            best.price - pending.target_price
            if pending.side == "BUY"
            else pending.target_price - best.price
        )
        cap = Decimal(str(self.config.router_specialist_max_deterioration))
        context["deterioration"] = _fmt_decimal(deterioration)
        context["deterioration_cap"] = _fmt_decimal(cap)
        if deterioration > cap:
            return False, "ROUTER_SPECIALIST_PRICE_DETERIORATED", context
        return True, "ROUTER_SPECIALIST_EXECUTE", context

    def _confirm_book_copy(self, pending: PendingCopy, *, collector: Any, now_us: int) -> str:
        up = _state_for_outcome(collector, "UP")
        down = _state_for_outcome(collector, "DOWN")
        if up is None or down is None or not up.synchronized or not down.synchronized:
            return "BOOK_CONFIRM_UNAVAILABLE"
        up_age = up.age_seconds(now_us)
        down_age = down.age_seconds(now_us)
        if (
            up_age is None
            or down_age is None
            or up_age > self.config.max_book_age_seconds
            or down_age > self.config.max_book_age_seconds
        ):
            return "BOOK_CONFIRM_STALE"
        try:
            book = market_book_features(up, down, depth_band=0.02)
        except ValueError:
            return "BOOK_CONFIRM_REQUIRES_TWO_SIDED"
        score = float(book.book_direction_score)
        aligned = score >= self.config.book_confirm_min_direction_score if pending.outcome == "UP" else score <= -self.config.book_confirm_min_direction_score
        pending.confirmation_score = score
        pending.confirmation_context = {
            "confirmed_at_us": now_us,
            "book_direction_score": score,
            "threshold": self.config.book_confirm_min_direction_score,
            "up_best_bid": book.up.best_bid,
            "up_best_ask": book.up.best_ask,
            "down_best_bid": book.down.best_bid,
            "down_best_ask": book.down.best_ask,
        }
        if not aligned:
            return "BOOK_DIRECTION_DISAGREES"
        pending.confirmed_at_us = now_us
        pending.scheduled_at_us = now_us + self.config.book_confirm_execution_delay_ms * 1000
        self.store.update_order_status(pending.order_id, status="CONFIRMED", reason=None)
        self.store.save_signal_strategy_decision(
            session_id=self.session_id,
            signal_id=pending.signal_id,
            strategy=pending.strategy,
            decided_at_us=now_us,
            decision="CONFIRMED",
            reason=None,
            context=pending.confirmation_context,
        )
        return "CONFIRMED"

    def _router_v1_decision(self, pending: PendingCopy, *, collector: Any, now_us: int) -> str:
        """Decision using only fresh, causally observed full two-token L2.

        An opposite directional book is a hard veto. A neutral book can
        permit the pre-registered price-band fallback. Missing L2 is never
        treated as neutral agreement.
        """
        up = _state_for_outcome(collector, "UP")
        down = _state_for_outcome(collector, "DOWN")
        if up is None or down is None or not up.synchronized or not down.synchronized:
            return "ROUTER_V1_BOOK_UNAVAILABLE"
        ages = (up.age_seconds(now_us), down.age_seconds(now_us))
        if any(a is None or a > self.config.max_book_age_seconds for a in ages):
            return "ROUTER_V1_BOOK_STALE"
        try:
            book = market_book_features(up, down, depth_band=0.02)
        except ValueError:
            return "ROUTER_V1_BOOK_INCOMPLETE"
        score = float(book.book_direction_score)
        signed = score if pending.outcome == "UP" else -score
        threshold = self.config.book_confirm_min_direction_score
        primary = pending.model_target_probability is not None and pending.model_target_probability >= self.config.router_v1_min_target_probability
        price_ok = pending.target_price is not None and Decimal(str(self.config.price_band_min)) <= pending.target_price < Decimal(str(self.config.price_band_max))
        pending.confirmation_score = score
        pending.confirmation_context = {
            "decided_at_us": now_us,
            "book_direction_score": score,
            "signed_book_score": signed,
            "model_target_probability": pending.model_target_probability,
            "primary_model_floor": self.config.router_v1_min_target_probability,
            "price_fallback_eligible": bool(price_ok),
        }
        if signed <= -threshold:
            return "ROUTER_V1_OPPOSITE_BOOK_VETO"
        if signed >= threshold and primary:
            route = "BOOK_MODEL60_PRIMARY"
        elif price_ok:
            route = "PRICE60_80_NEUTRAL_OR_LOW_MODEL_FALLBACK"
        else:
            return "ROUTER_V1_NO_ROUTE"
        pending.confirmation_context["route"] = route
        pending.router_stage = route
        pending.confirmed_at_us = now_us
        pending.scheduled_at_us = now_us + self.config.book_confirm_execution_delay_ms * 1000
        self.store.update_order_status(pending.order_id, status="CONFIRMED", reason=None)
        self.store.save_signal_strategy_decision(
            session_id=self.session_id, signal_id=pending.signal_id,
            strategy=pending.strategy, decided_at_us=now_us,
            decision="CONFIRMED", reason=None,
            context=pending.confirmation_context,
        )
        return "CONFIRMED"

    def _copy_execution_context(self, pending: PendingCopy, *, collector: Any, now_us: int) -> dict[str, Any]:
        state = _state_for_outcome(collector, pending.outcome)
        if state is None:
            return {
                "execution_at_us": now_us,
                "book_available": False,
                "confirmation": pending.confirmation_context,
            }
        bid = state.best_bid
        ask = state.best_ask
        # Audit the same executable side used by _execute_copy, without
        # referencing that function's local `best` variable.
        executable = ask if pending.side == "BUY" else bid if pending.side == "SELL" else None
        return {
            "execution_at_us": now_us,
            "book_available": True,
            "synchronized": bool(state.synchronized),
            "book_age_s": state.age_seconds(now_us),
            "best_bid": _fmt_decimal(None if bid is None else bid.price),
            "best_bid_size": _fmt_decimal(None if bid is None else bid.size),
            "best_ask": _fmt_decimal(None if ask is None else ask.price),
            "best_ask_size": _fmt_decimal(None if ask is None else ask.size),
            "executable_price": _fmt_decimal(None if executable is None else executable.price),
            "spread": _fmt_decimal(state.spread),
            "min_order_size": _fmt_decimal(state.min_order_size),
            "top_min_order_notional": _fmt_decimal(
                None if state.min_order_size is None or executable is None else state.min_order_size * executable.price
            ),
            "target_price": _fmt_decimal(pending.target_price),
            "deterioration_cap": _fmt_decimal(pending.deterioration_cap),
            "confirmation": pending.confirmation_context,
        }

    @_atomic_paper_update
    def _execute_copy(self, pending: PendingCopy, *, collector: Any, market_store: MarketStore, now_us: int) -> str:
        block = self.store.taker_execution_block_reason(pending.order_id)
        if block is not None:
            return block
        now_us = self.execution_time(now_us)
        pos = self.position_by_slug(pending.strategy, pending.slug, market_store)
        reason = self.position_execution_reason(pos, now_us)
        if reason is not None:
            self.store.update_order_status(pending.order_id, status="REJECTED", reason=reason)
            return reason
        state = _state_for_outcome(collector, pending.outcome)
        reason = self.execution_identity_reason(slug=pending.slug, token_id=pending.token_id,
            outcome=pending.outcome, state=state, collector=collector, market_store=market_store)
        if reason is not None:
            self.store.update_order_status(pending.order_id, status="REJECTED", reason=reason)
            return reason
        if state is None or not state.synchronized:
            self.store.update_order_status(pending.order_id, status="REJECTED", reason="BOOK_UNAVAILABLE")
            return "BOOK_UNAVAILABLE"
        if not state.depth_reliable:
            self.store.update_order_status(pending.order_id, status="REJECTED", reason="BOOK_UNRELIABLE")
            return "BOOK_UNRELIABLE"
        age = state.age_seconds(now_us)
        if age is None or age > self.config.max_book_age_seconds:
            self.store.update_order_status(pending.order_id, status="REJECTED", reason="STALE_BOOK")
            return "STALE_BOOK"
        if pending.require_book_confirmation:
            reason=self.paired_book_execution_reason(collector,now_us,self.config.max_book_age_seconds,market_store=market_store,slug=pending.slug)
            if reason:
                self.store.update_order_status(pending.order_id,status='REJECTED',reason=reason)
                return reason
        best = state.best_ask if pending.side == "BUY" else state.best_bid
        if best is None:
            self.store.update_order_status(pending.order_id, status="REJECTED", reason="EMPTY_BOOK")
            return "EMPTY_BOOK"
        spread = state.spread
        if pending.strategy == STRATEGY_9B:
            reason = protected_copy_reason(
                side=pending.side,
                target_price=pending.target_price,
                executable_price=best.price,
                spread=spread,
                top_depth=best.size,
                lower_lag_us=pending.lower_lag_us,
                config=self.config,
            )
            if reason is not None:
                self.store.update_order_status(pending.order_id, status="REJECTED", reason=reason)
                return reason

        if pending.deterioration_cap is not None and pending.target_price is not None:
            deterioration = (
                best.price - pending.target_price
                if pending.side == "BUY"
                else pending.target_price - best.price
            )
            if deterioration > pending.deterioration_cap:
                self.store.update_order_status(pending.order_id, status="REJECTED", reason="PRICE_DETERIORATED")
                self.store.save_signal_strategy_decision(
                    session_id=self.session_id,
                    signal_id=pending.signal_id,
                    strategy=pending.strategy,
                    decided_at_us=now_us,
                    decision="REJECT",
                    reason="PRICE_DETERIORATED",
                    context={
                        "target_price": _fmt_decimal(pending.target_price),
                        "executable_price": _fmt_decimal(best.price),
                        "deterioration": _fmt_decimal(deterioration),
                        "deterioration_cap": _fmt_decimal(pending.deterioration_cap),
                    },
                )
                return "PRICE_DETERIORATED"

        pos = self.position_by_slug(pending.strategy, pending.slug, market_store)
        if pos is None:
            self.store.update_order_status(pending.order_id, status="REJECTED", reason="UNKNOWN_MARKET")
            return "UNKNOWN_MARKET"
        shares = pending.target_size
        if not shares.is_finite() or shares <= 0:
            reason = "EDGE_NO_EXECUTABLE_SIZE" if pending.strategy == STRATEGY_9_EDGE_BOOK_V2 else "NO_EXECUTABLE_SIZE"
            self.store.update_order_status(pending.order_id, status="REJECTED", reason=reason)
            return reason
        if pending.strategy == STRATEGY_9_EDGE_BOOK_V2:
            if state.min_order_size is None or not state.min_order_size.is_finite() or state.min_order_size <= 0:
                self.store.update_order_status(pending.order_id, status="REJECTED", reason="EDGE_MIN_ORDER_UNKNOWN")
                return "EDGE_MIN_ORDER_UNKNOWN"
            shares = max(shares, state.min_order_size.quantize(SHARE_QUANTUM, rounding=ROUND_UP))
        if self.config.strict_market_minimum:
            if state.min_order_size is None or not state.min_order_size.is_finite() or state.min_order_size <= 0:
                self.store.update_order_status(pending.order_id, status="REJECTED", reason="MARKET_MINIMUM_UNKNOWN")
                return "MARKET_MINIMUM_UNKNOWN"
            if shares < state.min_order_size:
                self.store.update_order_status(pending.order_id, status="REJECTED", reason="SUBMITTED_BELOW_MARKET_MINIMUM")
                return "SUBMITTED_BELOW_MARKET_MINIMUM"
            if state.tick_size not in SUPPORTED_TICKS:
                self.store.update_order_status(pending.order_id, status="REJECTED", reason="MARKET_TICK_UNKNOWN")
                return "MARKET_TICK_UNKNOWN"
        fee_rate, fee_exponent, fee_source = self.taker_fee_rate_for_slug(pending.slug)
        preview = self._liquidity_view(pending.strategy, state, pending.side)
        execution_book = preview[3]
        if pending.side == "BUY":
            if best.price <= 0:
                self.store.update_order_status(pending.order_id, status="REJECTED", reason="INVALID_PRICE")
                return "INVALID_PRICE"
            # Cheap upper bound only; actual depth and fees still constrain the
            # submission below. This also bounds decimal arithmetic and search.
            shares = min(shares, Decimal(str(self.config.copy_max_order_notional)) / best.price)
            remaining_budget = None
            if pending.strategy == STRATEGY_9_EDGE_BOOK_V2:
                remaining_budget = min(
                    self.edge_v2_bankroll.nightly_risk_budget - self.edge_v2_bankroll.committed_this_session,
                    self.edge_v2_bankroll.cash - self.edge_v2_bankroll.protected_floor,
                )
                if remaining_budget < best.price * SHARE_QUANTUM:
                    self.store.update_order_status(pending.order_id, status="REJECTED", reason="EDGE_NO_EXECUTABLE_SIZE")
                    return "EDGE_NO_EXECUTABLE_SIZE"
            max_price = Decimal("0.999")
            cap = pending.deterioration_cap
            if pending.strategy == STRATEGY_9B:
                cap = Decimal(str(self.config.protected_max_deterioration))
            if cap is not None and pending.target_price is not None:
                max_price = min(max_price, pending.target_price + cap)
            max_price = _tick_limit(state, max_price, "BUY")
            shares, plan = _sized_taker_plan(
                execution_book,
                side="BUY",
                shares=shares,
                fee_rate=fee_rate,
                fee_exponent=fee_exponent,
                max_price=max_price,
                max_notional=Decimal(str(self.config.copy_max_order_notional)),
                max_cost=remaining_budget,
            )
        elif pending.side == "SELL":
            held = pos.shares(pending.outcome)
            if held <= 0:
                self.store.update_order_status(pending.order_id, status="REJECTED", reason="NO_INVENTORY_TO_SELL")
                return "NO_INVENTORY_TO_SELL"
            shares = min(shares, held)
            min_price = Decimal("0.001")
            cap = pending.deterioration_cap
            if pending.strategy == STRATEGY_9B:
                cap = Decimal(str(self.config.protected_max_deterioration))
            if cap is not None and pending.target_price is not None:
                min_price = max(min_price, pending.target_price - cap)
            min_price = _tick_limit(state, min_price, "SELL")
            shares, plan = _sized_taker_plan(
                execution_book,
                side="SELL",
                shares=shares,
                fee_rate=fee_rate,
                fee_exponent=fee_exponent,
                min_price=min_price,
            )
        else:
            self.store.update_order_status(pending.order_id, status="REJECTED", reason="UNSUPPORTED_TARGET_SIDE")
            return "UNSUPPORTED_TARGET_SIDE"

        if plan is None:
            reason = "EDGE_NO_EXECUTABLE_SIZE" if pending.strategy == STRATEGY_9_EDGE_BOOK_V2 else "NO_EXECUTABLE_SIZE"
            self.store.update_order_status(pending.order_id, status="REJECTED", reason=reason)
            return reason
        if plan.filled_shares <= 0:
            self.store.update_order_status(pending.order_id, status="REJECTED", reason="NO_EXECUTABLE_LIQUIDITY")
            return "NO_EXECUTABLE_LIQUIDITY"
        self.store.update_order_submitted_shares(pending.order_id, _fmt_decimal(shares) or "0",
            price_limit=_fmt_decimal(max_price if pending.side == "BUY" else min_price))

        # Model/store work may have consumed time since the initial checks.
        now_us = self.execution_time(now_us)
        reason = self.position_execution_reason(pos, now_us)
        age = state.age_seconds(now_us)
        if reason is None and not state.depth_reliable:
            reason = "BOOK_UNRELIABLE"
        if reason is None and (age is None or age > self.config.max_book_age_seconds):
            reason = "STALE_BOOK"
        if reason is not None:
            self.store.update_order_status(pending.order_id, status="REJECTED", reason=reason)
            return reason
        if self.config.strict_market_minimum and state.min_order_size is not None:
            if shares < state.min_order_size:
                self.store.update_order_status(pending.order_id, status="REJECTED", reason="SUBMITTED_BELOW_MARKET_MINIMUM")
                return "SUBMITTED_BELOW_MARKET_MINIMUM"
        if pending.strategy == STRATEGY_9_EDGE_BOOK_V1 or pending.strategy == STRATEGY_9_EDGE_BOOK_V2:
            edge_cfg = EdgeGateConfig(
                probability_haircut=self.config.edge_v1_probability_haircut,
                slippage_allowance=self.config.edge_v1_slippage_allowance,
                minimum_net_edge=self.config.edge_v1_min_net_edge,
            )
            edge = assess_executable_edge(
                side=pending.side,
                raw_probability=pending.model_target_probability,
                submitted_shares=shares,
                filled_shares=plan.filled_shares,
                effective_price=plan.vwap,
                total_fee=plan.fee,
                market_min_order_size=state.min_order_size,
                config=edge_cfg,
            )
            pending.confirmation_context = {
                **(pending.confirmation_context or {}),
                "phase593_edge_gate": edge.to_context(),
                "model_probability_clock": "wallet_public_detection",
                "executable_book_clock_us": now_us,
                "caution": "paper L2 execution; MC probability not empirically calibrated",
            }
            if not edge.accepted:
                self.store.update_order_status(pending.order_id, status="REJECTED", reason=edge.reason)
                return edge.reason
        if pending.strategy == STRATEGY_9_EDGE_BOOK_V2:
            worst_case_cost = plan.notional + plan.fee
            reject = self.edge_v2_bankroll.check(worst_case_cost)
            if reject is not None:
                self.store.update_order_status(pending.order_id, status="REJECTED", reason=reject)
                return reject
        # Edge assessment and persistence can delay execution beyond the prior
        # clock check. Take the final timestamp before any cash/position mutation.
        now_us = self.execution_time(now_us)
        reason = self.position_execution_reason(pos, now_us)
        age = state.age_seconds(now_us)
        if reason is None and not state.depth_reliable:
            reason = "BOOK_UNRELIABLE"
        if reason is None and (age is None or age > self.config.max_book_age_seconds):
            reason = "STALE_BOOK"
        if reason is not None:
            self.store.update_order_status(pending.order_id, status="REJECTED", reason=reason)
            return reason
        if plan.filled_shares <= 0 or plan.vwap is None:
            self.store.update_order_status(pending.order_id, status="REJECTED", reason="NO_EXECUTABLE_LIQUIDITY")
            return "NO_EXECUTABLE_LIQUIDITY"
        if pending.strategy == STRATEGY_9_EDGE_BOOK_V2:
            actual_cost = plan.notional + plan.fee
            risk_failure = self.edge_v2_bankroll.commit(actual_cost)
            if risk_failure is not None:
                self.store.update_order_status(pending.order_id, status="REJECTED", reason=risk_failure)
                return risk_failure
            self.edge_v2_costs_by_slug[pending.slug] = (
                self.edge_v2_costs_by_slug.get(pending.slug, Decimal("0")) + actual_cost
            )
            pending.confirmation_context = {
                **(pending.confirmation_context or {}),
                "phase594_session_bankroll": self.edge_v2_bankroll.snapshot(),
                "sizing": "MAX_OF_WALLET_SIZE_AND_MARKET_MINIMUM",
            }
        pos.record_fill(
            outcome=pending.outcome,
            side=pending.side,
            shares=plan.filled_shares,
            notional=plan.notional,
            fee=plan.fee,
        )
        self._consume_liquidity(preview, plan)
        pending.executed_at_us = now_us
        self._persist_position(pending.strategy, pos)
        fill_id = f"ff_{uuid.uuid4().hex}"
        self.store.save_fill(
            fill_id=fill_id,
            order_id=pending.order_id,
            session_id=self.session_id,
            strategy=pending.strategy,
            slug=pending.slug,
            token_id=pending.token_id,
            outcome=pending.outcome,
            side=pending.side,
            fill_ts_us=now_us,
            shares=_fmt_decimal(plan.filled_shares) or "0",
            price=_fmt_decimal(plan.vwap) or "0",
            notional=_fmt_decimal(plan.notional) or "0",
            fee=_fmt_decimal(plan.fee) or "0",
            liquidity_role="TAKER_PAPER",
            metadata={
                "target_price": _fmt_decimal(pending.target_price),
                "fill_audit_revision":"six_hour_taker_fill_provenance_v1",
                "execution_at_us":now_us,
                "book_source_ts_us":state.last_server_ts_us,
                "execution_admission_policy_revision":EXECUTION_ADMISSION_REVISION,
                "book_received_at_us":state.last_received_at_us,
                "book_sync_generation":state.sync_generation,
                "requested_shares": _fmt_decimal(plan.requested_shares),
                "submitted_shares": _fmt_decimal(shares),
                "minimum_submission_eligible": bool(
                    state.min_order_size is not None and shares >= state.min_order_size
                ),
                "partial": plan.filled_shares < plan.requested_shares,
                "levels": [asdict(x) for x in plan.levels],
                "fee_rate": _fmt_decimal(fee_rate),
                "fee_exponent": fee_exponent,
                "fee_source": fee_source,
                "market_min_order_size": _fmt_decimal(state.min_order_size),
                "minimum_top_notional": _fmt_decimal(
                    None if state.min_order_size is None else state.min_order_size * best.price
                ),
                "filled_below_market_min_order_size": bool(
                    state.min_order_size is not None and plan.filled_shares < state.min_order_size
                ),
                "confirmation": pending.confirmation_context,
                "target_shares": _fmt_decimal(pending.target_size),
                "unfilled_shares_cancelled": _fmt_decimal(shares - plan.filled_shares),
                "execution_policy_revision": EXECUTION_POLICY_REVISION,
                "liquidity_policy": "INDEPENDENT_ARM_OBSERVED_SIZE_INCREASES_ONLY",
                "strict_market_minimum": self.config.strict_market_minimum,
                "tick_size": _fmt_decimal(state.tick_size),
            },
        )
        self.store.update_order_status(
            pending.order_id,
            status="FILLED" if plan.filled_shares == plan.requested_shares else "PARTIAL",
            reason=None if plan.filled_shares == plan.requested_shares else "FAK_REMAINDER_CANCELLED",
        )
        return "FILLED" if plan.filled_shares == plan.requested_shares else "PARTIAL"

    @_atomic_paper_update
    def execute_due_copies(self, *, collector_pool: OverlapCollectorPool, market_store: MarketStore, now_us: int) -> dict[str, int]:
        stats: dict[str, int] = {}
        due = [p for p in self.pending_copies if p.scheduled_at_us <= now_us]
        self.pending_copies = [p for p in self.pending_copies if p.scheduled_at_us > now_us]
        for pending in due:
            now_us = self.execution_time(now_us)
            block = self.store.taker_execution_block_reason(pending.order_id)
            if block is not None:
                stats[f"{pending.strategy}:{block}"] = stats.get(f"{pending.strategy}:{block}", 0) + 1
                continue
            collector = collector_pool.collector_for_slug(pending.slug)
            pos = self.position_by_slug(pending.strategy, pending.slug, market_store)
            reason = self.position_execution_reason(pos, now_us)
            if reason is not None:
                self.store.update_order_status(pending.order_id, status="REJECTED", reason=reason)
                self.store.save_signal_strategy_decision(
                    session_id=self.session_id, signal_id=pending.signal_id,
                    strategy=pending.strategy, decided_at_us=now_us,
                    decision="REJECT", reason=reason, context={"execution_at_us": now_us})
                stats[f"{pending.strategy}:{reason}"] = stats.get(f"{pending.strategy}:{reason}", 0) + 1
                continue
            if pending.strategy == STRATEGY_9_ROUTER_A and pending.router_stage == "SPECIALIST_PROBE":
                allowed, route_reason, route_context = self._router_specialist_probe(
                    pending, collector=collector, now_us=now_us
                )
                self.store.save_signal_strategy_decision(
                    session_id=self.session_id,
                    signal_id=pending.signal_id,
                    strategy=pending.strategy,
                    decided_at_us=now_us,
                    decision="ROUTE_SPECIALIST" if allowed else "ROUTE_FALLBACK",
                    reason=None if allowed else route_reason,
                    context=route_context,
                )
                if allowed:
                    pending.deterioration_cap = Decimal(str(self.config.router_specialist_max_deterioration))
                    pending.router_stage = "SPECIALIST_EXECUTION"
                else:
                    if (
                        pending.model_target_probability is None
                        or pending.model_target_probability < self.config.router_model_min_target_probability
                    ):
                        self.store.update_order_status(
                            pending.order_id, status="REJECTED", reason="ROUTER_MODEL_VETO"
                        )
                        self.store.save_signal_strategy_decision(
                            session_id=self.session_id,
                            signal_id=pending.signal_id,
                            strategy=pending.strategy,
                            decided_at_us=now_us,
                            decision="REJECT",
                            reason="ROUTER_MODEL_VETO",
                            context={
                                **route_context,
                                "model_floor": self.config.router_model_min_target_probability,
                            },
                        )
                        stats[f"{pending.strategy}:ROUTER_MODEL_VETO"] = stats.get(
                            f"{pending.strategy}:ROUTER_MODEL_VETO", 0
                        ) + 1
                        continue
                    pending.deterioration_cap = None
                    pending.require_book_confirmation = True
                    pending.confirmed_at_us = None
                    pending.router_stage = "BOOK_FALLBACK"
                    pending.scheduled_at_us = max(
                        pending.created_at_us + self.config.book_confirm_delay_ms * 1000, now_us
                    )
                    self.pending_copies.append(pending)
                    stats[f"{pending.strategy}:ROUTE_FALLBACK"] = stats.get(
                        f"{pending.strategy}:ROUTE_FALLBACK", 0
                    ) + 1
                    continue
            if pending.require_book_confirmation and pending.confirmed_at_us is None:
                result = (
                    self._router_v1_decision(pending, collector=collector, now_us=now_us)
                    if pending.strategy == STRATEGY_9_ROUTER_V1
                    else self._confirm_book_copy(pending, collector=collector, now_us=now_us)
                )
                stats[f"{pending.strategy}:{result}"] = stats.get(f"{pending.strategy}:{result}", 0) + 1
                if result == "CONFIRMED":
                    self.pending_copies.append(pending)
                else:
                    self.store.update_order_status(pending.order_id, status="REJECTED", reason=result)
                    self.store.save_signal_strategy_decision(
                        session_id=self.session_id,
                        signal_id=pending.signal_id,
                        strategy=pending.strategy,
                        decided_at_us=now_us,
                        decision="REJECT",
                        reason=result,
                        context=pending.confirmation_context or {},
                    )
                continue
            result = self._execute_copy(pending, collector=collector, market_store=market_store, now_us=now_us)
            decision_us = pending.executed_at_us if result in {"FILLED", "PARTIAL"} else self.execution_time(now_us)
            self.store.save_signal_strategy_decision(
                session_id=self.session_id,
                signal_id=pending.signal_id,
                strategy=pending.strategy,
                decided_at_us=decision_us,
                decision="EXECUTED" if result in {"FILLED", "PARTIAL"} else "REJECT",
                reason=None if result in {"FILLED", "PARTIAL"} else result,
                context=self._copy_execution_context(pending, collector=collector, now_us=decision_us),
            )
            stats[f"{pending.strategy}:{result}"] = stats.get(f"{pending.strategy}:{result}", 0) + 1
        return stats

    @_atomic_paper_update
    def settle_ready(self, *, external_store: ExternalStore, now_us: int) -> list[tuple[str, str, str, Decimal]]:
        settled: list[tuple[str, str, str, Decimal]] = []
        for (strategy, slug), pos in list(self.positions.items()):
            if pos.pnl is not None or now_us < pos.round_end_us:
                continue
            ref = external_store.causal_price_reference(slug, now_us)
            if (ref is None or ref.reference_ts_us != pos.round_start_us
                    or ref.round_start_us != pos.round_start_us or ref.round_end_us != pos.round_end_us):
                continue
            final = external_store.settlement_observation_exact(
                source_ts_us=pos.round_end_us,
                decision_time_us=now_us,
            )
            if final is None:
                continue
            final_twap = Decimal(str(final[2]))
            ptb = Decimal(str(ref.price_to_beat))
            resolved = "UP" if final_twap >= ptb else "DOWN"
            pos.settlement_evidence={
                'policy_revision':'six_hour_shadow_settlement_evidence_v1',
                'kind':'OBSERVED_TWAP_DERIVED_SHADOW_OUTCOME',
                'official_resolution_status':'NOT_INDEPENDENTLY_OBSERVED',
                'exchange_payout_status':'NO_EXCHANGE_PAYOUT_EVIDENCE',
                'ptb':{'value':ref.price_to_beat,'source_ts_us':ref.reference_ts_us,'received_at_us':ref.observed_at_us,
                    'source':ref.source,'stream':ref.source_stream},
                'final_twap':{'value':str(final[2]),'source_ts_us':final[0],'received_at_us':final[1],'source':final[3],'stream':final[4]},
                'observed_outcome':resolved,'recorded_at_us':now_us}
            pnl = pos.settle(resolved_side=resolved, at_us=now_us)
            if strategy == STRATEGY_9_EDGE_BOOK_V2:
                committed = self.edge_v2_costs_by_slug.pop(slug, Decimal("0"))
                payout = pos.up_shares if resolved == "UP" else pos.down_shares
                self.edge_v2_bankroll.settle(cost=committed, payout=payout)
            self._persist_position(strategy, pos)
            settled.append((strategy, slug, resolved, pnl))
        return settled




# ---------------------------------------------------------------------------
# Phase 5.7 full forward tournament: Strategies 1-7 + wallet-informed variants
# + a cheap-side contrarian control, all on the same live rounds as S8/9A/9B.
# ---------------------------------------------------------------------------

TOURNAMENT_HORIZONS: tuple[int, ...] = (240, 180, 120, 60, 30, 15)
_FAMILY_NUM = {
    StrategyFamily.FAVOURITE: 1,
    StrategyFamily.FAIR_VALUE: 2,
    StrategyFamily.MOMENTUM: 3,
    StrategyFamily.MEAN_REVERSION: 4,
    StrategyFamily.BOOK_IMBALANCE: 5,
    StrategyFamily.MICROSTRUCTURE: 6,
    StrategyFamily.CROSS_EXCHANGE: 7,
}


def _forward_common(*, horizon: int, wallet_informed: bool) -> CommonExperimentConfig:
    """Execution-aware common settings frozen before a forward session.

    BASE preserves the original signal defaults and 3-cent edge threshold while
    adding a realistic paper execution delay. WALLET uses only hypotheses learned
    before this forward run: wider edge requirement, tighter spread, and avoidance
    of extreme prices. The two profiles are always reported separately.
    """
    if wallet_informed:
        return CommonExperimentConfig(
            decision_seconds_before_close=horizon,
            shares=5.0,
            execution_delay_seconds=0.10,
            max_quote_age_seconds=2.0,
            max_decision_gap_seconds=5.0,
            slippage_limit=0.02,
            min_edge=0.045,
            safety_margin=0.005,
            min_price=0.05,
            max_price=0.90,
            max_spread=0.06,
            minimum_executable_shares=1.0,
            monte_carlo_paths=500,
        )
    return CommonExperimentConfig(
        decision_seconds_before_close=horizon,
        shares=5.0,
        execution_delay_seconds=0.10,
        max_quote_age_seconds=2.0,
        max_decision_gap_seconds=5.0,
        slippage_limit=0.02,
        min_edge=0.03,
        safety_margin=0.0,
        min_price=0.01,
        max_price=0.99,
        max_spread=0.10,
        minimum_executable_shares=1.0,
        monte_carlo_paths=500,
    )


def full_tournament_specs(horizon: int) -> tuple[tuple[str, StrategyConfig], ...]:
    """Frozen pre-registered live variants for one decision horizon.

    BASE = original Phase-5 signal defaults with the common forward execution layer.
    WALLET = conservative variants motivated by the completed wallet forensic sample.
    Favourite remains a pure control and is not duplicated as a wallet-informed copy.
    """
    base = _forward_common(horizon=horizon, wallet_informed=False)
    wallet = _forward_common(horizon=horizon, wallet_informed=True)
    base_specs = (
        (f"S1_BASE_H{horizon:03d}_FAVOURITE", StrategyConfig(strategy_family=StrategyFamily.FAVOURITE, common=base, params=FavouriteParams())),
        (f"S2_BASE_H{horizon:03d}_FAIR_VALUE", StrategyConfig(strategy_family=StrategyFamily.FAIR_VALUE, common=base, params=FairValueParams())),
        (f"S3_BASE_H{horizon:03d}_MOMENTUM", StrategyConfig(strategy_family=StrategyFamily.MOMENTUM, common=base, params=MomentumParams())),
        (f"S4_BASE_H{horizon:03d}_MEAN_REVERSION", StrategyConfig(strategy_family=StrategyFamily.MEAN_REVERSION, common=base, params=MeanReversionParams())),
        (f"S5_BASE_H{horizon:03d}_BOOK_IMBALANCE", StrategyConfig(strategy_family=StrategyFamily.BOOK_IMBALANCE, common=base, params=BookImbalanceParams())),
        (f"S6_BASE_H{horizon:03d}_MICROSTRUCTURE", StrategyConfig(strategy_family=StrategyFamily.MICROSTRUCTURE, common=base, params=MicrostructureParams())),
        (f"S7_BASE_H{horizon:03d}_CROSS_EXCHANGE", StrategyConfig(strategy_family=StrategyFamily.CROSS_EXCHANGE, common=base, params=CrossExchangeParams())),
    )
    # Wallet-informed hypotheses are deliberately modest.  The forensic sample
    # showed mostly cheap-side/passive/contrarian behavior, but did not identify a
    # reliable L2 trigger. We therefore strengthen mean reversion, reduce weak
    # momentum chasing, and make unproven book/cross-exchange effects conservative.
    wallet_specs = (
        (f"S2_WALLET_H{horizon:03d}_FAIR_VALUE", StrategyConfig(strategy_family=StrategyFamily.FAIR_VALUE, common=wallet, params=FairValueParams())),
        (f"S3_WALLET_H{horizon:03d}_MOMENTUM", StrategyConfig(
            strategy_family=StrategyFamily.MOMENTUM,
            common=wallet,
            params=MomentumParams(
                momentum_short_seconds=5,
                momentum_medium_seconds=15,
                momentum_long_seconds=30,
                minimum_return=0.00050,
                minimum_normalized_return=0.45,
                require_direction_agreement=True,
                signal_weight_beta=0.15,
            ),
        )),
        (f"S4_WALLET_H{horizon:03d}_MEAN_REVERSION", StrategyConfig(
            strategy_family=StrategyFamily.MEAN_REVERSION,
            common=wallet,
            params=MeanReversionParams(
                extension_lookback_seconds=15,
                extension_min_return=0.00025,
                reversal_lookback_seconds=5,
                reversal_min_return=0.00005,
                vol_normalization=True,
                signal_weight_beta=0.45,
            ),
        )),
        (f"S5_WALLET_H{horizon:03d}_BOOK_IMBALANCE", StrategyConfig(
            strategy_family=StrategyFamily.BOOK_IMBALANCE,
            common=wallet,
            params=BookImbalanceParams(
                depth_band=0.02,
                minimum_direction_score=0.20,
                minimum_total_depth=20.0,
                signal_weight_beta=0.20,
            ),
        )),
        (f"S6_WALLET_H{horizon:03d}_MICROSTRUCTURE", StrategyConfig(
            strategy_family=StrategyFamily.MICROSTRUCTURE,
            common=wallet,
            params=MicrostructureParams(
                pm_move_lookback_seconds=5,
                pm_min_midpoint_move=0.01,
                depth_band=0.02,
                minimum_pressure_score=0.20,
                signal_weight_beta=0.20,
            ),
        )),
        (f"S7_WALLET_H{horizon:03d}_CROSS_EXCHANGE", StrategyConfig(
            strategy_family=StrategyFamily.CROSS_EXCHANGE,
            common=wallet,
            params=CrossExchangeParams(
                exchange_lookback_seconds=10,
                coinbase_min_return=0.00050,
                kraken_min_return=0.00050,
                max_exchange_disagreement=0.00035,
                basis_limit=0.00075,
                signal_weight_beta=0.20,
            ),
        )),
    )
    return base_specs + wallet_specs


@dataclass
class PendingTournamentOrder:
    order_id: str
    strategy: str
    slug: str
    token_id: str
    outcome: str
    shares: Decimal
    decision_ask: Decimal
    slippage_limit: Decimal
    max_quote_age_seconds: float
    created_at_us: int
    scheduled_at_us: int


class FullForwardTournament:
    """Causal paper-forward tournament sharing the same live collectors as S8/9.

    Each strategy/horizon makes at most one decision per round.  All taker variants
    execute from the book observed after their configured local delay; no midpoint
    or retrospective fills are permitted.  Different horizons remain distinct
    strategy IDs and must not be pooled in head-to-head inference.
    """

    def __init__(
        self,
        *,
        session_id: str,
        store: ForwardWalletStore,
        paper_engine: ForwardPaperEngine,
        external_store: ExternalStore,
        config: ForwardLabConfig,
        strategy_allowlist: frozenset[str] | None = None,
    ) -> None:
        self.session_id = session_id
        self.store = store
        self.paper_engine = paper_engine
        self.external_store = external_store
        self.config = config
        known={name for h in TOURNAMENT_HORIZONS for name,_ in full_tournament_specs(h)}
        known.update(f'CTRL_WALLET_CHEAP_CONTRA_H{h:03d}' for h in TOURNAMENT_HORIZONS)
        if strategy_allowlist is not None and (not strategy_allowlist or not set(strategy_allowlist)<=known):
            raise ValueError('TOURNAMENT_STRATEGY_ALLOWLIST_INVALID')
        self.strategy_allowlist=strategy_allowlist
        self.market_deadlines={}
        self.input_failures={}
        self.decision_engine = StrategyDecisionEngine()
        self.feature_engine = CausalBtcFeatureEngine(external_store)
        self.seen: set[tuple[str, str]] = set()
        self.pending: list[PendingTournamentOrder] = []
        self.book_history: deque[tuple[int, MarketBookFeatures]] = deque(maxlen=1800)
        self.last_history_us = 0
        self.book_history_slug: str | None = None

    def prune_memory(self,*,now_us,limit=10000):
        busy={p.slug for p in self.pending}
        retired={slug for slug,end in self.market_deadlines.items() if end+600_000_000<now_us and slug not in busy}
        self.seen={key for key in self.seen if key[0] not in retired}
        for slug in retired:self.market_deadlines.pop(slug)
        self.input_failures={key:value for key,value in self.input_failures.items() if key[0] not in retired}
        if len(self.seen)>limit or len(self.pending)>limit or len(self.market_deadlines)>limit:
            raise RuntimeError('TOURNAMENT_RESOURCE_STATE_LIMIT')
        return {'seen':len(self.seen),'pending':len(self.pending)}

    def observe_book(self, *, collector: Any, now_us: int) -> None:
        up = _state_for_outcome(collector, "UP")
        down = _state_for_outcome(collector, "DOWN")
        if up is None or down is None or up.slug != down.slug:
            return
        if self.book_history_slug != up.slug:
            self.book_history.clear()
            self.last_history_us = 0
            self.book_history_slug = up.slug
        if now_us - self.last_history_us < self.config.tournament_history_ms * 1000:
            return
        self.last_history_us = now_us
        try:
            up = _state_for_outcome(collector, "UP")
            down = _state_for_outcome(collector, "DOWN")
            if up is None or down is None:
                return
            if any(s.age_seconds(now_us) is None or s.age_seconds(now_us) > self.config.max_book_age_seconds
                   for s in (up, down)):
                return
            book = market_book_features(up, down, depth_band=0.02)
        except ValueError:
            return
        self.book_history.append((now_us, book))

    def _previous_book(self, *, now_us: int, lookback_seconds: int, slug: str | None = None) -> MarketBookFeatures | None:
        if slug is not None and slug != self.book_history_slug:
            return None
        target = now_us - int(lookback_seconds * MICROS)
        for ts_us, book in reversed(self.book_history):
            if ts_us <= target:
                return book
        return None

    @staticmethod
    def _token_for(market: DiscoveredMarket, outcome: str) -> str:
        return market.up_token_id if outcome.upper() == "UP" else market.down_token_id

    def _save_skip(
        self,
        *,
        strategy: str,
        market: DiscoveredMarket,
        now_us: int,
        reason: str,
        metadata: object,
    ) -> None:
        order_id = f"skip_{hashlib.sha256(f'{self.session_id}|{strategy}|{market.slug}'.encode()).hexdigest()[:24]}"
        self.store.save_order(
            order_id=order_id,
            session_id=self.session_id,
            strategy=strategy,
            signal_id=None,
            slug=market.slug,
            token_id=market.up_token_id,
            outcome="UP",
            side="SKIP",
            style="DECISION_ONLY",
            price_limit=None,
            shares="0",
            created_at_us=now_us,
            scheduled_at_us=now_us,
            status="SKIPPED",
            reason=reason,
            metadata=metadata,
        )

    def _schedule(
        self,
        *,
        strategy: str,
        cfg: CommonExperimentConfig,
        market: DiscoveredMarket,
        outcome: str,
        ask: Decimal,
        now_us: int,
        metadata: object,
    ) -> None:
        token_id = self._token_for(market, outcome)
        order_id = f"tourn_{hashlib.sha256(f'{self.session_id}|{strategy}|{market.slug}'.encode()).hexdigest()[:24]}"
        scheduled_at = now_us + int(self.config.tournament_execution_delay_ms * 1000)
        pending = PendingTournamentOrder(
            order_id=order_id,
            strategy=strategy,
            slug=market.slug,
            token_id=token_id,
            outcome=outcome,
            shares=Decimal(str(cfg.shares)),
            decision_ask=ask,
            slippage_limit=Decimal(str(cfg.slippage_limit)),
            max_quote_age_seconds=float(cfg.max_quote_age_seconds),
            created_at_us=now_us,
            scheduled_at_us=scheduled_at,
        )
        self.pending.append(pending)
        self.store.save_order(
            order_id=order_id,
            session_id=self.session_id,
            strategy=strategy,
            signal_id=None,
            slug=market.slug,
            token_id=token_id,
            outcome=outcome,
            side="BUY",
            style="FAK_PAPER",
            price_limit=_fmt_decimal(min(Decimal("0.999"), ask + pending.slippage_limit)),
            shares=_fmt_decimal(pending.shares) or "0",
            created_at_us=now_us,
            scheduled_at_us=scheduled_at,
            status="SCHEDULED",
            reason=None,
            metadata=metadata,
        )

    def _cheap_contra(
        self,
        *,
        horizon: int,
        market: DiscoveredMarket,
        book: MarketBookFeatures,
        features: Any,
        now_us: int,
    ) -> None:
        strategy = f"CTRL_WALLET_CHEAP_CONTRA_H{horizon:03d}"
        key = (market.slug, strategy)
        if key in self.seen:
            return
        self.seen.add(key)
        r5 = features.returns.get(5)
        if r5 is None or abs(r5) < 0.00010:
            self._save_skip(strategy=strategy, market=market, now_us=now_us, reason="RETURN_TOO_SMALL", metadata={"horizon": horizon, "return_5s": r5})
            return
        outcome = "DOWN" if r5 > 0 else "UP"
        side = book.down if outcome == "DOWN" else book.up
        if side.best_ask > 0.45:
            self._save_skip(strategy=strategy, market=market, now_us=now_us, reason="CHEAP_SIDE_NOT_CHEAP", metadata={"horizon": horizon, "return_5s": r5, "ask": side.best_ask})
            return
        if side.spread > 0.06:
            self._save_skip(strategy=strategy, market=market, now_us=now_us, reason="SPREAD_TOO_WIDE", metadata={"horizon": horizon, "return_5s": r5, "spread": side.spread})
            return
        cfg = _forward_common(horizon=horizon, wallet_informed=True)
        self._schedule(
            strategy=strategy,
            cfg=cfg,
            market=market,
            outcome=outcome,
            ask=Decimal(str(side.best_ask)),
            now_us=now_us,
            metadata={
                "profile": "WALLET_CONTROL",
                "family": "cheap_side_contrarian",
                "horizon_seconds": horizon,
                "return_5s": r5,
                "decision_ask": side.best_ask,
            },
        )

    def evaluate_due(self, *, market: DiscoveredMarket, collector: Any, now_us: int) -> dict[str, int]:
        stats: dict[str, int] = {}
        self.market_deadlines[market.slug]=market.round_end_us
        if not self.config.enable_full_tournament or collector is None:
            return stats
        seconds_remaining = (market.round_end_us - now_us) / MICROS
        if seconds_remaining <= 0:
            return stats
        current_book: MarketBookFeatures | None = None
        full_two_sided_book = False
        up = _state_for_outcome(collector, "UP")
        down = _state_for_outcome(collector, "DOWN")
        if (up is not None and down is not None
                and all(s.age_seconds(now_us) is not None and s.age_seconds(now_us) <= self.config.max_book_age_seconds
                        for s in (up, down))):
            try:
                current_book = market_book_features(up, down, depth_band=0.02)
                full_two_sided_book = True
            except ValueError:
                # Legitimate near-boundary markets can be synchronized but one-sided
                # (for example UP bid=.999/no ask and DOWN no bid/ask=.001).
                # Complete missing sides only at the binary 0/1 bounds for decision
                # math. Actual scheduling still requires the real ask to exist.
                try:
                    current_book = boundary_completed_market_book_features(up, down, depth_band=0.02)
                except ValueError:
                    current_book = None

        for horizon in TOURNAMENT_HORIZONS:
            due_us = market.round_end_us - horizon * MICROS
            if now_us < due_us:
                continue
            group_key = (market.slug, f"H{horizon:03d}")
            # The group is complete when every pre-registered strategy has been seen.
            group_names = [name for name, _ in full_tournament_specs(horizon)] + [f"CTRL_WALLET_CHEAP_CONTRA_H{horizon:03d}"]
            if self.strategy_allowlist is not None:group_names=[name for name in group_names if name in self.strategy_allowlist]
            if all((market.slug, name) in self.seen for name in group_names):
                continue
            lateness = (now_us - due_us) / MICROS
            if lateness > self.config.tournament_max_decision_gap_seconds:
                late_reason = "MISSED_DECISION_WINDOW_BOOK_UNAVAILABLE" if current_book is None else "MISSED_DECISION_WINDOW"
                for name in group_names:
                    key = (market.slug, name)
                    if key in self.seen:
                        continue
                    self.seen.add(key)
                    self._save_skip(strategy=name, market=market, now_us=now_us, reason=late_reason, metadata={"horizon_seconds": horizon, "lateness_seconds": lateness,"last_input_failure":self.input_failures.get(group_key)})
                    stats["SKIPPED_LATE"] = stats.get("SKIPPED_LATE", 0) + 1
                continue
            if current_book is None:
                # Retry while still inside the configured decision-gap window.
                continue

            # Build the causal state once and share it across strategies at this horizon.
            # Phase 5.9 keeps the original dual-source path whenever possible, but a
            # single fresh external predictor may carry non-cross-exchange strategies.
            # S7 still requires both exchanges and is skipped explicitly below.
            feature_mode = "DUAL_SOURCE"
            try:
                PriceReferenceMapper(self.external_store).map_existing_exact(market, now_us)
                features = self.feature_engine.build(
                    market=market,
                    decision_time_us=now_us,
                    return_windows=(5, 10, 15, 30, 60),
                    volatility_windows=(15, 30, 60, 120),
                )
                lab = build_strategy_lab_features(features)
                model = TwapSettlementModel(
                    self.feature_engine,
                    SettlementModelConfig(paths=self.config.model_paths, volatility_lookback_seconds=60),
                ).evaluate(market=market, decision_time_us=now_us, features=features)
            except FeatureUnavailable as exc:
                self.input_failures[group_key]=str(exc)
                if str(exc) not in {"STALE_KRAKEN_HISTORY", "STALE_COINBASE_HISTORY"}:
                    # Retry inside the decision-gap window rather than converting a
                    # transient non-predictor gap into a permanent skip.
                    continue
                try:
                    features = self.feature_engine.build_available_sources(
                        market=market,
                        decision_time_us=now_us,
                        return_windows=(5, 10, 15, 30, 60),
                        volatility_windows=(15, 30, 60, 120),
                    )
                    if len(features.current_sources) != 1:
                        continue
                    feature_mode = "SINGLE_SOURCE_FALLBACK"
                    lab = build_strategy_lab_features(features)
                    model = TwapSettlementModel(
                        self.feature_engine,
                        SettlementModelConfig(
                            paths=self.config.model_paths,
                            volatility_lookback_seconds=60,
                            allow_single_source_known_samples=True,
                        ),
                    ).evaluate(market=market, decision_time_us=now_us, features=features)
                except FeatureUnavailable as fallback_error:
                    self.input_failures[group_key]=str(fallback_error)
                    continue

            self.input_failures.pop(group_key,None)

            control_name = f"CTRL_WALLET_CHEAP_CONTRA_H{horizon:03d}"
            if control_name not in group_names:
                pass
            elif full_two_sided_book:
                self._cheap_contra(horizon=horizon, market=market, book=current_book, features=features, now_us=now_us)
            elif (market.slug, control_name) not in self.seen:
                self.seen.add((market.slug, control_name))
                self._save_skip(
                    strategy=control_name,
                    market=market,
                    now_us=now_us,
                    reason="FULL_TWO_SIDED_BOOK_REQUIRED",
                    metadata={"horizon_seconds": horizon, "book_quality": "ONE_SIDED"},
                )
                stats["SKIP"] = stats.get("SKIP", 0) + 1

            for strategy, cfg in full_tournament_specs(horizon):
                if strategy not in group_names:continue
                key = (market.slug, strategy)
                if key in self.seen:
                    continue
                if cfg.strategy_family is StrategyFamily.CROSS_EXCHANGE and len(features.current_sources) < 2:
                    self.seen.add(key)
                    self._save_skip(
                        strategy=strategy,
                        market=market,
                        now_us=now_us,
                        reason="CURRENT_DUAL_SOURCE_REQUIRED",
                        metadata={
                            "profile": "WALLET" if "_WALLET_" in strategy else "BASE",
                            "family": cfg.strategy_family.value,
                            "horizon_seconds": horizon,
                            "feature_mode": feature_mode,
                            "current_sources": list(features.current_sources),
                        },
                    )
                    stats["SKIP"] = stats.get("SKIP", 0) + 1
                    continue
                if (not full_two_sided_book) and cfg.strategy_family in {
                    StrategyFamily.BOOK_IMBALANCE,
                    StrategyFamily.MICROSTRUCTURE,
                }:
                    self.seen.add(key)
                    self._save_skip(
                        strategy=strategy,
                        market=market,
                        now_us=now_us,
                        reason="FULL_TWO_SIDED_BOOK_REQUIRED",
                        metadata={
                            "profile": "WALLET" if "_WALLET_" in strategy else "BASE",
                            "family": cfg.strategy_family.value,
                            "horizon_seconds": horizon,
                            "book_quality": "ONE_SIDED",
                        },
                    )
                    stats["SKIP"] = stats.get("SKIP", 0) + 1
                    continue

                previous = None
                if cfg.strategy_family is StrategyFamily.MICROSTRUCTURE:
                    previous = self._previous_book(now_us=now_us, lookback_seconds=cfg.params.pm_move_lookback_seconds,
                                                   slug=market.slug)
                inputs = StrategyInputs(
                    btc=features,
                    base_p_up=model.p_up,
                    book=current_book,
                    previous_book=previous,
                    lab=lab,
                )
                decision = self.decision_engine.evaluate(cfg, inputs)
                self.seen.add(key)
                meta = {
                    "profile": "WALLET" if "_WALLET_" in strategy else "BASE",
                    "family": cfg.strategy_family.value,
                    "horizon_seconds": horizon,
                    "base_p_up": decision.base_p_up,
                    "final_p_up": decision.final_p_up,
                    "signal_score": decision.signal_score,
                    "up_edge": decision.up_edge,
                    "down_edge": decision.down_edge,
                    "reason": decision.reason,
                    "time_regime": decision.time_regime,
                    "volatility_regime": decision.volatility_regime,
                    "signal_components": [asdict(x) for x in decision.signal_components],
                    "feature_mode": feature_mode,
                    "current_sources": list(features.current_sources),
                }
                if decision.action is Action.SKIP:
                    self._save_skip(strategy=strategy, market=market, now_us=now_us, reason=decision.reason, metadata=meta)
                    stats["SKIP"] = stats.get("SKIP", 0) + 1
                    continue
                outcome = "UP" if decision.action is Action.BUY_UP else "DOWN"
                raw_state = up if outcome == "UP" else down
                raw_ask = None if raw_state is None else raw_state.best_ask
                if raw_state is None or not raw_state.synchronized or raw_ask is None:
                    self._save_skip(
                        strategy=strategy,
                        market=market,
                        now_us=now_us,
                        reason="NO_EXECUTABLE_ASK",
                        metadata=meta | {"book_quality": "ONE_SIDED"},
                    )
                    stats["SKIP"] = stats.get("SKIP", 0) + 1
                    continue
                ask_value = float(raw_ask.price)
                self._schedule(
                    strategy=strategy,
                    cfg=cfg.common,
                    market=market,
                    outcome=outcome,
                    ask=Decimal(str(ask_value)),
                    now_us=now_us,
                    metadata=meta | {"decision_ask": ask_value},
                )
                stats["SCHEDULED"] = stats.get("SCHEDULED", 0) + 1
        return stats

    @_atomic_paper_update
    def execute_due(self, *, collector_pool: OverlapCollectorPool, market_store: MarketStore, now_us: int) -> dict[str, int]:
        stats: dict[str, int] = {}
        due = [p for p in self.pending if p.scheduled_at_us <= now_us]
        self.pending = [p for p in self.pending if p.scheduled_at_us > now_us]
        for pending in due:
            now_us = self.paper_engine.execution_time(now_us)
            block = self.store.taker_execution_block_reason(pending.order_id)
            if block is not None:
                stats[block] = stats.get(block, 0) + 1
                continue
            pos = self.paper_engine.position_by_slug(pending.strategy, pending.slug, market_store)
            reason = self.paper_engine.position_execution_reason(pos, now_us)
            if reason is not None:
                self.store.update_order_status(pending.order_id, status="REJECTED", reason=reason)
                stats[reason] = stats.get(reason, 0) + 1
                continue
            collector = collector_pool.collector_for_slug(pending.slug)
            state = _state_for_outcome(collector, pending.outcome)
            reason = self.paper_engine.execution_identity_reason(slug=pending.slug,
                token_id=pending.token_id, outcome=pending.outcome, state=state,
                collector=collector, market_store=market_store)
            if reason is not None:
                self.store.update_order_status(pending.order_id, status="REJECTED", reason=reason)
                stats[reason] = stats.get(reason, 0) + 1
                continue
            if state is None or not state.synchronized:
                self.store.update_order_status(pending.order_id, status="REJECTED", reason="BOOK_UNAVAILABLE")
                stats["BOOK_UNAVAILABLE"] = stats.get("BOOK_UNAVAILABLE", 0) + 1
                continue
            if not state.depth_reliable:
                self.store.update_order_status(pending.order_id, status="REJECTED", reason="BOOK_UNRELIABLE")
                stats["BOOK_UNRELIABLE"] = stats.get("BOOK_UNRELIABLE", 0) + 1
                continue
            age = state.age_seconds(now_us)
            if age is None or age > pending.max_quote_age_seconds:
                self.store.update_order_status(pending.order_id, status="REJECTED", reason="STALE_BOOK")
                stats["STALE_BOOK"] = stats.get("STALE_BOOK", 0) + 1
                continue
            if pending.strategy.endswith(('_BOOK_IMBALANCE','_MICROSTRUCTURE')):
                reason=self.paper_engine.paired_book_execution_reason(collector,now_us,pending.max_quote_age_seconds,market_store=market_store,slug=pending.slug)
                if reason:
                    self.store.update_order_status(pending.order_id,status='REJECTED',reason=reason)
                    stats[reason]=stats.get(reason,0)+1
                    continue
            if self.config.strict_market_minimum:
                if state.min_order_size is None or not state.min_order_size.is_finite() or state.min_order_size <= 0:
                    self.store.update_order_status(pending.order_id, status="REJECTED", reason="MARKET_MINIMUM_UNKNOWN")
                    stats["MARKET_MINIMUM_UNKNOWN"] = stats.get("MARKET_MINIMUM_UNKNOWN", 0) + 1
                    continue
                if pending.shares < state.min_order_size:
                    self.store.update_order_status(pending.order_id, status="REJECTED", reason="SUBMITTED_BELOW_MARKET_MINIMUM")
                    stats["SUBMITTED_BELOW_MARKET_MINIMUM"] = stats.get("SUBMITTED_BELOW_MARKET_MINIMUM", 0) + 1
                    continue
                if state.tick_size not in SUPPORTED_TICKS:
                    self.store.update_order_status(pending.order_id, status="REJECTED", reason="MARKET_TICK_UNKNOWN")
                    stats["MARKET_TICK_UNKNOWN"] = stats.get("MARKET_TICK_UNKNOWN", 0) + 1
                    continue
            max_price = min(Decimal("0.999"), pending.decision_ask + pending.slippage_limit)
            max_price = _tick_limit(state, max_price, "BUY")
            fee_rate, fee_exponent, fee_source = self.paper_engine.taker_fee_rate_for_slug(pending.slug)
            preview = self.paper_engine._liquidity_view(pending.strategy, state, "BUY")
            submitted, plan = _sized_taker_plan(
                preview[3],
                side="BUY",
                shares=pending.shares,
                fee_rate=fee_rate,
                fee_exponent=fee_exponent,
                max_price=max_price,
            )
            if plan is None:
                self.store.update_order_status(pending.order_id, status="REJECTED", reason="NO_EXECUTABLE_SIZE")
                stats["NO_EXECUTABLE_SIZE"] = stats.get("NO_EXECUTABLE_SIZE", 0) + 1
                continue
            self.store.update_order_submitted_shares(pending.order_id, _fmt_decimal(submitted) or "0",
                price_limit=_fmt_decimal(max_price))
            if self.config.strict_market_minimum and submitted < state.min_order_size:
                self.store.update_order_status(pending.order_id, status="REJECTED", reason="SUBMITTED_BELOW_MARKET_MINIMUM")
                stats["SUBMITTED_BELOW_MARKET_MINIMUM"] = stats.get("SUBMITTED_BELOW_MARKET_MINIMUM", 0) + 1
                continue
            if plan.filled_shares <= 0 or plan.vwap is None:
                self.store.update_order_status(pending.order_id, status="REJECTED", reason="NO_EXECUTABLE_LIQUIDITY")
                stats["NO_LIQUIDITY"] = stats.get("NO_LIQUIDITY", 0) + 1
                continue
            pos = self.paper_engine.position_by_slug(pending.strategy, pending.slug, market_store)
            if pos is None:
                self.store.update_order_status(pending.order_id, status="REJECTED", reason="UNKNOWN_MARKET")
                stats["UNKNOWN_MARKET"] = stats.get("UNKNOWN_MARKET", 0) + 1
                continue
            now_us = self.paper_engine.execution_time(now_us)
            reason = self.paper_engine.position_execution_reason(pos, now_us)
            age = state.age_seconds(now_us)
            if reason is None and not state.depth_reliable:
                reason = "BOOK_UNRELIABLE"
            if reason is None and (age is None or age > pending.max_quote_age_seconds):
                reason = "STALE_BOOK"
            if reason is not None:
                self.store.update_order_status(pending.order_id, status="REJECTED", reason=reason)
                stats[reason] = stats.get(reason, 0) + 1
                continue
            pos.record_fill(
                outcome=pending.outcome,
                side="BUY",
                shares=plan.filled_shares,
                notional=plan.notional,
                fee=plan.fee,
            )
            self.paper_engine._persist_position(pending.strategy, pos)
            self.paper_engine._consume_liquidity(preview, plan)
            self.store.save_fill(
                fill_id=f"tf_{uuid.uuid4().hex}",
                order_id=pending.order_id,
                session_id=self.session_id,
                strategy=pending.strategy,
                slug=pending.slug,
                token_id=pending.token_id,
                outcome=pending.outcome,
                side="BUY",
                fill_ts_us=now_us,
                shares=_fmt_decimal(plan.filled_shares) or "0",
                price=_fmt_decimal(plan.vwap) or "0",
                notional=_fmt_decimal(plan.notional) or "0",
                fee=_fmt_decimal(plan.fee) or "0",
                liquidity_role="TAKER_PAPER",
                metadata={
                    "decision_ask": _fmt_decimal(pending.decision_ask),
                    "fill_audit_revision":"six_hour_taker_fill_provenance_v1",
                    "execution_at_us":now_us,
                    "book_source_ts_us":state.last_server_ts_us,
                    "execution_admission_policy_revision":EXECUTION_ADMISSION_REVISION,
                    "book_received_at_us":state.last_received_at_us,
                    "book_sync_generation":state.sync_generation,
                    "tick_size":_fmt_decimal(state.tick_size),
                    "max_price": _fmt_decimal(max_price),
                    "requested_shares": _fmt_decimal(plan.requested_shares),
                    "partial": plan.filled_shares < plan.requested_shares,
                    "levels": [asdict(x) for x in plan.levels],
                    "fee_rate": _fmt_decimal(fee_rate),
                    "fee_exponent": fee_exponent,
                    "fee_source": fee_source,
                    "market_min_order_size": _fmt_decimal(state.min_order_size),
                    "submitted_shares": _fmt_decimal(submitted),
                    "minimum_submission_eligible": bool(
                        state.min_order_size is not None and submitted >= state.min_order_size
                    ),
                    "decision_shares": _fmt_decimal(pending.shares),
                    "unfilled_shares_cancelled": _fmt_decimal(submitted - plan.filled_shares),
                    "execution_policy_revision": EXECUTION_POLICY_REVISION,
                },
            )
            status = "FILLED" if plan.filled_shares == plan.requested_shares else "PARTIAL"
            self.store.update_order_status(pending.order_id, status=status,
                reason=None if status == "FILLED" else "FAK_REMAINDER_CANCELLED")
            stats[status] = stats.get(status, 0) + 1
        return stats


def verify_full_tournament_config() -> dict[str, bool]:
    names: list[str] = []
    for horizon in TOURNAMENT_HORIZONS:
        names.extend(name for name, _ in full_tournament_specs(horizon))
        names.append(f"CTRL_WALLET_CHEAP_CONTRA_H{horizon:03d}")
    sample_specs = dict(full_tournament_specs(60))
    base_mom = sample_specs["S3_BASE_H060_MOMENTUM"].params
    wal_mom = sample_specs["S3_WALLET_H060_MOMENTUM"].params
    wal_mr = sample_specs["S4_WALLET_H060_MEAN_REVERSION"].params
    wal_book = sample_specs["S5_WALLET_H060_BOOK_IMBALANCE"].params
    checks = {
        "six frozen decision horizons": TOURNAMENT_HORIZONS == (240, 180, 120, 60, 30, 15),
        "42 baseline variants": sum(1 for n in names if "_BASE_" in n) == 42,
        "36 wallet-informed variants": sum(1 for n in names if "_WALLET_" in n and not n.startswith("CTRL_")) == 36,
        "six cheap-side contrarian controls": sum(1 for n in names if n.startswith("CTRL_")) == 6,
        "84 distinct tournament variants": len(names) == 84 and len(set(names)) == 84,
        "wallet momentum is stricter and lower-weight": isinstance(base_mom, MomentumParams) and isinstance(wal_mom, MomentumParams) and wal_mom.minimum_return > base_mom.minimum_return and wal_mom.signal_weight_beta < base_mom.signal_weight_beta,
        "wallet mean-reversion uses 15s extension and 5s reversal": isinstance(wal_mr, MeanReversionParams) and wal_mr.extension_lookback_seconds == 15 and wal_mr.reversal_lookback_seconds == 5,
        "wallet book signal does not claim strong forensic certainty": isinstance(wal_book, BookImbalanceParams) and wal_book.signal_weight_beta <= 0.20,
        "taker execution uses actual delayed L2": _forward_common(horizon=60, wallet_informed=False).execution_delay_seconds > 0,
        "real trading remains absent": True,
    }
    return checks


async def _bounded_btc_discovery(rest: PolymarketPublicRest, *, timeout_seconds: float) -> DiscoveredMarket:
    """Bound one discovery attempt so it can never stall the hot forward loop.

    Only the current, next, and immediately previous 5-minute slugs are needed in
    live mode.  The task itself is launched in the background; this timeout is a
    second fail-safe for slow Gamma responses.
    """
    return await asyncio.wait_for(
        rest.discover_btc_5m(candidate_offsets=(0, 1, -1)),
        timeout=timeout_seconds,
    )


async def _bounded_market_fee_info(
    rest: PolymarketPublicRest,
    market: DiscoveredMarket,
    *,
    timeout_seconds: float = 2.0,
) -> tuple[str, Decimal, int, dict[str, Any]]:
    payload = await asyncio.wait_for(
        rest.get_clob_market_info(market.condition_id),
        timeout=timeout_seconds,
    )
    fd = payload.get("fd")
    if not isinstance(fd, dict) or fd.get("r") is None:
        raise RuntimeError("CLOB market info did not expose an fd fee block")
    rate = Decimal(str(fd.get("r")))
    exponent = int(fd.get("e", 1))
    if rate < 0:
        raise RuntimeError("negative fee rate")
    return market.slug, rate, exponent, payload


def _active_pool_market(pool: Any, *, asset: str, now_us: int) -> tuple[DiscoveredMarket | None, Any | None]:
    """Return the pool market only while its contract window is actually live.

    Discovery may temporarily fall back to the most recent expired event when the
    public event API is late publishing the new 5-minute market.  Forward logic
    must never continue evaluating an expired round in that gap.
    """
    market, collector = pool.current(asset)
    if market is None:
        return None, None
    if not (market.round_start_us <= now_us < market.round_end_us):
        return None, None
    return market, collector



def _wallet_detection_sample(*, row, pool, observed_at_us: int, session_id: str,
                             wallet: str, external_store, model_paths: int):
    """Bind public attribution to its verified contract and original observable clock."""
    slug = _row_slug(row)
    collector = pool.collector_for_slug(slug)
    market = None if collector is None else collector.market
    reason = None
    if market is None:
        reason = "SIGNAL_MARKET_UNAVAILABLE"
    elif (market.slug != slug or not slug.startswith("btc-updown-5m-")
          or market.round_end_us - market.round_start_us != 300 * MICROS
          or slug != f"btc-updown-5m-{market.round_start_us // MICROS}"
          or market.round_start_us % (300 * MICROS) != 0
          or row.token_id not in market.token_ids
          or market.outcome_for_token(row.token_id) != (row.outcome or "").upper()):
        reason = "SIGNAL_MARKET_MISMATCH"
    elif not market.round_start_us <= observed_at_us < market.round_end_us:
        reason = "SIGNAL_MARKET_NOT_LIVE_AT_DETECTION"
    sample = _sample_from_state(
        session_id=session_id, wallet=wallet, market=market if reason is None else None,
        # A current book is not a historical book snapshot. Never attach future L2
        # to a frozen detection-time model sample; execution checks current L2 later.
        book_collector=None, external_store=external_store, model_paths=model_paths,
        decision_time_us=observed_at_us)
    if reason is not None:
        return replace(sample, slug=slug, unavailable_reason=reason)
    return sample


async def run_forward_wallet_lab(
    root: Path,
    *,
    requested_wallet: str,
    seconds: float | None,
    config: ForwardLabConfig,
) -> int:
    if not config.strict_market_minimum:
        raise ValueError("STRICT_EXECUTION_REQUIRED: forward sessions require market minimum and tick validation")
    config.validate()
    from ..feeds.clock_health import require_host_sync,host_sync_status
    host_preflight=await asyncio.to_thread(require_host_sync)
    session_id = str(uuid.uuid4())
    started_us = now_epoch_us()
    deadline_mono = None if seconds is None else time.perf_counter() + seconds

    async with PolymarketDataClient() as resolver, PolymarketPublicRest() as rest, FastAttributionPoller() as poller:
        wallet, _ = await resolver.resolve_proxy_wallet(requested_wallet)
        initial_market = await rest.discover_btc_5m()
        baseline_start = max(1, int(started_us / MICROS) - config.recent_window_seconds)

        with (
            MarketStore.open_project(root) as market_store,
            ExternalStore.open_project(root) as cb_store,
            ExternalStore.open_project(root) as kr_store,
            ExternalStore.open_project(root) as twap_store,
            ExternalStore.open_project(root) as read_store,
            ForensicsStore.open_project(root) as forensic_store,
            ForwardWalletStore.open_project(root) as forward_store,
        ):
            frozen_hash = phase591_config_hash(config)
            forward_store.start_session(
                session_id=session_id,
                wallet=wallet,
                started_at_us=started_us,
                config={"phase": PHASE_VERSION, "execution_policy_revision": EXECUTION_POLICY_REVISION,'host_clock_preflight':host_preflight,
                        "execution_admission_policy_revision":EXECUTION_ADMISSION_REVISION,
                        "resource_policy_revision":"offline_durable_retention_v1",
                        "reference_integrity_policy_revision":REFERENCE_INTEGRITY_POLICY_REVISION,
                        "quote_projection_policy_revision":QUOTE_PROJECTION_POLICY_REVISION,
                        "maker_execution_policy_revision":config.s8_research_policy,
                        "model_policy_revision": MODEL_POLICY_REVISION, "settlement_policy_revision": RESOLUTION_POLICY_REVISION,
                        "accounting_policy_revision": ACCOUNTING_POLICY_REVISION,
                        "storage_profile": STORAGE_PROFILE, "config_hash": frozen_hash, **asdict(config)},
            )
            forensic_store.start_live_session(
                session_id=session_id,
                wallet=wallet,
                requested_wallet=requested_wallet,
                started_at_us=started_us,
                config={"phase5_9_1_wallet_alpha_lab": True, "execution_policy_revision": EXECUTION_POLICY_REVISION,
                        "execution_admission_policy_revision":EXECUTION_ADMISSION_REVISION,
                        "resource_policy_revision":"offline_durable_retention_v1",
                        "reference_integrity_policy_revision":REFERENCE_INTEGRITY_POLICY_REVISION,
                        "quote_projection_policy_revision":QUOTE_PROJECTION_POLICY_REVISION,
                        "maker_execution_policy_revision":config.s8_research_policy,
                        "model_policy_revision": MODEL_POLICY_REVISION, "settlement_policy_revision": RESOLUTION_POLICY_REVISION,
                        "accounting_policy_revision": ACCOUNTING_POLICY_REVISION,
                        "storage_profile": STORAGE_PROFILE, "config_hash": frozen_hash, **asdict(config)},
            )
            coinbase = CoinbaseCollector(root=root, store=cb_store)
            kraken = KrakenCollector(root=root, store=kr_store)
            twap = AuthenticatedTwapCollector(root=root, store=twap_store, market=initial_market)
            pool = OverlapCollectorPool(root=root, rest=rest, store=market_store,
                                        grace_seconds=config.rollover_grace_seconds, forward_session_id=session_id)
            await pool.ensure_current(initial_market)
            clock_guard = RuntimeClockHealth(utc_us=now_epoch_us(), monotonic_seconds=time.monotonic())
            clock_guard.record_host_sync(host_preflight)
            def execution_clock() -> int:
                return clock_guard.validate(now_epoch_us(), time.monotonic())
            engine = ForwardPaperEngine(session_id=session_id, config=config, store=forward_store,
                                        execution_clock=execution_clock)
            long_tasks = [asyncio.create_task(coinbase.run()), asyncio.create_task(kraken.run()), asyncio.create_task(twap.run())]
            async def monitor_host_sync():
                while True:
                    await asyncio.sleep(60)
                    status=await asyncio.to_thread(host_sync_status)
                    clock_guard.record_host_sync(status)
                    if status['state']!='SERVICE_REPORTS_SYNCHRONIZED':
                        clock_guard.failed=True
                        raise RuntimeError('CLOCK_SYNC_UNVERIFIED: time-service synchronization was lost')
            sync_monitor=asyncio.create_task(monitor_host_sync())
            long_tasks.append(sync_monitor)
            tournament = (
                FullForwardTournament(
                    session_id=session_id,
                    store=forward_store,
                    paper_engine=engine,
                    external_store=read_store,
                    config=config,
                )
                if config.enable_full_tournament
                else None
            )

            source_stats: dict[str, Any] = {
                "trades": {"polls": 0, "200": 0, "429": 0, "503": 0, "rtts_us": deque(maxlen=4096)},
                "activity": {"polls": 0, "200": 0, "429": 0, "503": 0, "rtts_us": deque(maxlen=4096)},
                "first_source": {"trades": 0, "activity": 0},
                "clob_pre_attribution": 0,
            }
            signal_source: dict[str, tuple[str, int]] = {}
            seq_signal = 0
            latest_sample = None
            loop = asyncio.get_running_loop()
            next_trades_poll = loop.time()
            next_activity_poll = loop.time() + min(0.05, config.activity_poll_ms / 2000.0)
            next_telemetry = loop.time()
            next_discovery = loop.time()
            discovery_task: asyncio.Task[DiscoveredMarket] | None = None
            fee_task: asyncio.Task[tuple[str, Decimal, int, dict[str, Any]]] | None = asyncio.create_task(
                _bounded_market_fee_info(rest, initial_market)
            )
            fee_attempted_slugs: set[str] = {initial_market.slug}
            discovery_failures = 0
            poll_tasks: dict[str, asyncio.Task[PollBatch] | None] = {"trades": None, "activity": None}
            last_status = loop.time()
            last_market: DiscoveredMarket | None = initial_market
            last_loop_enter = loop.time()
            max_loop_gap_ms = 0.0
            last_sample_compute_ms = 0.0
            next_storage_health = loop.time()
            latest_storage = None

            print("BTC Lab V3 - PHASE 5.9.2 EVIDENCE-ROUTED WALLET ALPHA FORWARD LAB" if config.enable_full_tournament else "BTC Lab V3 - PHASE 5.9.2 EVIDENCE-ROUTED WALLET ALPHA LAB")
            print(f"  session          {session_id}")
            print(f"  config hash      {frozen_hash[:16]}... (frozen before first signal)")
            print(f"  target wallet    {wallet}")
            print(f"  S8               {config.s8_research_policy}, PAPER")
            print(f"  S9A              literal target-wallet mirror, PAPER")
            print(f"  S9B              protected target-wallet mirror, PAPER")
            print("  S9 controls       literal copy + legacy filters + BOOK-CONFIRM-75")
            print("  S9 challengers    BOOK-MODEL30/40 + FIRST3-LARGE-DET7.5 + LARGE-DET7.5")
            print("  S9 router         ROUTER-A: specialist FIRST3+LARGE+DET7.5 else BOOK+MODEL30")
            if config.enable_full_tournament:
                print("  S1-S7            42 BASE variants across 240/180/120/60/30/15s")
                print("  wallet variants  36 pre-registered wallet-informed variants")
                print("  controls         6 cheap-side contrarian controls")
                print(f"  total variants   {84 + 1 + len(PHASE590_WALLET_STRATEGIES) + 2} including S8 + wallet-copy + new EDGE arm")
                print(f"  tournament delay {config.tournament_execution_delay_ms}ms nominal local paper delay")
            print(f"  attribution      /v2/trades {config.trades_poll_ms}ms + /v2/activity {config.activity_poll_ms}ms race")
            print("  CLOB clock       event-level market trades/L2; used for context, not wallet identity")
            print(f"  copy delay       {config.copy_execution_delay_ms}ms local paper reaction after first public attribution")
            print(f"  rollover         {config.rollover_grace_seconds:.0f}s collector overlap")
            print("  maker model      displayed-queue-ahead, exact-price aggressive-sell fills only")
            print("  taker fees       live CLOB fd schedule when available; %.4f fallback" % config.taker_fee_rate)
            print("  S8 invariant     aggressor received_at must be strictly after maker creation")
            print(f"  market storage   {STORAGE_PROFILE}; full payloads stay in gzip raw archive")
            print("  disk guard       stop cleanly below 8 GiB free; warn on abnormal DB growth")
            print("  orders           PAPER ONLY")
            print("  real trading     DISABLED")
            print("  stop             Ctrl+C")

            try:
                # The public wallet baseline still completes before any signal is
                # admitted. Independent books/reference feeds warm up concurrently;
                # a slow attribution endpoint must not delay their first subscription.
                baseline_trades, baseline_activity = await asyncio.gather(
                    poller.poll(source="trades", wallet=wallet, start_epoch_s=baseline_start),
                    poller.poll(source="activity", wallet=wallet, start_epoch_s=baseline_start),
                )
                baseline_rows: list[WalletTrade | WalletActivity] = [*baseline_trades.rows, *baseline_activity.rows]
                seen_economic = {economic_key(row) for row in baseline_rows if _row_is_trade(row)}
                seen_source_rows: dict[str, set[str]] = {
                    "trades": {_row_id(r) for r in baseline_trades.rows},
                    "activity": {_row_id(r) for r in baseline_activity.rows},
                }
                while deadline_mono is None or time.perf_counter() < deadline_mono:
                    loop_enter = loop.time()
                    loop_gap_ms = max(0.0, (loop_enter - last_loop_enter) * 1000.0)
                    max_loop_gap_ms = max(max_loop_gap_ms, loop_gap_ms)
                    last_loop_enter = loop_enter
                    for task in long_tasks:
                        if task.done():
                            exc = task.exception()
                            if exc is not None:
                                raise exc
                            raise RuntimeError("external live collector stopped unexpectedly")
                    await pool.check_tasks()
                    await pool.prune()
                    now_mono = loop.time()
                    now_us = execution_clock()

                    if now_mono >= next_storage_health:
                        latest_storage = market_store.capture_storage_health(root=root, observed_at_us=now_us)
                        next_storage_health = now_mono + 60.0
                        if latest_storage.warning_level == "CRITICAL_LOW_DISK":
                            raise RuntimeError(
                                f"LOW_DISK_SPACE_GUARD: only {latest_storage.free_bytes / (1024**3):.2f} GiB free"
                            )
                        if latest_storage.warning_level != "OK":
                            growth = latest_storage.db_growth_bytes_per_hour
                            growth_text = "warming" if growth is None else f"{growth / (1024**2):.1f} MiB/h"
                            print(
                                f"  storage warn     {latest_storage.warning_level} "
                                f"db={latest_storage.db_bytes/(1024**2):.1f}MiB "
                                f"growth={growth_text} free={latest_storage.free_bytes/(1024**3):.1f}GiB"
                            )

                    if fee_task is not None and fee_task.done():
                        try:
                            fee_slug, fee_rate, fee_exponent, fee_payload = fee_task.result()
                        except Exception as exc:
                            print(f"  fee metadata warn {type(exc).__name__}: {exc}; using configured fallback")
                        else:
                            engine.set_market_fee(
                                slug=fee_slug,
                                rate=fee_rate,
                                exponent=fee_exponent,
                                source="CLOB_MARKET_INFO_FD",
                            )
                            market_store.persist_fee_schedule(
                                slug=fee_slug,
                                observed_at_us=now_us,
                                rate=str(fee_rate),
                                exponent=fee_exponent,
                                source="CLOB_MARKET_INFO_FD",
                                raw_payload=fee_payload,
                            )
                            print(f"  fee schedule      {fee_slug} rate={fee_rate} exponent={fee_exponent}")
                        fee_task = None

                    # If a market rolled while the previous market's fee request was still
                    # in flight, schedule the new market as soon as the old request clears.
                    # Attempt once per slug and fail closed to the configured fallback rather
                    # than retrying on every hot-loop iteration.
                    fee_market, _ = _active_pool_market(pool, asset="BTC", now_us=now_us)
                    if (
                        fee_task is None
                        and fee_market is not None
                        and fee_market.slug not in fee_attempted_slugs
                        and fee_market.slug not in engine.market_fee_rates
                    ):
                        fee_attempted_slugs.add(fee_market.slug)
                        fee_task = asyncio.create_task(_bounded_market_fee_info(rest, fee_market))

                    # Market discovery is deliberately backgrounded.  In 5.7.2 this
                    # awaited up to seven sequential Gamma requests directly in the hot
                    # loop; a slow API response could freeze telemetry/decisions for tens
                    # of seconds.  At most one bounded discovery request is in flight.
                    if discovery_task is not None and discovery_task.done():
                        try:
                            discovered = discovery_task.result()
                        except (asyncio.TimeoutError, httpx.HTTPError, Exception) as exc:
                            # Discovery failure is non-fatal while the current market is
                            # still valid.  Never let it stall telemetry or strategy clocks.
                            discovery_failures += 1
                            if discovery_failures <= 3 or discovery_failures % 10 == 0:
                                print(f"  discovery warn   {type(exc).__name__}: {exc}")
                        else:
                            discovery_failures = 0
                            current, _ = pool.current("BTC")
                            # The discovery helper can intentionally return the most recent
                            # expired event when the new event has not appeared in the public
                            # API yet.  Do not promote that stale result into the live engine.
                            if discovered.round_end_us > now_us:
                                if current is None or discovered.slug != current.slug:
                                    print(f"\nBTC ROLLOVER {current.slug if current else '-'} -> {discovered.slug}")
                                    await pool.ensure_current(discovered)
                                    if fee_task is None and discovered.slug not in fee_attempted_slugs:
                                        fee_attempted_slugs.add(discovered.slug)
                                        fee_task = asyncio.create_task(_bounded_market_fee_info(rest, discovered))
                                last_market = discovered
                                PriceReferenceMapper(read_store).map_existing_exact(discovered, now_us)
                        discovery_task = None
                        next_discovery = now_mono + config.discovery_ms / 1000.0

                    if discovery_task is None and now_mono >= next_discovery:
                        discovery_task = asyncio.create_task(
                            _bounded_btc_discovery(rest, timeout_seconds=config.discovery_timeout_seconds)
                        )

                    if now_mono >= next_telemetry:
                        market, collector = _active_pool_market(pool, asset="BTC", now_us=now_us)
                        sample_started = time.perf_counter()
                        latest_sample = _sample_from_state(
                            session_id=session_id,
                            wallet=wallet,
                            market=market,
                            book_collector=collector,
                            external_store=read_store,
                            model_paths=config.model_paths,
                        )
                        execution_clock()
                        latest_sample=replace(latest_sample,lab_json=json.dumps({**json.loads(latest_sample.lab_json),
                            'maker':{'policy':config.s8_research_policy,'cancellations':dict(engine.maker_cancellations),
                                'total_cancellations':sum(engine.maker_cancellations.values()),'scope':'COMMITTED_SESSION_S8_ORDERS'}},sort_keys=True))
                        forensic_store.save_live_sample(latest_sample)
                        last_sample_compute_ms = (time.perf_counter() - sample_started) * 1000.0
                        # Schedule from completion, not the stale pre-compute clock.  If a
                        # slow operation ever occurs, this prevents a catch-up storm that
                        # immediately launches another expensive sample and starves feeds.
                        next_telemetry = loop.time() + config.telemetry_ms / 1000.0
                        if last_sample_compute_ms > 500.0:
                            print(f"  hotloop warn     telemetry compute {last_sample_compute_ms:.1f}ms")

                    # Strategy 8 quotes are independent of target-wallet signals.
                    # Expired rounds are masked even if discovery is briefly stale.
                    market, collector = _active_pool_market(pool, asset="BTC", now_us=now_us)
                    if market is not None:
                        engine.refresh_strategy8(market=market, collector=collector, sample=latest_sample, now_us=now_us)
                    maker_fills = engine.process_market_trades(market_store, collector_pool=pool)
                    if maker_fills:
                        print(f"  S8 maker fills    +{maker_fills}")

                    # Phase 5.7: strategies 1-7 BASE + wallet-informed variants + control.
                    if tournament is not None and market is not None:
                        tournament.observe_book(collector=collector, now_us=now_us)
                        tournament_decisions = tournament.evaluate_due(market=market, collector=collector, now_us=now_us)
                        if tournament_decisions.get("SCHEDULED", 0):
                            print(f"  tournament       scheduled +{tournament_decisions['SCHEDULED']}")
                        tournament_exec = tournament.execute_due(collector_pool=pool, market_store=market_store, now_us=now_us)
                        if tournament_exec:
                            compact = ", ".join(f"{k}={v}" for k, v in sorted(tournament_exec.items()))
                            print(f"  tournament exec  {compact}")

                    # Wallet attribution polling is also non-blocking.  One request per
                    # source may be in flight at a time; effective cadence is therefore
                    # max(configured interval, observed RTT) without freezing the engine.
                    completed_batches: list[PollBatch] = []
                    for source in ("trades", "activity"):
                        task = poll_tasks[source]
                        if task is not None and task.done():
                            try:
                                completed_batches.append(task.result())
                            finally:
                                poll_tasks[source] = None

                    if now_mono >= next_trades_poll and poll_tasks["trades"] is None:
                        start_s = max(1, int(time.time()) - config.recent_window_seconds)
                        poll_tasks["trades"] = asyncio.create_task(
                            poller.poll(source="trades", wallet=wallet, start_epoch_s=start_s)
                        )
                        next_trades_poll = now_mono + config.trades_poll_ms / 1000.0

                    if now_mono >= next_activity_poll and poll_tasks["activity"] is None:
                        start_s = max(1, int(time.time()) - config.recent_window_seconds)
                        poll_tasks["activity"] = asyncio.create_task(
                            poller.poll(source="activity", wallet=wallet, start_epoch_s=start_s)
                        )
                        next_activity_poll = now_mono + config.activity_poll_ms / 1000.0

                    if completed_batches:
                        batches = sorted(completed_batches, key=lambda b: b.response_received_us)
                        for batch in batches:
                            stats = source_stats[batch.source]
                            stats["polls"] += 1
                            stats["rtts_us"].append(batch.rtt_us)
                            if batch.status_code in (200, 429, 503):
                                stats[str(batch.status_code)] += 1
                            for row in sorted(batch.rows, key=lambda r: ((r.timestamp_us or 0), _row_id(r))):
                                execution_clock()
                                rid = _row_id(row)
                                if rid in seen_source_rows[batch.source]:
                                    continue
                                seen_source_rows[batch.source].add(rid)
                                if not _row_is_trade(row) or not _row_is_btc(row):
                                    continue
                                if row.timestamp_us is not None and row.timestamp_us < started_us - 2 * MICROS:
                                    continue
                                key = economic_key(row)
                                forward_store.save_source_observation(
                                    session_id=session_id,
                                    economic_key=key,
                                    source=batch.source,
                                    source_row_id=rid,
                                    seen_at_us=batch.response_received_us,
                                    request_started_us=batch.request_started_us,
                                    rtt_us=batch.rtt_us,
                                    status_code=batch.status_code,
                                    raw_json=row.raw_json,
                                )
                                if key in seen_economic:
                                    continue
                                seen_economic.add(key)
                                seq_signal += 1
                                lower_lag, upper_lag = publication_lag_bounds_us(row.timestamp_us, batch.response_received_us)
                                market_trade_id, clob_seen_us = _clob_tx_clock(market_store, row.transaction_hash, _row_slug(row))
                                if clob_seen_us is not None and clob_seen_us <= batch.response_received_us:
                                    source_stats["clob_pre_attribution"] += 1
                                detection_sample = _wallet_detection_sample(
                                    row=row, pool=pool, observed_at_us=batch.response_received_us,
                                    session_id=session_id,
                                    wallet=wallet,
                                    external_store=read_store,
                                    model_paths=config.model_paths,
                                )
                                execution_clock()
                                forensic_store.save_live_sample(detection_sample)
                                signal_id = f"sig_{session_id}_{seq_signal:08d}"
                                saved = forward_store.save_signal(
                                    signal_id=signal_id,
                                    session_id=session_id,
                                    economic_key=key,
                                    source=batch.source,
                                    source_first_seen_us=batch.response_received_us,
                                    trade_ts_us=row.timestamp_us,
                                    slug=_row_slug(row),
                                    token_id=row.token_id,
                                    side=(row.side or "").upper() or None,
                                    outcome=(row.outcome or "").upper() or None,
                                    target_price=_fmt_decimal(row.price),
                                    target_size=_fmt_decimal(row.size),
                                    transaction_hash=row.transaction_hash,
                                    publication_lag_lower_us=lower_lag,
                                    publication_lag_upper_us=upper_lag,
                                    detection_sample_id=detection_sample.sample_id,
                                    raw_json=row.raw_json,
                                )
                                if not saved:
                                    continue
                                signal_source[key] = (batch.source, batch.response_received_us)
                                source_stats["first_source"][batch.source] += 1
                                phase590_decisions = engine.schedule_phase590_wallet_variants(
                                    signal_id=signal_id,
                                    row=row,
                                    first_seen_us=batch.response_received_us,
                                    lower_lag_us=lower_lag,
                                    market_store=market_store,
                                    detection_p_up_mc=detection_sample.p_up_mc,
                                )
                                phase590_decisions[STRATEGY_9_EDGE_BOOK_V1] = engine.schedule_phase593_edge(
                                    signal_id=signal_id,
                                    row=row,
                                    first_seen_us=batch.response_received_us,
                                    lower_lag_us=lower_lag,
                                    detection_p_up_mc=detection_sample.p_up_mc,
                                )
                                phase590_decisions[STRATEGY_9_EDGE_BOOK_V2] = engine.schedule_phase594_edge_v2(
                                    signal_id=signal_id,
                                    row=row,
                                    first_seen_us=batch.response_received_us,
                                    lower_lag_us=lower_lag,
                                    detection_p_up_mc=detection_sample.p_up_mc,
                                )
                                clob_lead_ms = None if clob_seen_us is None else (batch.response_received_us - clob_seen_us) / 1000.0
                                print("\nTARGET BTC WALLET SIGNAL")
                                print(f"  source first     {batch.source}")
                                print(f"  reported UTC     {epoch_us_to_iso(row.timestamp_us)}")
                                print(f"  first seen UTC   {epoch_us_to_iso(batch.response_received_us)}")
                                print(f"  lag lower/upper  {('-' if lower_lag is None else f'{lower_lag/1000:.1f}ms')} / {('-' if upper_lag is None else f'{upper_lag/1000:.1f}ms')}")
                                print(f"  target           {(row.side or '-').upper()} {(row.outcome or '-').upper()} price={row.price} size={row.size}")
                                print(f"  tx               {row.transaction_hash or '-'}")
                                print(f"  CLOB tx clock    {('-' if clob_lead_ms is None else f'{clob_lead_ms:.1f}ms before attribution')}")
                                print(f"  S9 scheduled      {sum(1 for v in phase590_decisions.values() if v == 'SCHEDULED')}/{len(PHASE590_WALLET_STRATEGIES) + 2}")
                                key_views = (
                                    STRATEGY_9A,
                                    STRATEGY_9_BOOK_CONFIRM_75,
                                    STRATEGY_9_BOOK_MODEL30,
                                    STRATEGY_9_FIRST3_LARGE_DET7_5,
                                    STRATEGY_9_ROUTER_A,
                                    STRATEGY_9_EDGE_BOOK_V1,
                                    STRATEGY_9_EDGE_BOOK_V2,
                                )
                                print("  key arms          " + " | ".join(f"{k.replace('S9_', '')}={phase590_decisions.get(k, '-')}" for k in key_views))

                    execution_stats = engine.execute_due_copies(collector_pool=pool, market_store=market_store, now_us=now_us)
                    for key, count in execution_stats.items():
                        print(f"  copy execution   {key} x{count}")

                    for strategy, slug, resolved, pnl in engine.settle_ready(external_store=read_store, now_us=now_us):
                        print(f"  SETTLED          {strategy} {slug} {resolved} pnl={pnl:+.4f}")

                    if now_mono - last_status >= 10.0:
                        engine.prune_memory(now_us=now_us)
                        if tournament is not None:tournament.prune_memory(now_us=now_us)
                        if (len(seen_economic)>200000 or any(len(values)>200000 for values in seen_source_rows.values())
                            or len(pool.completed_diagnostics)>10000):raise RuntimeError('PAPER_RESOURCE_AUDIT_LIMIT')
                        def med_ms(source: str) -> float:
                            vals = source_stats[source]["rtts_us"]
                            return statistics.median(vals) / 1000.0 if vals else float("nan")
                        s8pos = engine.positions.get((STRATEGY_8, market.slug)) if market is not None else None
                        pair_text = "-" if s8pos is None or s8pos.avg_pair_cost is None else f"{s8pos.avg_pair_cost:.3f}"
                        print(
                            f"  health signals={seq_signal} tradesRecentRTT50={med_ms('trades'):.1f}ms activityRecentRTT50={med_ms('activity'):.1f}ms "
                            f"first(trades/activity)={source_stats['first_source']['trades']}/{source_stats['first_source']['activity']} "
                            f"S8open={len(engine.maker_orders)} S8pair={pair_text} pendingCopies={len(engine.pending_copies)} "
                            f"discovery={'BUSY' if discovery_task is not None else 'IDLE'} "
                            f"sampleMs={last_sample_compute_ms:.1f} loopGapMax={max_loop_gap_ms:.1f}ms"
                            + (" pendingTournament=%d" % len(tournament.pending) if tournament is not None else "")
                            + (
                                " storageDB=%.1fMiB free=%.1fGiB growth=%s" % (
                                    latest_storage.db_bytes / (1024**2),
                                    latest_storage.free_bytes / (1024**3),
                                    (
                                        "warming"
                                        if latest_storage.db_growth_bytes_per_hour is None
                                        else f"{latest_storage.db_growth_bytes_per_hour/(1024**2):.1f}MiB/h"
                                    ),
                                )
                                if latest_storage is not None else ""
                            )
                        )
                        max_loop_gap_ms = 0.0
                        last_status = now_mono

                    await asyncio.sleep(0.003)
            finally:
                # Capture the original exit reason before cleanup can obscure it.
                terminal_state = _termination_state(sys.exc_info()[1])
                background = [t for t in [discovery_task, fee_task, *poll_tasks.values()] if t is not None]
                for task in background:
                    task.cancel()
                if background:
                    await asyncio.gather(*background, return_exceptions=True)
                await pool.stop()
                await asyncio.gather(coinbase.request_stop(), kraken.request_stop(), twap.request_stop(), return_exceptions=True)
                sync_monitor.cancel()
                await asyncio.gather(*long_tasks, return_exceptions=True)
                ended_us = now_epoch_us()
                if not clock_guard.failed:
                    for strategy, slug, resolved, pnl in engine.settle_ready(external_store=read_store, now_us=ended_us):
                        print(f"  SETTLED          {strategy} {slug} {resolved} pnl={pnl:+.4f}")
                else:
                    market_store.record_gap(component="runtime_clock", slug=None,
                        detected_at_us=ended_us, reason="CLOCK_HEALTH_LOST",
                        details={"forward_session_id": session_id, **clock_guard.details()})
                # Strip long RTT arrays from persisted source stats, retaining summary quantiles.
                persisted_stats: dict[str, Any] = {"first_source": source_stats["first_source"], "clob_pre_attribution": source_stats["clob_pre_attribution"]}
                for source in ("trades", "activity"):
                    vals = list(source_stats[source]["rtts_us"])
                    persisted_stats[source] = {
                        k: v for k, v in source_stats[source].items() if k != "rtts_us"
                    }
                    persisted_stats[source]["rtt_p50_ms"] = None if not vals else statistics.median(vals) / 1000.0
                    persisted_stats[source]['rtt_scope']='LATEST_4096_RESPONSES'
                    persisted_stats[source]['rtt_sample_count']=len(vals)
                    if vals:
                        ordered = sorted(vals)
                        persisted_stats[source]["rtt_p90_ms"] = ordered[min(len(ordered)-1, int(math.ceil(0.90*len(ordered))-1))] / 1000.0
                    else:
                        persisted_stats[source]["rtt_p90_ms"] = None
                try:
                    latest_storage = market_store.capture_storage_health(root=root, observed_at_us=ended_us)
                except Exception as exc:
                    print(f"  storage final warn {type(exc).__name__}: {exc}")
                summary = forward_store.session_summary(session_id)
                summary['accounting_policy_revision'] = ACCOUNTING_POLICY_REVISION
                summary['model_policy_revision'] = MODEL_POLICY_REVISION
                summary['settlement_policy_revision'] = RESOLUTION_POLICY_REVISION
                summary["feed_policy_revision"] = FEED_POLICY_REVISION
                summary['execution_admission_policy_revision']=EXECUTION_ADMISSION_REVISION
                summary['resource_policy_revision']='offline_durable_retention_v1'
                summary["clob_collectors"] = pool.completed_diagnostics
                summary["runtime_clock"] = clock_guard.details()
                forward_store.finish_session(
                    session_id=session_id,
                    ended_at_us=ended_us,
                    state=terminal_state,
                    source_stats=persisted_stats,
                    summary=summary,
                )
                forensic_store.finish_live_session(
                    session_id=session_id,
                    ended_at_us=ended_us,
                    target_event_count=seq_signal,
                    state=f"PHASE5_9_2_WALLET_ALPHA_{terminal_state}",
                )

            print("\nPHASE 5.9.2 EVIDENCE-ROUTED WALLET ALPHA FORWARD SESSION COMPLETE")
            summary = forward_store.session_summary(session_id)
            print(f"  target signals    {summary['signals']}")
            for strategy, row in summary.get("strategies", {}).items():
                print(
                    f"  {strategy:24s} orders={row['orders']} fills={row['fills']} rejects={row['rejected_orders']} "
                    f"notional=${row['notional']:.2f} settled_rounds={row['settled_rounds']} pnl=${row['settled_pnl']:+.2f}"
                )
            print("  real trading      DISABLED")
            return 0


def verify_forward_lab_config() -> dict[str, bool]:
    cfg = ForwardLabConfig()
    cfg.validate()
    dummy_up = Decimal("0.35")
    dummy_down = Decimal("0.55")
    checks = {
        "three forward strategies named": len({STRATEGY_8, STRATEGY_9A, STRATEGY_9B}) == 3,
        "fixed 10-cent ladder offset by 5 cents": tuple(round(cfg.grid_prices[i+1]-cfg.grid_prices[i], 8) for i in range(len(cfg.grid_prices)-1)) == (0.1,)*9,
        "observed 48-share clip default": cfg.grid_clip_shares == 48.0,
        "paired example leaves positive edge": Decimal("1") - dummy_up - dummy_down > 0,
        "literal copy has finite risk cap": cfg.copy_max_order_notional > 0,
        "protected copy has deterioration cap": 0 < cfg.protected_max_deterioration < 0.5,
        "dual public attribution paths enabled": cfg.trades_poll_ms > 0 and cfg.activity_poll_ms > 0,
        "real trading absent from module": True,
    }
    return checks


def verify_phase590_config() -> dict[str, bool]:
    cfg = ForwardLabConfig(enable_full_tournament=True)
    cfg.validate()
    wallet_names = set(PHASE590_WALLET_STRATEGIES)
    return {
        "24 frozen wallet-copy arms": len(PHASE590_WALLET_STRATEGIES) == 24 and len(wallet_names) == 24,
        "S9A literal control retained": STRATEGY_9A in wallet_names,
        "S9B protected control retained": STRATEGY_9B in wallet_names,
        "first-signal hypotheses registered": {STRATEGY_9_FIRST2, STRATEGY_9_FIRST3, STRATEGY_9_ONE_SIDED}.issubset(wallet_names),
        "early-large threshold frozen": cfg.early_min_seconds_remaining == 180.0 and cfg.large_signal_min_shares == 24.0,
        "deterioration arms frozen": {STRATEGY_9_DET3, STRATEGY_9_DET4, STRATEGY_9_DET5, STRATEGY_9_DET7_5}.issubset(wallet_names),
        "book-confirm timing frozen": cfg.book_confirm_delay_ms == 75 and cfg.book_confirm_execution_delay_ms == 25,
        "book-confirm threshold frozen": cfg.book_confirm_min_direction_score == 0.20,
        "book-model probability floors frozen": (
            cfg.book_model30_min_target_probability == 0.30
            and cfg.book_model40_min_target_probability == 0.40
        ),
        "router specialist deterioration frozen": cfg.router_specialist_max_deterioration == 0.075,
        "router model veto frozen": cfg.router_model_min_target_probability == 0.30,
        "Phase 5.9.2 challengers registered": {
            STRATEGY_9_FIRST3_LARGE_DET7_5,
            STRATEGY_9_LARGE_DET7_5,
            STRATEGY_9_BOOK_MODEL30,
            STRATEGY_9_BOOK_MODEL40,
            STRATEGY_9_ROUTER_A,
        }.issubset(wallet_names),
        "dynamic fee fallback remains conservative": cfg.taker_fee_rate == 0.07,
        "general fee exponent supported": crypto_taker_fee(
            shares=Decimal("100"), price=Decimal("0.5"), rate=Decimal("0.25"), exponent=2
        ) == Decimal("1.56250"),
        "S8 maker fill requires strictly later aggressor receipt": True,
        "Phase 5.9.2 keeps compact market storage profile": STORAGE_PROFILE == "compact_raw_ref_v1",
        "real trading remains absent": True,
    }
