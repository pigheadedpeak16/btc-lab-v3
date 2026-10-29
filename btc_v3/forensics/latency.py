from __future__ import annotations

import asyncio
import json
import statistics
import time
import uuid
from dataclasses import asdict, dataclass
from decimal import Decimal
from datetime import datetime,timezone
from pathlib import Path
from typing import Any

import httpx

from ..feeds.coinbase import CoinbaseCollector
from ..feeds.kraken import KrakenCollector
from ..feeds.polymarket import DiscoveredMarket, PolymarketPublicRest
from ..feeds.polymarket_collector import PolymarketBookCollector
from ..feeds.settlement_reference import AuthenticatedTwapCollector, PriceReferenceMapper
from ..storage.external_store import ExternalStore
from ..storage.market_store import MarketStore
from ..timeutil import epoch_us_to_iso, now_epoch_us
from .client import DATA_API_BASE, PolymarketDataClient, _loads_decimal
from .live import MarketCollectorManager, _sample_from_state
from .models import WalletTrade
from .store import ForensicsStore

MICROS = 1_000_000
TARGET_THRESHOLD_MS = 200.0


@dataclass(frozen=True)
class LatencyLabConfig:
    poll_ms: int = 100
    telemetry_ms: int = 1000
    discovery_ms: int = 1000
    recent_window_seconds: int = 20
    model_paths: int = 500
    correlation_window_ms: int = 2500
    nearby_window_ms: int = 5000
    rollover_grace_seconds: float = 15.0
    min_verdict_samples: int = 20

    def validate(self) -> None:
        if self.poll_ms < 50:
            raise ValueError("poll_ms must be >= 50")
        if self.telemetry_ms < 250:
            raise ValueError("telemetry_ms must be >= 250")
        if self.discovery_ms < 500:
            raise ValueError("discovery_ms must be >= 500")
        if self.recent_window_seconds < 5:
            raise ValueError("recent_window_seconds must be >= 5")
        if self.model_paths < 100:
            raise ValueError("model_paths must be >= 100")
        if self.correlation_window_ms < 250:
            raise ValueError("correlation_window_ms must be >= 250")
        if self.nearby_window_ms < self.correlation_window_ms:
            raise ValueError("nearby_window_ms must be >= correlation_window_ms")
        if self.rollover_grace_seconds < 2:
            raise ValueError("rollover_grace_seconds must be >= 2")
        if self.min_verdict_samples < 5:
            raise ValueError("min_verdict_samples must be >= 5")


@dataclass(frozen=True)
class PollObservation:
    poll_id: str
    session_id: str
    seq: int
    request_started_us: int
    response_received_us: int
    rtt_us: int
    status_code: int | None
    retry_after_ms: int | None
    trace_id: str | None
    row_count: int
    newest_trade_ts_us: int | None
    error_text: str | None


@dataclass(frozen=True)
class MarketTradeCandidate:
    market_trade_id: int
    slug: str
    token_id: str | None
    source_trade_id: str | None
    source_ts_us: int | None
    received_at_us: int
    price: str
    size: str
    taker_side: str | None


@dataclass(frozen=True)
class CorrelationResult:
    quality: str
    candidate: MarketTradeCandidate | None
    candidate_count: int


@dataclass(frozen=True)
class LatencyEvent:
    latency_event_id: str
    session_id: str
    wallet: str
    trade_id: str
    trade_ts_us: int | None
    api_first_seen_us: int
    api_request_started_us: int
    api_rtt_us: int
    slug: str | None
    token_id: str | None
    side: str | None
    outcome: str | None
    price: str | None
    size: str | None
    transaction_hash: str | None
    market_trade_id: int | None
    market_received_at_us: int | None
    market_source_ts_us: int | None
    market_to_wallet_us: int | None
    correlation_quality: str
    correlation_candidate_count: int
    detection_sample_id: str | None
    current_executable_price: str | None
    copy_price_delta: str | None
    raw_json: str


@dataclass(frozen=True)
class LatencyVerdict:
    sample_count: int
    p50_ms: float | None
    p90_ms: float | None
    p95_ms: float | None
    under_200ms_fraction: float | None
    verdict: str


def latency_verdict(values_us: list[int], *, min_samples: int = 20) -> LatencyVerdict:
    clean = sorted(v for v in values_us if v >= 0)
    if not clean:
        return LatencyVerdict(0, None, None, None, None, "NO_CORRELATED_EVENTS")

    def percentile(q: float) -> float:
        if len(clean) == 1:
            return clean[0] / 1000.0
        pos = (len(clean) - 1) * q
        lo = int(pos)
        hi = min(lo + 1, len(clean) - 1)
        frac = pos - lo
        return (clean[lo] + (clean[hi] - clean[lo]) * frac) / 1000.0

    p50 = percentile(0.50)
    p90 = percentile(0.90)
    p95 = percentile(0.95)
    under = sum(v <= int(TARGET_THRESHOLD_MS * 1000) for v in clean) / len(clean)
    if len(clean) < min_samples:
        verdict = "INSUFFICIENT_SAMPLES"
    elif p90 <= TARGET_THRESHOLD_MS:
        verdict = "CONSISTENT_SUB_200MS"
    elif p50 <= TARGET_THRESHOLD_MS:
        verdict = "OCCASIONAL_SUB_200MS"
    else:
        verdict = "NOT_SUB_200MS"
    return LatencyVerdict(len(clean), p50, p90, p95, under, verdict)


def _decimal_text(value: Decimal | None) -> str | None:
    return None if value is None else format(value, "f")


def correlate_market_trade(
    trade: WalletTrade,
    candidates: list[MarketTradeCandidate],
    *,
    window_us: int,
) -> CorrelationResult:
    if not candidates:
        return CorrelationResult("NO_MATCH", None, 0)
    tx = (trade.transaction_hash or "").lower()
    if tx:
        exact_tx = [row for row in candidates if row.source_trade_id and row.source_trade_id.lower() == tx]
        if len(exact_tx) == 1:
            return CorrelationResult("EXACT_TX", exact_tx[0], 1)
        if len(exact_tx) > 1:
            narrowed = [row for row in exact_tx if (trade.token_id is None or row.token_id == trade.token_id) and (trade.price is None or Decimal(row.price) == trade.price)]
            if len(narrowed) == 1:
                return CorrelationResult("EXACT_TX_SIGNATURE", narrowed[0], len(exact_tx))
            return CorrelationResult("AMBIGUOUS_TX", None, len(exact_tx))
    price_time: list[MarketTradeCandidate] = []
    for row in candidates:
        if trade.slug and row.slug != trade.slug:
            continue
        if trade.token_id and row.token_id != trade.token_id:
            continue
        if trade.price is not None and Decimal(row.price) != trade.price:
            continue
        if trade.timestamp_us is not None and row.source_ts_us is not None and abs(row.source_ts_us - trade.timestamp_us) > window_us:
            continue
        price_time.append(row)
    if len(price_time) == 1:
        row = price_time[0]
        if trade.size is not None and Decimal(row.size) == trade.size:
            return CorrelationResult("UNIQUE_SIGNATURE", row, 1)
        return CorrelationResult("UNIQUE_PRICE_TIME", row, 1)
    if len(price_time) > 1:
        exact_size = [row for row in price_time if trade.size is not None and Decimal(row.size) == trade.size]
        if len(exact_size) == 1:
            return CorrelationResult("UNIQUE_SIGNATURE", exact_size[0], len(price_time))
        if len(exact_size) > 1:
            return CorrelationResult("AMBIGUOUS_SIGNATURE", None, len(price_time))
        return CorrelationResult("AMBIGUOUS_PRICE_TIME", None, len(price_time))
    return CorrelationResult("NO_MATCH", None, 0)


def _market_candidates(
    market_store: MarketStore,
    trade: WalletTrade,
    *,
    window_us: int,
) -> list[MarketTradeCandidate]:
    if not trade.slug:
        return []
    clauses = ["slug=?"]
    params: list[Any] = [trade.slug]
    if trade.token_id:
        clauses.append("token_id=?")
        params.append(trade.token_id)
    if trade.timestamp_us is not None:
        clauses.append("(source_ts_us IS NULL OR source_ts_us BETWEEN ? AND ?)")
        params.extend([trade.timestamp_us - window_us, trade.timestamp_us + window_us])
    rows = market_store.conn.execute(
        f"""
        SELECT id,slug,token_id,source_trade_id,source_ts_us,received_at_us,price,size,taker_side
        FROM market_trades
        WHERE {' AND '.join(clauses)}
        ORDER BY received_at_us DESC
        LIMIT 200
        """,
        params,
    ).fetchall()
    return [MarketTradeCandidate(*row) for row in rows]


def _nearby_market_candidates(market_store: MarketStore, trade: WalletTrade, *, window_us: int) -> list[MarketTradeCandidate]:
    if not trade.slug:
        return []
    params: list[Any] = [trade.slug]
    clauses = ["slug=?"]
    if trade.token_id:
        clauses.append("token_id=?")
        params.append(trade.token_id)
    if trade.timestamp_us is not None:
        clauses.append("(source_ts_us IS NULL OR source_ts_us BETWEEN ? AND ?)")
        params.extend([trade.timestamp_us - window_us, trade.timestamp_us + window_us])
    rows = market_store.conn.execute(
        f"""SELECT id,slug,token_id,source_trade_id,source_ts_us,received_at_us,price,size,taker_side
        FROM market_trades WHERE {' AND '.join(clauses)}
        ORDER BY ABS(COALESCE(source_ts_us, received_at_us) - ?) ASC, received_at_us ASC LIMIT 12""",
        [*params, trade.timestamp_us or 0],
    ).fetchall()
    return [MarketTradeCandidate(*row) for row in rows]


def _shadow_quote(trade: WalletTrade, collector: Any) -> tuple[str | None, str | None]:
    if collector is None or not trade.slug or collector.market.slug != trade.slug:
        return None, None
    wanted = (trade.outcome or "").upper()
    state = next((s for s in collector.states.values() if s.outcome.upper() == wanted), None)
    if state is None or not state.synchronized:
        return None, None
    side = (trade.side or "").upper()
    level = state.best_ask if side == "BUY" else state.best_bid if side == "SELL" else None
    if level is None:
        return None, None
    executable = Decimal(str(level.price))
    delta = None
    if trade.price is not None:
        delta = executable - trade.price if side == "BUY" else trade.price - executable
    return format(executable, "f"), None if delta is None else format(delta, "f")


@dataclass
class _CollectorSlot:
    market: DiscoveredMarket
    collector: PolymarketBookCollector
    task: asyncio.Task
    retire_after_mono: float | None = None


class OverlapCollectorPool:
    def __init__(self, *, root: Path, rest: PolymarketPublicRest, store: MarketStore, grace_seconds: float,
                 forward_session_id: str | None = None) -> None:
        self.root = root
        self.rest = rest
        self.store = store
        self.grace_seconds = grace_seconds
        self.forward_session_id = forward_session_id
        self.completed_diagnostics: list[dict[str, Any]] = []
        self.slots: dict[str, _CollectorSlot] = {}
        self.current_by_asset: dict[str, str] = {}
        # Retired collectors are stopped in background tasks.  Waiting for an old
        # collector to close inside the live strategy loop can block every other
        # asyncio task (Coinbase/Kraken/TWAP, telemetry, decision clocks) when that
        # collector happens to be inside a slow REST resync.
        self._retire_tasks: dict[str, asyncio.Task[None]] = {}
        self._prepare_task: asyncio.Task | None = None
        self._next_prepare_mono = 0.0
        self.preparation_state = 'NOT_ATTEMPTED'

    async def _prepare_visible_next(self, current: DiscoveredMarket) -> None:
        try:
            market=await asyncio.wait_for(self.rest.discover_btc_5m(
                now=datetime.fromtimestamp(current.round_end_us/1e6,timezone.utc),candidate_offsets=(0,)),timeout=3)
            await self.prepare_upcoming(market,current=current,at_us=now_epoch_us())
            self.preparation_state='VERIFIED_UPCOMING_SUBSCRIBED'
        except (Exception,):
            # Public next-round discovery is optional; fixed codes avoid SDK/body disclosure.
            self.preparation_state='UPCOMING_NOT_VERIFIABLE'

    @staticmethod
    def _asset(market: DiscoveredMarket) -> str:
        return "ETH" if market.slug.lower().startswith("eth-") else "BTC"

    async def ensure_current(self, market: DiscoveredMarket) -> None:
        asset = self._asset(market)
        old_slug = self.current_by_asset.get(asset)
        if market.slug not in self.slots:
            self.store.persist_market(market, now_epoch_us())
            collector = PolymarketBookCollector(root=self.root, market=market, store=self.store, rest=self.rest,
                                               forward_session_id=self.forward_session_id)
            task = asyncio.create_task(collector.run())
            self.slots[market.slug] = _CollectorSlot(market, collector, task)
            await asyncio.sleep(0)
        self.current_by_asset[asset] = market.slug
        if old_slug and old_slug != market.slug and old_slug in self.slots:
            self.slots[old_slug].retire_after_mono = asyncio.get_running_loop().time() + self.grace_seconds

    async def prepare_upcoming(self, market: DiscoveredMarket, *, current: DiscoveredMarket, at_us: int) -> None:
        """Subscribe to a verified next round without promoting it into execution.

        Only one adjacent upcoming market per asset is admitted. The current
        pointer changes solely through ensure_current after live discovery.
        """
        if (self._asset(market)!=self._asset(current) or market.closed or not market.active
            or market.round_start_us!=current.round_end_us or market.round_end_us-market.round_start_us!=300_000_000
            or not current.round_start_us<=at_us<current.round_end_us or market.up_token_id==market.down_token_id
            or set(market.token_ids)&set(current.token_ids)):
            raise ValueError('UPCOMING_MARKET_IDENTITY_INVALID')
        expected=f'{self._asset(market).lower()}-updown-5m-{market.round_start_us//1_000_000}'
        if market.slug!=expected:raise ValueError('UPCOMING_MARKET_IDENTITY_INVALID')
        if market.slug in self.slots:return
        self.store.persist_market(market,at_us)
        collector=PolymarketBookCollector(root=self.root,market=market,store=self.store,rest=self.rest,
            forward_session_id=self.forward_session_id)
        self.slots[market.slug]=_CollectorSlot(market,collector,asyncio.create_task(collector.run()))
        await asyncio.sleep(0)

    async def _retire_slot(self, slug: str, slot: _CollectorSlot) -> None:
        """Stop an expired collector without blocking the hot live loop.

        Old markets are context-only once the overlap grace period has elapsed.
        If a collector is stuck in a public REST request/resync, bound shutdown and
        cancel it rather than freezing live predictor feeds and strategy clocks.
        """
        try:
            try:
                await asyncio.wait_for(slot.collector.request_stop(), timeout=2.0)
            except (asyncio.TimeoutError, Exception):
                pass
            try:
                await asyncio.wait_for(asyncio.shield(slot.task), timeout=5.0)
            except asyncio.TimeoutError:
                slot.task.cancel()
                await asyncio.gather(slot.task, return_exceptions=True)
        finally:
            if hasattr(slot.collector, "_details"):
                self.completed_diagnostics.append(slot.collector._details())
            self._retire_tasks.pop(slug, None)

    async def prune(self) -> None:
        """Schedule retirement of expired overlap collectors and return promptly."""
        now = asyncio.get_running_loop().time()
        if self._prepare_task is not None and self._prepare_task.done():
            await asyncio.gather(self._prepare_task,return_exceptions=True)
            self._prepare_task=None
        current,_=self.current('BTC')
        utc=now_epoch_us()
        if (current is not None and 0<current.round_end_us-utc<=60_000_000
            and self._prepare_task is None and now>=self._next_prepare_mono
            and not any(s.market.round_start_us==current.round_end_us for s in self.slots.values())):
            self._next_prepare_mono=now+10
            self._prepare_task=asyncio.create_task(self._prepare_visible_next(current))
        for slug, slot in list(self.slots.items()):
            # A prepared round never adopted by discovery must not leak forever.
            if slot.retire_after_mono is None and slug not in self.current_by_asset.values() and slot.market.round_end_us<=now_epoch_us():
                slot.retire_after_mono=now
            if slot.retire_after_mono is None or now < slot.retire_after_mono:
                continue
            # Remove it from the active pool immediately; it is an expired market.
            self.slots.pop(slug, None)
            if slug not in self._retire_tasks:
                self._retire_tasks[slug] = asyncio.create_task(self._retire_slot(slug, slot))

        # Consume completed background exceptions without awaiting in-flight work.
        for slug, task in list(self._retire_tasks.items()):
            if not task.done():
                continue
            try:
                task.result()
            except asyncio.CancelledError:
                pass
            except Exception:
                # Retirement is best-effort for an already-expired market and must
                # never make the current market unhealthy.
                pass
            self._retire_tasks.pop(slug, None)

    def collector_for_slug(self, slug: str | None) -> PolymarketBookCollector | None:
        slot = self.slots.get(slug or "")
        return slot.collector if slot else None

    def current(self, asset: str) -> tuple[DiscoveredMarket | None, PolymarketBookCollector | None]:
        slug = self.current_by_asset.get(asset.upper())
        slot = self.slots.get(slug or "")
        return (slot.market, slot.collector) if slot else (None, None)

    async def check_tasks(self) -> None:
        for slot in self.slots.values():
            if slot.task.done():
                exc = slot.task.exception()
                if exc is not None:
                    raise exc
                raise RuntimeError(f"Polymarket collector stopped unexpectedly for {slot.market.slug}")

    async def stop(self) -> None:
        if self._prepare_task is not None:
            self._prepare_task.cancel()
            await asyncio.gather(self._prepare_task,return_exceptions=True)
            self._prepare_task=None
        slots = list(self.slots.values())
        self.slots.clear()
        retire = list(self._retire_tasks.values())
        self._retire_tasks.clear()
        for slot in slots:
            try:
                await asyncio.wait_for(slot.collector.request_stop(), timeout=2.0)
            except Exception:
                pass
        for slot in slots:
            if not slot.task.done():
                slot.task.cancel()
        for task in retire:
            if not task.done():
                task.cancel()
        await asyncio.gather(*(slot.task for slot in slots), *retire, return_exceptions=True)
        for slot in slots:
            if hasattr(slot.collector, "_details"):
                self.completed_diagnostics.append(slot.collector._details())


class FastWalletPoller:
    """No-retry latency probe. 429/503 responses are measurements, not hidden retries."""

    def __init__(self, *, client: httpx.AsyncClient | None = None, timeout_seconds: float = 3.0) -> None:
        self._owned = client is None
        self.client = client or httpx.AsyncClient(
            timeout=timeout_seconds,
            headers={"Accept": "application/json", "User-Agent": "btc-lab-v3-wallet-latency/0.5.5"},
            follow_redirects=True,
        )

    async def __aenter__(self) -> "FastWalletPoller":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._owned:
            await self.client.aclose()

    async def poll(self, *, session_id: str, seq: int, wallet: str, start_epoch_s: int) -> tuple[PollObservation, list[WalletTrade]]:
        request_started_us = time.time_ns() // 1000
        mono_start = time.perf_counter_ns()
        status: int | None = None
        retry_after_ms: int | None = None
        trace_id: str | None = None
        rows: list[WalletTrade] = []
        error_text: str | None = None
        newest: int | None = None
        try:
            response = await self.client.get(
                f"{DATA_API_BASE}/v2/trades",
                params={
                    "user": wallet,
                    "start": start_epoch_s,
                    "end": 0,
                    "limit": 100,
                    "taker_only": "false",
                },
            )
            status = response.status_code
            trace_id = response.headers.get("x-trace-id")
            retry = response.headers.get("Retry-After")
            if retry:
                try:
                    retry_after_ms = int(float(retry) * 1000)
                except ValueError:
                    retry_after_ms = None
            if status == 200:
                payload = _loads_decimal(response.text)
                raw_rows = payload.get("data") if isinstance(payload, dict) else None
                if isinstance(raw_rows, list):
                    rows = [WalletTrade.from_api(wallet, raw) for raw in raw_rows if isinstance(raw, dict)]
                    timestamps = [row.timestamp_us for row in rows if row.timestamp_us is not None]
                    newest = max(timestamps) if timestamps else None
            else:
                error_text = response.text[:500]
        except httpx.HTTPError as exc:
            error_text = f"{type(exc).__name__}: {exc}"
        response_received_us = time.time_ns() // 1000
        rtt_us = max(0, (time.perf_counter_ns() - mono_start) // 1000)
        observation = PollObservation(
            poll_id=f"poll_{session_id}_{seq}",
            session_id=session_id,
            seq=seq,
            request_started_us=request_started_us,
            response_received_us=response_received_us,
            rtt_us=rtt_us,
            status_code=status,
            retry_after_ms=retry_after_ms,
            trace_id=trace_id,
            row_count=len(rows),
            newest_trade_ts_us=newest,
            error_text=error_text,
        )
        return observation, rows


async def _wait_for_correlation(
    market_store: MarketStore,
    trade: WalletTrade,
    *,
    window_us: int,
    timeout_seconds: float = 2.0,
) -> CorrelationResult:
    deadline = time.perf_counter() + timeout_seconds
    result = CorrelationResult("NO_MATCH", None, 0)
    while time.perf_counter() < deadline:
        candidates = _market_candidates(market_store, trade, window_us=window_us)
        result = correlate_market_trade(trade, candidates, window_us=window_us)
        if result.candidate is not None or result.quality.startswith("AMBIGUOUS"):
            return result
        await asyncio.sleep(0.02)
    return result


async def run_wallet_latency_lab(
    root: Path,
    *,
    requested_wallet: str,
    seconds: float | None,
    config: LatencyLabConfig,
) -> int:
    config.validate()
    session_id = str(uuid.uuid4())
    started_us = time.time_ns() // 1000
    deadline_mono = None if seconds is None else time.perf_counter() + seconds

    async with PolymarketDataClient() as resolver, PolymarketPublicRest() as rest, FastWalletPoller() as poller:
        wallet, _ = await resolver.resolve_proxy_wallet(requested_wallet)
        market = await rest.discover_btc_5m()
        try:
            eth_market = await rest.discover_eth_5m()
        except Exception:
            eth_market = None
        baseline_start_s = max(1, int(started_us / MICROS) - config.recent_window_seconds)
        baseline_obs, baseline_rows = await poller.poll(
            session_id=session_id,
            seq=0,
            wallet=wallet,
            start_epoch_s=baseline_start_s,
        )
        seen = {row.trade_id for row in baseline_rows}

        with (
            MarketStore.open_project(root) as market_store,
            ExternalStore.open_project(root) as cb_store,
            ExternalStore.open_project(root) as kr_store,
            ExternalStore.open_project(root) as twap_store,
            ExternalStore.open_project(root) as read_store,
            ForensicsStore.open_project(root) as forensic_store,
        ):
            forensic_store.start_latency_session(
                session_id=session_id,
                wallet=wallet,
                requested_wallet=requested_wallet,
                started_at_us=started_us,
                config=asdict(config),
            )
            forensic_store.start_live_session(
                session_id=session_id,
                wallet=wallet,
                requested_wallet=requested_wallet,
                started_at_us=started_us,
                config={"latency_lab": True, **asdict(config)},
            )
            forensic_store.save_latency_poll(baseline_obs)

            coinbase = CoinbaseCollector(root=root, store=cb_store)
            kraken = KrakenCollector(root=root, store=kr_store)
            twap = AuthenticatedTwapCollector(root=root, store=twap_store, market=market)
            collector_pool = OverlapCollectorPool(root=root, rest=rest, store=market_store, grace_seconds=config.rollover_grace_seconds)
            await collector_pool.ensure_current(market)
            if eth_market is not None:
                await collector_pool.ensure_current(eth_market)
            long_tasks = [
                asyncio.create_task(coinbase.run()),
                asyncio.create_task(kraken.run()),
                asyncio.create_task(twap.run()),
            ]

            seq = 0
            event_count = 0
            rate_limited = 0
            service_unavailable = 0
            poll_rtts: list[int] = []
            correlated_latencies: list[int] = []
            loop = asyncio.get_running_loop()
            next_poll = loop.time()
            next_telemetry = loop.time()
            next_discovery = loop.time()
            last_status_print = loop.time()
            latest_sample_id: str | None = None

            print("BTC Lab V3 - TARGET WALLET LATENCY LAB")
            print(f"  session          {session_id}")
            print(f"  wallet           {wallet}")
            print(f"  wallet poll      {config.poll_ms} ms requested cadence")
            print(f"  telemetry        {config.telemetry_ms} ms derived snapshots + full raw feed capture")
            print("  market clock     public CLOB last_trade_price receive timestamps")
            print("  crypto markets   BTC 5m + ETH 5m latency capture; BTC-only strategy telemetry")
            print(f"  rollover         {config.rollover_grace_seconds:.0f}s overlapping old/new CLOB collectors")
            print("  wallet clock     first completed Data API response containing the trade")
            print("  target           consistent detection <= 200 ms")
            print("  retries          NONE inside latency poll; 429/503 are recorded as measurements")
            print("  orders           NONE")
            print("  real trading     DISABLED")
            print("  stop             Ctrl+C")

            try:
                while deadline_mono is None or time.perf_counter() < deadline_mono:
                    for task in long_tasks:
                        if task.done():
                            exc = task.exception()
                            if exc is not None:
                                raise exc
                            raise RuntimeError("a live external collector stopped unexpectedly")
                    await collector_pool.check_tasks()
                    await collector_pool.prune()

                    mono = loop.time()
                    if mono >= next_discovery:
                        discovered = await rest.discover_btc_5m()
                        current_btc, _ = collector_pool.current("BTC")
                        if current_btc is None or discovered.slug != current_btc.slug:
                            print(f"  BTC rollover     {current_btc.slug if current_btc else '-'} -> {discovered.slug} (overlap {config.rollover_grace_seconds:.0f}s)")
                            await collector_pool.ensure_current(discovered)
                        try:
                            discovered_eth = await rest.discover_eth_5m()
                            current_eth, _ = collector_pool.current("ETH")
                            if current_eth is None or discovered_eth.slug != current_eth.slug:
                                print(f"  ETH rollover     {current_eth.slug if current_eth else '-'} -> {discovered_eth.slug} (overlap {config.rollover_grace_seconds:.0f}s)")
                                await collector_pool.ensure_current(discovered_eth)
                        except Exception as exc:
                            print(f"[WARN] ETH 5m discovery failed temporarily: {type(exc).__name__}: {exc}")
                        PriceReferenceMapper(read_store).map_existing_exact(discovered, now_epoch_us())
                        next_discovery = mono + config.discovery_ms / 1000.0

                    if mono >= next_telemetry:
                        sample = _sample_from_state(
                            session_id=session_id,
                            wallet=wallet,
                            market=collector_pool.current("BTC")[0],
                            book_collector=collector_pool.current("BTC")[1],
                            external_store=read_store,
                            model_paths=config.model_paths,
                        )
                        forensic_store.save_live_sample(sample)
                        latest_sample_id = sample.sample_id
                        next_telemetry = mono + config.telemetry_ms / 1000.0

                    if mono >= next_poll:
                        seq += 1
                        start_s = max(1, int(time.time()) - config.recent_window_seconds)
                        obs, rows = await poller.poll(
                            session_id=session_id,
                            seq=seq,
                            wallet=wallet,
                            start_epoch_s=start_s,
                        )
                        forensic_store.save_latency_poll(obs)
                        poll_rtts.append(obs.rtt_us)
                        if obs.status_code == 429:
                            rate_limited += 1
                        elif obs.status_code == 503:
                            service_unavailable += 1

                        for trade in sorted(rows, key=lambda r: ((r.timestamp_us or 0), r.trade_id)):
                            if trade.trade_id in seen:
                                continue
                            seen.add(trade.trade_id)
                            if trade.timestamp_us is not None and trade.timestamp_us < started_us - 2 * MICROS:
                                continue
                            if not trade.is_crypto_5m:
                                continue

                            detection_sample = None
                            if trade.is_btc_5m:
                                btc_market, btc_collector = collector_pool.current("BTC")
                                detection_sample = _sample_from_state(
                                    session_id=session_id, wallet=wallet, market=btc_market,
                                    book_collector=btc_collector, external_store=read_store, model_paths=config.model_paths,
                                )
                                forensic_store.save_live_sample(detection_sample)
                                latest_sample_id = detection_sample.sample_id
                            correlation = await _wait_for_correlation(
                                market_store,
                                trade,
                                window_us=config.correlation_window_ms * 1000,
                            )
                            candidate = correlation.candidate
                            delta_us = None
                            if candidate is not None:
                                delta_us = obs.response_received_us - candidate.received_at_us
                                if delta_us >= 0:
                                    correlated_latencies.append(delta_us)
                            trade_collector = collector_pool.collector_for_slug(trade.slug)
                            executable, price_delta = _shadow_quote(trade, trade_collector)
                            event = LatencyEvent(
                                latency_event_id=f"lat_{session_id}_{trade.trade_id}",
                                session_id=session_id,
                                wallet=wallet,
                                trade_id=trade.trade_id,
                                trade_ts_us=trade.timestamp_us,
                                api_first_seen_us=obs.response_received_us,
                                api_request_started_us=obs.request_started_us,
                                api_rtt_us=obs.rtt_us,
                                slug=trade.slug,
                                token_id=trade.token_id,
                                side=(trade.side or "").upper() or None,
                                outcome=(trade.outcome or "").upper() or None,
                                price=_decimal_text(trade.price),
                                size=_decimal_text(trade.size),
                                transaction_hash=trade.transaction_hash,
                                market_trade_id=candidate.market_trade_id if candidate else None,
                                market_received_at_us=candidate.received_at_us if candidate else None,
                                market_source_ts_us=candidate.source_ts_us if candidate else None,
                                market_to_wallet_us=delta_us,
                                correlation_quality=correlation.quality,
                                correlation_candidate_count=correlation.candidate_count,
                                detection_sample_id=detection_sample.sample_id if detection_sample is not None else None,
                                current_executable_price=executable,
                                copy_price_delta=price_delta,
                                raw_json=trade.raw_json,
                            )
                            forensic_store.save_latency_event(event)
                            event_count += 1
                            verdict = latency_verdict(correlated_latencies, min_samples=config.min_verdict_samples)

                            print("\nTARGET WALLET TRADE FIRST SEEN")
                            print(f"  reported UTC     {epoch_us_to_iso(trade.timestamp_us)} (coarse wallet-feed clock)")
                            print(f"  API first seen   {epoch_us_to_iso(obs.response_received_us)}")
                            print(f"  API RTT          {obs.rtt_us / 1000.0:.1f} ms")
                            print(f"  action           {(trade.side or '-').upper()} {(trade.outcome or '-').upper()}")
                            asset_name = "BTC" if trade.is_btc_5m else "ETH"
                            print(f"  asset            {asset_name} 5m")
                            print(f"  trade id         {trade.trade_id}")
                            print(f"  tx hash          {trade.transaction_hash or '-'}")
                            print(f"  slug             {trade.slug or '-'}")
                            print(f"  token            {trade.token_id or '-'}")
                            print(f"  target price     {trade.price if trade.price is not None else '-'}")
                            print(f"  target size      {trade.size if trade.size is not None else '-'}")
                            print(f"  attribution      {trade.attribution or '-'}")
                            print(f"  correlation      {correlation.quality} candidates={correlation.candidate_count}")
                            if candidate is not None:
                                print(f"  CLOB received    {epoch_us_to_iso(candidate.received_at_us)}")
                                print(f"  market->wallet   {delta_us / 1000.0:.1f} ms")
                            else:
                                print("  market->wallet   UNRESOLVED")
                                nearby = _nearby_market_candidates(market_store, trade, window_us=config.nearby_window_ms * 1000)
                                if nearby:
                                    print(f"  nearby CLOB      {len(nearby)} event(s) within +/-{config.nearby_window_ms}ms")
                                    for row in nearby[:5]:
                                        offset = None if (trade.timestamp_us is None or row.source_ts_us is None) else (row.source_ts_us - trade.timestamp_us) / 1000.0
                                        off_text = "?" if offset is None else f"{offset:+.1f}ms"
                                        print(f"    {off_text} price={row.price} size={row.size} side={row.taker_side or '-'} tx={row.source_trade_id or '-'}")
                                else:
                                    print(f"  nearby CLOB      NONE within +/-{config.nearby_window_ms}ms")
                            print(f"  executable now   {executable or 'UNAVAILABLE'}")
                            print(f"  copy price delta {price_delta or '-'}")
                            if detection_sample is not None and detection_sample.p_up_mc is not None:
                                print(f"  our P(UP)        {detection_sample.p_up_mc:.4f}")
                            elif trade.is_eth_5m:
                                print("  BTC model        N/A (ETH used for latency only)")
                            print(f"  <=200ms share    {('-' if verdict.under_200ms_fraction is None else f'{100*verdict.under_200ms_fraction:.1f}%')}")
                            print(f"  provisional      {verdict.verdict}")

                        # Cadence is measured from the requested schedule. If a request takes longer than
                        # poll_ms, the next request starts immediately rather than overlapping requests.
                        next_poll += config.poll_ms / 1000.0
                        if next_poll < loop.time() - config.poll_ms / 1000.0:
                            next_poll = loop.time()

                    if mono - last_status_print >= 10.0:
                        rtt50 = statistics.median(poll_rtts) / 1000.0 if poll_rtts else float("nan")
                        verdict = latency_verdict(correlated_latencies, min_samples=config.min_verdict_samples)
                        print(
                            f"  health polls={seq} events={event_count} 429={rate_limited} 503={service_unavailable} "
                            f"RTT50={rtt50:.1f}ms correlated={verdict.sample_count} verdict={verdict.verdict}"
                        )
                        last_status_print = mono

                    await asyncio.sleep(0.005)
            finally:
                await collector_pool.stop()
                await asyncio.gather(
                    coinbase.request_stop(), kraken.request_stop(), twap.request_stop(), return_exceptions=True
                )
                await asyncio.gather(*long_tasks, return_exceptions=True)
                verdict = latency_verdict(correlated_latencies, min_samples=config.min_verdict_samples)
                ended_us = time.time_ns() // 1000
                forensic_store.finish_latency_session(
                    session_id=session_id,
                    ended_at_us=ended_us,
                    poll_count=seq,
                    target_event_count=event_count,
                    rate_limit_count=rate_limited,
                    service_unavailable_count=service_unavailable,
                    verdict=asdict(verdict),
                    state="STOPPED",
                )
                forensic_store.finish_live_session(
                    session_id=session_id,
                    ended_at_us=ended_us,
                    target_event_count=event_count,
                    state="LATENCY_LAB_STOPPED",
                )

            print("\nLatency-lab session complete")
            _print_latency_summary(verdict, seq, rate_limited, service_unavailable, poll_rtts)
            return 0


def _print_latency_summary(
    verdict: LatencyVerdict,
    poll_count: int,
    rate_limit_count: int,
    service_unavailable_count: int,
    poll_rtts: list[int],
) -> None:
    print(f"  poll requests     {poll_count}")
    print(f"  HTTP 429          {rate_limit_count}")
    print(f"  HTTP 503          {service_unavailable_count}")
    if poll_rtts:
        print(f"  API RTT p50       {statistics.median(poll_rtts) / 1000.0:.1f} ms")
    print(f"  correlated events {verdict.sample_count}")
    print(f"  market->wallet p50 {('-' if verdict.p50_ms is None else f'{verdict.p50_ms:.1f} ms')}")
    print(f"  market->wallet p90 {('-' if verdict.p90_ms is None else f'{verdict.p90_ms:.1f} ms')}")
    print(f"  market->wallet p95 {('-' if verdict.p95_ms is None else f'{verdict.p95_ms:.1f} ms')}")
    print(f"  <=200 ms fraction {('-' if verdict.under_200ms_fraction is None else f'{100*verdict.under_200ms_fraction:.1f}%')}")
    print(f"  verdict           {verdict.verdict}")
    print("  interpretation    CONSISTENT_SUB_200MS requires p90 <= 200 ms and enough correlated trades")
    print("  trading            DISABLED; this command only measures and records")


def latency_report(root: Path, wallet: str, *, min_samples: int = 20) -> LatencyVerdict:
    with ForensicsStore.open_project(root) as store:
        values = store.correlated_wallet_latencies(wallet)
    return latency_verdict(values, min_samples=min_samples)
