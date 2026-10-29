from __future__ import annotations

import asyncio
import json
import uuid
from collections import deque
from dataclasses import asdict, dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Deque

from ..features.books import MarketBookFeatures, market_book_features
from ..features.btc import CausalBtcFeatureEngine, FeatureUnavailable
from ..features.lab import build_strategy_lab_features
from ..feeds.coinbase import CoinbaseCollector
from ..feeds.kraken import KrakenCollector
from ..feeds.polymarket import DiscoveredMarket, PolymarketPublicRest
from ..feeds.polymarket_collector import PolymarketBookCollector
from ..feeds.settlement_reference import AuthenticatedTwapCollector, PriceReferenceMapper
from ..models.settlement import SettlementModelConfig, TwapSettlementModel
from ..storage.external_store import ExternalStore
from ..storage.market_store import MarketStore
from ..timeutil import epoch_us_to_iso, now_epoch_us
from .client import PolymarketDataClient
from .models import WalletTrade
from .store import ForensicsStore

MICROS = 1_000_000


@dataclass(frozen=True)
class LiveForensicsConfig:
    poll_seconds: float = 1.0
    sample_seconds: float = 1.0
    discovery_seconds: float = 2.0
    prebuffer_seconds: int = 120
    model_paths: int = 2_000

    def validate(self) -> None:
        if self.poll_seconds < 0.5:
            raise ValueError("poll_seconds must be >= 0.5")
        if self.sample_seconds < 0.25:
            raise ValueError("sample_seconds must be >= 0.25")
        if self.discovery_seconds < 0.5:
            raise ValueError("discovery_seconds must be >= 0.5")
        if self.prebuffer_seconds < 30:
            raise ValueError("prebuffer_seconds must be >= 30")
        if self.model_paths < 100:
            raise ValueError("model_paths must be >= 100")


@dataclass(frozen=True)
class LiveTelemetrySample:
    sample_id: str
    session_id: str
    wallet: str
    sample_ts_us: int
    slug: str | None
    round_start_us: int | None
    round_end_us: int | None
    seconds_remaining: float | None
    state: str
    unavailable_reason: str | None
    ptb: float | None
    official_twap: float | None
    coinbase_price: float | None
    kraken_price: float | None
    predictor_price: float | None
    p_up_mc: float | None
    p_up_analytic: float | None
    distance_dollars: float | None
    returns_json: str
    volatility_json: str
    lab_json: str
    book_json: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class TargetWalletEvent:
    event_id: str
    session_id: str
    wallet: str
    trade_id: str
    trade_ts_us: int | None
    detected_at_us: int
    observable_lag_us: int | None
    slug: str | None
    side: str | None
    outcome: str | None
    price: str | None
    size: str | None
    attribution: str | None
    pre_trade_sample_id: str | None
    detection_sample_id: str | None
    current_executable_price: str | None
    copy_price_delta: str | None
    market_context_matches: bool
    raw_json: str


class SeenTradeIds:
    def __init__(self, initial: set[str] | None = None) -> None:
        self._seen = set(initial or ())

    def mark_new(self, trades: list[WalletTrade]) -> list[WalletTrade]:
        new_rows: list[WalletTrade] = []
        for trade in sorted(trades, key=lambda row: ((row.timestamp_us or 0), row.trade_id)):
            if trade.trade_id in self._seen:
                continue
            self._seen.add(trade.trade_id)
            new_rows.append(trade)
        return new_rows


def last_sample_at_or_before(samples: Deque[LiveTelemetrySample], target_us: int | None) -> LiveTelemetrySample | None:
    if target_us is None:
        return None
    for sample in reversed(samples):
        if sample.sample_ts_us <= target_us:
            return sample
    return None


def _decimal_text(value: float | Decimal | None) -> str | None:
    if value is None:
        return None
    return format(Decimal(str(value)), "f")


def _book_payload(book: MarketBookFeatures | None) -> dict[str, Any]:
    if book is None:
        return {}
    return {
        "up": asdict(book.up),
        "down": asdict(book.down),
        "book_direction_score": book.book_direction_score,
    }


def _raw_collector_book_payload(collector: PolymarketBookCollector | None, slug: str | None, decision_time_us: int | None=None) -> dict[str, Any]:
    """Expose raw top-of-book health even when full feature construction is impossible.

    This is diagnostic only; it never fabricates missing liquidity and is not used
    as a strategy input.
    """
    if collector is None or slug is None or collector.market.slug != slug:
        return {}
    out: dict[str, Any] = {}
    at_us=now_epoch_us() if decision_time_us is None else decision_time_us
    for state in collector.states.values():
        bid = state.best_bid
        ask = state.best_ask
        out[state.outcome.lower()] = {
            "outcome": state.outcome.upper(),
            "best_bid": None if bid is None else float(bid.price),
            "best_ask": None if ask is None else float(ask.price),
            "spread": None if state.spread is None else float(state.spread),
            "bid_levels": len(state.bids),
            "ask_levels": len(state.asks),
            "synchronized": bool(state.synchronized),
            "sync_generation": int(state.sync_generation),
            "depth_reliable":state.depth_reliable,"integrity_reason":state.integrity_reason,
            "source_ts_us":state.last_server_ts_us,"received_at_us":state.last_received_at_us,
            "source_age_seconds":None if state.last_server_ts_us is None else (at_us-state.last_server_ts_us)/MICROS,
            "receive_age_seconds":None if state.last_received_at_us is None else (at_us-state.last_received_at_us)/MICROS,
        }
    out["book_direction_score"] = None
    out["quality"] = "RAW_PARTIAL"
    return out


def _external_health_payload(external_store: ExternalStore, now_us: int) -> dict[str, Any]:
    """Compact source-age diagnostics persisted with every telemetry sample."""
    try:
        components = external_store.component_health()
    except Exception as exc:
        return {"error": type(exc).__name__}
    out: dict[str, Any] = {}
    for component in ("coinbase_btc_usd", "kraken_btc_usd", "settlement_reference"):
        row = components.get(component)
        if row is None:
            out[component] = {"state": "MISSING"}
            continue
        out[component] = {
            "state": row.state,
            "message_age_s": None
            if row.last_message_at_us is None
            else max(0.0, (now_us - row.last_message_at_us) / MICROS),
            "tick_age_s": None
            if row.last_tick_at_us is None
            else max(0.0, (now_us - row.last_tick_at_us) / MICROS),
            "reconnect_count": row.reconnect_count,
            "error_count": row.error_count,
            "details": row.details,
            "source_trade_age_s":None if type(row.details.get('latest_source_ts_us')) is not int else (now_us-row.details['latest_source_ts_us'])/MICROS,
            "status_age_s":(now_us-row.updated_at_us)/MICROS if row.updated_at_us is not None else None,
        }
    return out


def _shadow_price(trade: WalletTrade, collector: PolymarketBookCollector | None) -> tuple[str | None, str | None, bool]:
    if collector is None or not trade.slug or trade.slug != collector.market.slug:
        return None, None, False
    target_state = None
    for state in collector.states.values():
        if state.outcome.upper() == (trade.outcome or "").upper():
            target_state = state
            break
    if target_state is None or not target_state.synchronized:
        return None, None, True
    side = (trade.side or "").upper()
    level = target_state.best_ask if side == "BUY" else target_state.best_bid if side == "SELL" else None
    if level is None:
        return None, None, True
    executable = level.price
    if trade.price is None:
        return format(executable, "f"), None, True
    delta = executable - trade.price if side == "BUY" else trade.price - executable
    return format(executable, "f"), format(delta, "f"), True


class MarketCollectorManager:
    def __init__(self, *, root: Path, rest: PolymarketPublicRest, store: MarketStore) -> None:
        self.root = root
        self.rest = rest
        self.store = store
        self.market: DiscoveredMarket | None = None
        self.collector: PolymarketBookCollector | None = None
        self.task: asyncio.Task | None = None

    async def switch(self, market: DiscoveredMarket) -> None:
        if self.market is not None and self.market.slug == market.slug and self.task is not None and not self.task.done():
            return
        await self.stop()
        self.market = market
        self.store.persist_market(market, now_epoch_us())
        self.collector = PolymarketBookCollector(root=self.root, market=market, store=self.store, rest=self.rest)
        self.task = asyncio.create_task(self.collector.run())

    async def stop(self) -> None:
        collector = self.collector
        task = self.task
        self.collector = None
        self.task = None
        if collector is not None:
            await collector.request_stop()
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)


def _sample_from_state(
    *,
    session_id: str,
    wallet: str,
    market: DiscoveredMarket | None,
    book_collector: PolymarketBookCollector | None,
    external_store: ExternalStore,
    model_paths: int,
    decision_time_us: int | None = None,
) -> LiveTelemetrySample:
    now_us = now_epoch_us() if decision_time_us is None else decision_time_us
    sample_id = f"sample_{session_id}_{now_us}"
    if decision_time_us is not None:
        sample_id += f"_{market.slug if market is not None else 'unavailable'}"
    source_health = _external_health_payload(external_store, now_us)
    if market is None:
        return LiveTelemetrySample(
            sample_id, session_id, wallet, now_us, None, None, None, None,
            "NO_MARKET", "NO_LIVE_MARKET", None, None, None, None, None,
            None, None, None, "{}", "{}", "{}", "{}",
        )

    book: MarketBookFeatures | None = None
    book_reason: str | None = None
    book_partial = False
    if book_collector is not None and book_collector.market.slug == market.slug:
        try:
            up = next(state for state in book_collector.states.values() if state.outcome == "UP")
            down = next(state for state in book_collector.states.values() if state.outcome == "DOWN")
            problems: list[str] = []
            unsync = False
            for state in (up, down):
                if not state.synchronized:
                    problems.append(f"{state.outcome.upper()}_UNSYNC")
                    unsync = True
                    continue
                age = state.age_seconds(now_us)
                if age is None or age > 2.0:
                    problems.append(f"{state.outcome.upper()}_DEPTH_UNRELIABLE_OR_STALE")
                    unsync = True
                    continue
                if state.best_bid is None:
                    problems.append(f"{state.outcome.upper()}_NO_BID")
                if state.best_ask is None:
                    problems.append(f"{state.outcome.upper()}_NO_ASK")
            if not problems:
                book = market_book_features(up, down, depth_band=0.03)
            elif not unsync:
                # Synchronized one-sided books are a valid market condition near
                # binary price boundaries. They are partial, not a collector outage.
                book_partial = True
                book_reason = ",".join(problems)
            else:
                book_reason = ",".join(problems)
        except (StopIteration, ValueError) as exc:
            book_reason = str(exc)
    else:
        book_reason = "MISSING_BOOK_COLLECTOR"

    engine = CausalBtcFeatureEngine(external_store)
    PriceReferenceMapper(external_store).map_existing_exact(market, now_us)
    retained_reference=external_store.market_reference_status(market,now_us)
    try:
        features = engine.build(market=market, decision_time_us=now_us)
        lab = build_strategy_lab_features(features)
        result = TwapSettlementModel(
            engine,
            SettlementModelConfig(paths=model_paths, volatility_lookback_seconds=60),
        ).evaluate(market=market, decision_time_us=now_us, features=features)
        return LiveTelemetrySample(
            sample_id=sample_id,
            session_id=session_id,
            wallet=wallet,
            sample_ts_us=now_us,
            slug=market.slug,
            round_start_us=market.round_start_us,
            round_end_us=market.round_end_us,
            seconds_remaining=features.seconds_remaining,
            state=(
                "READY"
                if book is not None
                else "READY_PARTIAL_BOOK"
                if book_partial
                else "MODEL_READY_BOOK_UNAVAILABLE"
            ),
            unavailable_reason=book_reason,
            ptb=result.price_to_beat,
            official_twap=result.official_twap,
            coinbase_price=features.coinbase_price,
            kraken_price=features.kraken_price,
            predictor_price=result.predictor_price,
            p_up_mc=result.p_up,
            p_up_analytic=result.analytical_p_up,
            distance_dollars=features.distance_dollars,
            returns_json=json.dumps(features.returns, sort_keys=True, separators=(",", ":")),
            volatility_json=json.dumps(features.realized_volatility, sort_keys=True, separators=(",", ":")),
            lab_json=json.dumps(
                {
                    **asdict(lab),
                    "market_reference": retained_reference,
                    "predictor_sources": list(features.current_sources),
                    "predictor_health": source_health,
                    "model_policy_revision": result.model_policy_revision,
                    "calibration_state": result.calibration_state,
                    "mc_standard_error": result.mc_standard_error,
                    "official_twap_source_ts_us": features.official_twap_source_ts_us,
                },
                sort_keys=True,
                separators=(",", ":"),
                default=str,
            ),
            book_json=json.dumps(
                {**_book_payload(book),'health':_raw_collector_book_payload(book_collector,market.slug,now_us)} if book is not None else _raw_collector_book_payload(book_collector, market.slug,now_us),
                sort_keys=True,
                separators=(",", ":"),
            ),
        )
    except FeatureUnavailable as exc:
        return LiveTelemetrySample(
            sample_id=sample_id,
            session_id=session_id,
            wallet=wallet,
            sample_ts_us=now_us,
            slug=market.slug,
            round_start_us=market.round_start_us,
            round_end_us=market.round_end_us,
            seconds_remaining=(market.round_end_us - now_us) / MICROS,
            state="WAITING",
            unavailable_reason=str(exc),
            ptb=None if retained_reference['price_to_beat'] is None else float(retained_reference['price_to_beat']),
            official_twap=None,
            coinbase_price=None,
            kraken_price=None,
            predictor_price=None,
            p_up_mc=None,
            p_up_analytic=None,
            distance_dollars=None,
            returns_json="{}",
            volatility_json="{}",
            lab_json=json.dumps(
                {"predictor_health": source_health,"market_reference":retained_reference,
                 "causal_history_failure": engine.last_unavailable_context},
                sort_keys=True,
                separators=(",", ":"),
                default=str,
            ),
            book_json=json.dumps(
                {**_book_payload(book),'health':_raw_collector_book_payload(book_collector,market.slug,now_us)} if book is not None else _raw_collector_book_payload(book_collector, market.slug,now_us),
                sort_keys=True,
                separators=(",", ":"),
            ),
        )


async def run_live_wallet_forensics(
    root: Path,
    *,
    requested_wallet: str,
    seconds: float | None,
    config: LiveForensicsConfig,
) -> int:
    config.validate()
    session_id = str(uuid.uuid4())
    started_us = now_epoch_us()
    deadline_us = None if seconds is None else started_us + int(seconds * MICROS)

    async with PolymarketDataClient() as data_client, PolymarketPublicRest() as rest:
        wallet, _profile = await data_client.resolve_proxy_wallet(requested_wallet)
        initial_market = await rest.discover_btc_5m()
        baseline_start = max(1, int(started_us / MICROS) - 120)
        baseline = await data_client.trades(
            wallet,
            start=baseline_start,
            end=0,
            limit=500,
            max_pages=1,
            include_maker_rows=True,
        )
        seen = SeenTradeIds({row.trade_id for row in baseline})

        with (
            MarketStore.open_project(root) as market_store,
            ExternalStore.open_project(root) as cb_store,
            ExternalStore.open_project(root) as kr_store,
            ExternalStore.open_project(root) as twap_store,
            ExternalStore.open_project(root) as read_store,
            ForensicsStore.open_project(root) as forensic_store,
        ):
            forensic_store.start_live_session(
                session_id=session_id,
                wallet=wallet,
                requested_wallet=requested_wallet,
                started_at_us=started_us,
                config=asdict(config),
            )

            coinbase = CoinbaseCollector(root=root, store=cb_store)
            kraken = KrakenCollector(root=root, store=kr_store)
            twap = AuthenticatedTwapCollector(root=root, store=twap_store, market=initial_market)
            market_manager = MarketCollectorManager(root=root, rest=rest, store=market_store)
            await market_manager.switch(initial_market)
            long_tasks = [
                asyncio.create_task(coinbase.run()),
                asyncio.create_task(kraken.run()),
                asyncio.create_task(twap.run()),
            ]

            sample_buffer: Deque[LiveTelemetrySample] = deque(
                maxlen=max(60, int(config.prebuffer_seconds / config.sample_seconds) + 10)
            )
            next_sample = 0.0
            next_poll = 0.0
            next_discovery = 0.0
            loop = asyncio.get_running_loop()
            event_count = 0
            last_wait_reason: str | None = None

            print("Live target-wallet strategy forensics")
            print(f"  session        {session_id}")
            print(f"  wallet         {wallet}")
            print(f"  public poll    {config.poll_seconds:.2f}s")
            print(f"  telemetry      {config.sample_seconds:.2f}s causal samples")
            print(f"  prebuffer      {config.prebuffer_seconds}s")
            print(f"  model paths    {config.model_paths}")
            print("  copy mode      SHADOW ONLY - records executable quote, sends no orders")
            print("  real trading   DISABLED")
            print("  stop           Ctrl+C")

            try:
                while deadline_us is None or now_epoch_us() < deadline_us:
                    for task in long_tasks:
                        if task.done():
                            exc = task.exception()
                            if exc is not None:
                                raise exc
                            raise RuntimeError("a live external collector stopped unexpectedly")
                    if market_manager.task is not None and market_manager.task.done():
                        exc = market_manager.task.exception()
                        if exc is not None:
                            raise exc
                        raise RuntimeError("the Polymarket L2 collector stopped unexpectedly")

                    mono = loop.time()
                    if mono >= next_discovery:
                        discovered = await rest.discover_btc_5m()
                        if market_manager.market is None or discovered.slug != market_manager.market.slug:
                            print(f"  market rollover {market_manager.market.slug if market_manager.market else '-'} -> {discovered.slug}")
                            await market_manager.switch(discovered)
                        PriceReferenceMapper(read_store).map_existing_exact(discovered, now_epoch_us())
                        next_discovery = mono + config.discovery_seconds

                    if mono >= next_sample:
                        sample = _sample_from_state(
                            session_id=session_id,
                            wallet=wallet,
                            market=market_manager.market,
                            book_collector=market_manager.collector,
                            external_store=read_store,
                            model_paths=config.model_paths,
                        )
                        forensic_store.save_live_sample(sample)
                        sample_buffer.append(sample)
                        if sample.state != "READY" and sample.unavailable_reason != last_wait_reason:
                            print(f"  telemetry wait {sample.unavailable_reason}")
                            last_wait_reason = sample.unavailable_reason
                        elif sample.state.startswith("READY"):
                            last_wait_reason = None
                        next_sample = mono + config.sample_seconds

                    if mono >= next_poll:
                        recent = await data_client.trades(
                            wallet,
                            start=baseline_start,
                            end=0,
                            limit=500,
                            max_pages=1,
                            include_maker_rows=True,
                        )
                        for trade in seen.mark_new(recent):
                            detected_us = now_epoch_us()
                            if trade.timestamp_us is not None and trade.timestamp_us < started_us - 5 * MICROS:
                                continue
                            if not trade.is_btc_5m:
                                continue

                            # Capture an immediate detection-time sample in addition to the 1 Hz grid.
                            detection_sample = _sample_from_state(
                                session_id=session_id,
                                wallet=wallet,
                                market=market_manager.market,
                                book_collector=market_manager.collector,
                                external_store=read_store,
                                model_paths=config.model_paths,
                            )
                            forensic_store.save_live_sample(detection_sample)
                            sample_buffer.append(detection_sample)
                            pre = last_sample_at_or_before(sample_buffer, trade.timestamp_us)
                            executable, delta, context_matches = _shadow_price(trade, market_manager.collector)
                            lag = None if trade.timestamp_us is None else max(0, detected_us - trade.timestamp_us)
                            event = TargetWalletEvent(
                                event_id=f"target_{session_id}_{trade.trade_id}",
                                session_id=session_id,
                                wallet=wallet,
                                trade_id=trade.trade_id,
                                trade_ts_us=trade.timestamp_us,
                                detected_at_us=detected_us,
                                observable_lag_us=lag,
                                slug=trade.slug,
                                side=(trade.side or "").upper() or None,
                                outcome=(trade.outcome or "").upper() or None,
                                price=_decimal_text(trade.price),
                                size=_decimal_text(trade.size),
                                attribution=trade.attribution,
                                pre_trade_sample_id=pre.sample_id if pre else None,
                                detection_sample_id=detection_sample.sample_id,
                                current_executable_price=executable,
                                copy_price_delta=delta,
                                market_context_matches=context_matches,
                                raw_json=trade.raw_json,
                            )
                            forensic_store.save_target_event(event)
                            event_count += 1
                            print("\nTARGET BTC5M TRADE DETECTED")
                            print(f"  reported UTC   {epoch_us_to_iso(trade.timestamp_us)}")
                            print(f"  detected UTC   {epoch_us_to_iso(detected_us)}")
                            print(f"  observable lag {('-' if lag is None else f'{lag / MICROS:.3f}s')}")
                            print(f"  market         {trade.slug or '-'}")
                            print(f"  action         {(trade.side or '-').upper()} {(trade.outcome or '-').upper()}")
                            print(f"  target price   {trade.price if trade.price is not None else '-'}")
                            print(f"  target size    {trade.size if trade.size is not None else '-'}")
                            print(f"  attribution    {trade.attribution or 'UNKNOWN'}")
                            print(f"  pre-sample     {pre.sample_id if pre else 'UNAVAILABLE'}")
                            print(f"  our quote now  {executable or 'UNAVAILABLE'}")
                            print(f"  price delta    {delta or '-'}")
                            if detection_sample.p_up_mc is not None:
                                print(f"  our P(UP) now  {detection_sample.p_up_mc:.4f}")
                            print("  shadow action  RECORDED ONLY; no order submitted")

                        next_poll = mono + config.poll_seconds

                    await asyncio.sleep(0.05)
            finally:
                await market_manager.stop()
                await asyncio.gather(
                    coinbase.request_stop(),
                    kraken.request_stop(),
                    twap.request_stop(),
                    return_exceptions=True,
                )
                await asyncio.gather(*long_tasks, return_exceptions=True)
                forensic_store.finish_live_session(
                    session_id=session_id,
                    ended_at_us=now_epoch_us(),
                    target_event_count=event_count,
                    state="STOPPED",
                )

            print("\nLive forensic session complete")
            print(f"  session        {session_id}")
            print(f"  target events  {event_count}")
            print("  raw market/external feeds remain stored in the existing V3 databases")
            print("  target events + derived snapshots stored in research/v3_research.sqlite")
            return 0

COPY_QUOTE_DELAYS_MS = (0, 250, 500, 1000, 2000, 5000)


@dataclass(frozen=True)
class CopyQuoteObservation:
    quote_id: str
    event_id: str
    session_id: str
    wallet: str
    delay_ms: int
    quote_ts_us: int
    slug: str | None
    side: str | None
    outcome: str | None
    target_price: str | None
    executable_price: str | None
    price_delta: str | None
    market_context_matches: bool
    details_json: str


async def _capture_copy_quotes(
    *,
    trade: WalletTrade,
    event: TargetWalletEvent,
    collector_manager: MarketCollectorManager,
    store: ForensicsStore,
) -> None:
    prior_delay = 0
    for delay_ms in COPY_QUOTE_DELAYS_MS:
        if delay_ms > prior_delay:
            await asyncio.sleep((delay_ms - prior_delay) / 1000.0)
        executable, delta, context_matches = _shadow_price(trade, collector_manager.collector)
        quote_ts_us = now_epoch_us()
        quote = CopyQuoteObservation(
            quote_id=f"copyq_{event.event_id}_{delay_ms}",
            event_id=event.event_id,
            session_id=event.session_id,
            wallet=event.wallet,
            delay_ms=delay_ms,
            quote_ts_us=quote_ts_us,
            slug=trade.slug,
            side=(trade.side or "").upper() or None,
            outcome=(trade.outcome or "").upper() or None,
            target_price=_decimal_text(trade.price),
            executable_price=executable,
            price_delta=delta,
            market_context_matches=context_matches,
            details_json=json.dumps(
                {
                    "delay_basis": "AFTER_PUBLIC_DETECTION",
                    "reported_trade_ts_us": trade.timestamp_us,
                    "detected_at_us": event.detected_at_us,
                    "observable_lag_us": event.observable_lag_us,
                },
                sort_keys=True,
                separators=(",", ":"),
            ),
        )
        store.save_copy_quote(quote)
        prior_delay = delay_ms


async def run_live_wallet_tournament(
    root: Path,
    *,
    requested_wallets: list[str],
    seconds: float | None,
    config: LiveForensicsConfig,
) -> int:
    """Watch several public wallets against one shared causal market telemetry stream.

    This is research/shadow mode only. It never submits an order. Copy quotes are sampled
    after each target trade becomes publicly observable at 0/250/500/1000/2000/5000 ms.
    """
    config.validate()
    if not requested_wallets:
        raise ValueError("requested_wallets must not be empty")
    if len(requested_wallets) > 20:
        raise ValueError("at most 20 target wallets may be watched in one process")

    session_id = str(uuid.uuid4())
    started_us = now_epoch_us()
    deadline_us = None if seconds is None else started_us + int(seconds * MICROS)

    async with PolymarketDataClient() as data_client, PolymarketPublicRest() as rest:
        resolved: list[str] = []
        for requested in requested_wallets:
            wallet, _profile = await data_client.resolve_proxy_wallet(requested)
            if wallet.lower() not in {row.lower() for row in resolved}:
                resolved.append(wallet)
        initial_market = await rest.discover_btc_5m()
        baseline_start = max(1, int(started_us / MICROS) - 120)
        seen_by_wallet: dict[str, SeenTradeIds] = {}
        for wallet in resolved:
            baseline = await data_client.trades(
                wallet,
                start=baseline_start,
                end=0,
                limit=500,
                max_pages=1,
                include_maker_rows=True,
            )
            seen_by_wallet[wallet.lower()] = SeenTradeIds({row.trade_id for row in baseline})

        with (
            MarketStore.open_project(root) as market_store,
            ExternalStore.open_project(root) as cb_store,
            ExternalStore.open_project(root) as kr_store,
            ExternalStore.open_project(root) as twap_store,
            ExternalStore.open_project(root) as read_store,
            ForensicsStore.open_project(root) as forensic_store,
        ):
            forensic_store.start_live_session(
                session_id=session_id,
                wallet="multi",
                requested_wallet=",".join(requested_wallets),
                started_at_us=started_us,
                config={**asdict(config), "wallets": resolved, "copy_quote_delays_ms": list(COPY_QUOTE_DELAYS_MS)},
            )

            coinbase = CoinbaseCollector(root=root, store=cb_store)
            kraken = KrakenCollector(root=root, store=kr_store)
            twap = AuthenticatedTwapCollector(root=root, store=twap_store, market=initial_market)
            market_manager = MarketCollectorManager(root=root, rest=rest, store=market_store)
            await market_manager.switch(initial_market)
            long_tasks = [
                asyncio.create_task(coinbase.run()),
                asyncio.create_task(kraken.run()),
                asyncio.create_task(twap.run()),
            ]
            quote_tasks: set[asyncio.Task] = set()

            sample_buffer: Deque[LiveTelemetrySample] = deque(
                maxlen=max(60, int(config.prebuffer_seconds / config.sample_seconds) + 10)
            )
            next_sample = 0.0
            next_poll = 0.0
            next_discovery = 0.0
            loop = asyncio.get_running_loop()
            event_count = 0
            last_wait_reason: str | None = None

            print("Live multi-wallet strategy forensics")
            print(f"  session        {session_id}")
            print(f"  wallets        {len(resolved)}")
            for wallet in resolved:
                print(f"    {wallet}")
            print(f"  public poll    {config.poll_seconds:.2f}s per tournament cycle")
            print(f"  telemetry      {config.sample_seconds:.2f}s shared causal samples")
            print(f"  prebuffer      {config.prebuffer_seconds}s")
            print(f"  copy curve     {', '.join(str(v) + 'ms' for v in COPY_QUOTE_DELAYS_MS)} after detection")
            print("  copy mode      SHADOW ONLY - quotes recorded, no orders")
            print("  real trading   DISABLED")
            print("  stop           Ctrl+C")

            try:
                while deadline_us is None or now_epoch_us() < deadline_us:
                    for task in long_tasks:
                        if task.done():
                            exc = task.exception()
                            if exc is not None:
                                raise exc
                            raise RuntimeError("a live external collector stopped unexpectedly")
                    if market_manager.task is not None and market_manager.task.done():
                        exc = market_manager.task.exception()
                        if exc is not None:
                            raise exc
                        raise RuntimeError("the Polymarket L2 collector stopped unexpectedly")

                    mono = loop.time()
                    if mono >= next_discovery:
                        discovered = await rest.discover_btc_5m()
                        if market_manager.market is None or discovered.slug != market_manager.market.slug:
                            print(f"  market rollover {market_manager.market.slug if market_manager.market else '-'} -> {discovered.slug}")
                            await market_manager.switch(discovered)
                        PriceReferenceMapper(read_store).map_existing_exact(discovered, now_epoch_us())
                        next_discovery = mono + config.discovery_seconds

                    if mono >= next_sample:
                        sample = _sample_from_state(
                            session_id=session_id,
                            wallet="multi",
                            market=market_manager.market,
                            book_collector=market_manager.collector,
                            external_store=read_store,
                            model_paths=config.model_paths,
                        )
                        forensic_store.save_live_sample(sample)
                        sample_buffer.append(sample)
                        if sample.state != "READY" and sample.unavailable_reason != last_wait_reason:
                            print(f"  telemetry wait {sample.unavailable_reason}")
                            last_wait_reason = sample.unavailable_reason
                        elif sample.state.startswith("READY"):
                            last_wait_reason = None
                        next_sample = mono + config.sample_seconds

                    if mono >= next_poll:
                        for wallet in resolved:
                            recent = await data_client.trades(
                                wallet,
                                start=baseline_start,
                                end=0,
                                limit=500,
                                max_pages=1,
                                include_maker_rows=True,
                            )
                            seen = seen_by_wallet[wallet.lower()]
                            for trade in seen.mark_new(recent):
                                detected_us = now_epoch_us()
                                if trade.timestamp_us is not None and trade.timestamp_us < started_us - 5 * MICROS:
                                    continue
                                if not trade.is_btc_5m:
                                    continue

                                detection_sample = _sample_from_state(
                                    session_id=session_id,
                                    wallet="multi",
                                    market=market_manager.market,
                                    book_collector=market_manager.collector,
                                    external_store=read_store,
                                    model_paths=config.model_paths,
                                )
                                forensic_store.save_live_sample(detection_sample)
                                sample_buffer.append(detection_sample)
                                pre = last_sample_at_or_before(sample_buffer, trade.timestamp_us)
                                executable, delta, context_matches = _shadow_price(trade, market_manager.collector)
                                lag = None if trade.timestamp_us is None else max(0, detected_us - trade.timestamp_us)
                                event = TargetWalletEvent(
                                    event_id=f"target_{session_id}_{trade.trade_id}",
                                    session_id=session_id,
                                    wallet=wallet,
                                    trade_id=trade.trade_id,
                                    trade_ts_us=trade.timestamp_us,
                                    detected_at_us=detected_us,
                                    observable_lag_us=lag,
                                    slug=trade.slug,
                                    side=(trade.side or "").upper() or None,
                                    outcome=(trade.outcome or "").upper() or None,
                                    price=_decimal_text(trade.price),
                                    size=_decimal_text(trade.size),
                                    attribution=trade.attribution,
                                    pre_trade_sample_id=pre.sample_id if pre else None,
                                    detection_sample_id=detection_sample.sample_id,
                                    current_executable_price=executable,
                                    copy_price_delta=delta,
                                    market_context_matches=context_matches,
                                    raw_json=trade.raw_json,
                                )
                                forensic_store.save_target_event(event)
                                task = asyncio.create_task(
                                    _capture_copy_quotes(
                                        trade=trade,
                                        event=event,
                                        collector_manager=market_manager,
                                        store=forensic_store,
                                    )
                                )
                                quote_tasks.add(task)
                                task.add_done_callback(quote_tasks.discard)
                                event_count += 1
                                print("\nTARGET BTC5M TRADE DETECTED")
                                print(f"  wallet         {wallet}")
                                print(f"  reported UTC   {epoch_us_to_iso(trade.timestamp_us)}")
                                print(f"  detected UTC   {epoch_us_to_iso(detected_us)}")
                                print(f"  observable lag {('-' if lag is None else f'{lag / MICROS:.3f}s')}")
                                print(f"  market         {trade.slug or '-'}")
                                print(f"  action         {(trade.side or '-').upper()} {(trade.outcome or '-').upper()}")
                                print(f"  target price   {trade.price if trade.price is not None else '-'}")
                                print(f"  target size    {trade.size if trade.size is not None else '-'}")
                                print(f"  attribution    {trade.attribution or 'UNKNOWN'}")
                                print(f"  pre-sample     {pre.sample_id if pre else 'UNAVAILABLE'}")
                                print(f"  quote @detect  {executable or 'UNAVAILABLE'}")
                                print(f"  price delta    {delta or '-'}")
                                if detection_sample.p_up_mc is not None:
                                    print(f"  our P(UP) now  {detection_sample.p_up_mc:.4f}")
                                print("  shadow action  RECORDED ONLY; no order submitted")

                        next_poll = mono + config.poll_seconds

                    await asyncio.sleep(0.05)
            finally:
                await market_manager.stop()
                await asyncio.gather(
                    coinbase.request_stop(),
                    kraken.request_stop(),
                    twap.request_stop(),
                    return_exceptions=True,
                )
                await asyncio.gather(*long_tasks, return_exceptions=True)
                if quote_tasks:
                    await asyncio.gather(*list(quote_tasks), return_exceptions=True)
                forensic_store.finish_live_session(
                    session_id=session_id,
                    ended_at_us=now_epoch_us(),
                    target_event_count=event_count,
                    state="STOPPED",
                )

            print("\nLive multi-wallet forensic session complete")
            print(f"  session        {session_id}")
            print(f"  target events  {event_count}")
            print("  quotes         0/250/500/1000/2000/5000ms after public detection")
            print("  real trading   DISABLED")
            return 0
