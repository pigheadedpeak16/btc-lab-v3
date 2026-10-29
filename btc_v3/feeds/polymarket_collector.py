from __future__ import annotations

import asyncio
import json
import queue
import threading
import time
import uuid
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import httpx
from websockets.sync.client import connect as ws_sync_connect

from ..state.book_state import OrderBookState
from ..state.readiness import ReadinessTimeline
from .clock_health import ClockCorrelation
from ..storage.market_store import MarketStore
from ..timeutil import now_epoch_us, parse_source_timestamp_us
from .polymarket import CLOB_MARKET_WS_URL, DiscoveredMarket, PolymarketPublicRest
from .raw_archive import RawJsonlArchive

COMPONENT = "polymarket_clob"
FEED_POLICY_REVISION = "phase595_clocked_recovery_v1"
EXECUTION_ADMISSION_REVISION = 'phase595_dual_clock_execution_v1'


@dataclass
class CollectorCounters:
    reconnect_count: int = 0
    error_count: int = 0
    raw_messages: int = 0
    book_snapshots: int = 0
    book_deltas: int = 0
    trades: int = 0
    ignored_unsynced_deltas: int = 0
    bba_mismatches: int = 0
    resync_requests: int = 0
    resync_failures: int = 0
    soft_resync_suppressed: int = 0
    resync_rate_limited: int = 0
    resync_failure_causes: dict[str, int] = field(default_factory=dict)
    resync_successes: int = 0
    receive_queue_peak: int = 0
    receive_queue_overflows: int = 0
    receive_processing_peak_ms: float = 0.0
    receive_backlog_peak_ms: float = 0.0
    heartbeat_pings: int = 0
    heartbeat_pongs: int = 0
    disconnect_causes: dict[str, int] = field(default_factory=dict)
    gap_duration_us: int = 0
    resync_superseded: int = 0
    source_ahead_of_receive_events: int = 0
    source_ahead_peak_us: int = 0


class PolymarketBookCollector:
    """Public/read-only CLOB market collector. There is no order-placement path."""

    def __init__(
        self,
        *,
        root: Path,
        market: DiscoveredMarket,
        store: MarketStore,
        rest: PolymarketPublicRest,
        ws_url: str = CLOB_MARKET_WS_URL,
        heartbeat_seconds: float = 10.0,
        forward_session_id: str | None = None,
    ) -> None:
        self.root = root
        self.market = market
        self.store = store
        self.rest = rest
        self.ws_url = ws_url
        self.heartbeat_seconds = heartbeat_seconds
        self.forward_session_id = forward_session_id
        self.session_id = str(uuid.uuid4())
        self.counters = CollectorCounters()
        self.clock_correlation=ClockCorrelation()
        self._message_timing=None
        self.receive_sequence = 0
        self.last_message_at_us: int | None = None
        self.last_book_at_us: int | None = None
        self.states = {
            market.up_token_id: OrderBookState(market.slug, market.up_token_id, "UP"),
            market.down_token_id: OrderBookState(market.slug, market.down_token_id, "DOWN"),
        }
        self._raw = RawJsonlArchive(root / "data" / "raw", COMPONENT)
        self._open_gap_ids: list[int] = []
        self._stop_requested = asyncio.Event()
        self._active_ws: Any | None = None
        self._ws_reader_stop: threading.Event | None = None
        self._reader_thread: threading.Thread | None = None
        self._reader_failed = threading.Event()
        self._connection_generation = 0
        self._gap_started_us: int | None = None
        self._ws_queue_capacity = 4096
        self._ws_backlog_max_age_us = 2_000_000
        self._soft_failure_streak: dict[str, int] = {}
        self._soft_next_allowed_mono: dict[str, float] = {}
        # Soft resyncs are deliberately backgrounded and coalesced per token.
        # Keep the last levels for inspection, but suspect depth cannot execute.
        self._resync_tasks: dict[str, asyncio.Task[None]] = {}
        self._last_bba: dict[str, tuple[Decimal | None, Decimal | None]] = {}
        # Auxiliary BBA frames may race snapshots/deltas. Avoid REST storms.
        self._last_soft_resync_mono: dict[str, float] = {}
        # ~100 CLOB messages/s must not perform two extra SQLite commits each.
        self._last_progress_us: int | None = None
        self._last_healthy_us: int | None = None
        self._startup_constraints_errors: dict[str,str] = {}
        self.readiness=ReadinessTimeline(started_us=now_epoch_us())

    def _details(self) -> dict[str, Any]:
        self.readiness.observe(self.market,self.states,at_us=now_epoch_us())
        tokens: dict[str, Any] = {}
        for token_id, state in self.states.items():
            bid = state.best_bid
            ask = state.best_ask
            tokens[state.outcome] = {
                "token_id": token_id,
                "synchronized": state.synchronized,
                "depth_reliable": state.depth_reliable,
                "depth_integrity": state.depth_integrity,
                "integrity_reason": "TRANSPORT_LOSS" if self._reader_failed.is_set() else state.integrity_reason,
                "depth_age_seconds": state.age_seconds(now_epoch_us()),
                "sync_generation": state.sync_generation,
                "bid_levels": len(state.bids),
                "ask_levels": len(state.asks),
                "best_bid": str(bid.price) if bid else None,
                "best_ask": str(ask.price) if ask else None,
                "tick_size": str(state.tick_size) if state.tick_size is not None else None,
                "min_order_size": str(state.min_order_size) if state.min_order_size is not None else None,
                "last_server_ts_us": state.last_server_ts_us,
                "last_verified_at_us": state.last_verified_at_us,
                "last_received_at_us": state.last_received_at_us,
                "last_bba_received_at_us": state.last_bba_received_at_us,
                "last_bba_server_ts_us": state.last_bba_server_ts_us,
                "last_tick_received_at_us": state.last_tick_received_at_us,
            }
        return {
            "feed_policy_revision": FEED_POLICY_REVISION,
            "collector_session_id": self.session_id,
            "forward_session_id": self.forward_session_id,
            "slug": self.market.slug,
            "reconnect_count": self.counters.reconnect_count,
            "disconnect_causes": dict(self.counters.disconnect_causes),
            "heartbeat_pings": self.counters.heartbeat_pings,
            "heartbeat_pongs": self.counters.heartbeat_pongs,
            "gap_duration_us": self.counters.gap_duration_us,
            "open_gap_age_us": (None if self._gap_started_us is None else max(0, now_epoch_us()-self._gap_started_us)),
            "tokens": tokens,
            "raw_messages": self.counters.raw_messages,
            "book_snapshots": self.counters.book_snapshots,
            "book_deltas": self.counters.book_deltas,
            "trades": self.counters.trades,
            "ignored_unsynced_deltas": self.counters.ignored_unsynced_deltas,
            "bba_mismatches": self.counters.bba_mismatches,
            "resync_requests": self.counters.resync_requests,
            "resync_failures": self.counters.resync_failures,
            "resync_successes": self.counters.resync_successes,
            "resync_superseded": self.counters.resync_superseded,
            "receive_queue_peak": self.counters.receive_queue_peak,
            "receive_queue_overflows": self.counters.receive_queue_overflows,
            "receive_processing_peak_ms": round(self.counters.receive_processing_peak_ms, 3),
            "receive_backlog_peak_ms": round(self.counters.receive_backlog_peak_ms, 3),
            "soft_resync_suppressed": self.counters.soft_resync_suppressed,
            "resync_failure_causes": dict(self.counters.resync_failure_causes),
            "resync_rate_limited":self.counters.resync_rate_limited,
            'clock_correlation':self.clock_correlation.snapshot(),
            "resync_inflight": sorted(token for token, task in self._resync_tasks.items() if not task.done()),
            "startup_constraints_errors": dict(self._startup_constraints_errors),
            "startup_readiness": self.readiness.snapshot(),
            "source_ahead_of_receive_events": self.counters.source_ahead_of_receive_events,
            "source_ahead_peak_us": self.counters.source_ahead_peak_us,
            "clock_scope": "Source/receive mismatch is diagnostic; no time offset or freshness tolerance applied",
        }

    def _publish_status(self, state: str) -> None:
        self.store.set_status(
            component=COMPONENT,
            session_id=self.session_id,
            slug=self.market.slug,
            state=state,
            updated_at_us=now_epoch_us(),
            last_message_at_us=self.last_message_at_us,
            last_book_at_us=self.last_book_at_us,
            reconnect_count=self.counters.reconnect_count,
            error_count=self.counters.error_count,
            details=self._details(),
        )

    def _publish_progress_if_due(self, received_at_us: int) -> None:
        """Rate-limit status/heartbeat writes, never raw messages or book deltas.

        This is intentionally only monitoring I/O: the stream's causal receives,
        L2 state updates, and raw archival remain per-message and unchanged.
        """
        if self._last_healthy_us is None or received_at_us - self._last_healthy_us >= 1_000_000:
            self.store.mark_healthy(self.session_id, received_at_us)
            self._last_healthy_us = received_at_us
        if self._last_progress_us is None or received_at_us - self._last_progress_us >= 1_000_000:
            self._publish_status("LIVE" if all(st.depth_reliable for st in self.states.values()) else "SYNCING")
            self._last_progress_us = received_at_us

    def _recover_if_ready(self, received_at_us: int) -> None:
        self.readiness.observe(self.market,self.states,at_us=now_epoch_us())
        if not all(st.depth_reliable for st in self.states.values()):
            return
        for gap_id in self._open_gap_ids:
            self.store.recover_gap(gap_id, received_at_us)
        self._open_gap_ids.clear()
        if self._gap_started_us is not None:
            self.counters.gap_duration_us += max(0, received_at_us-self._gap_started_us)
            self._gap_started_us = None

    def _depth_gap(self, token_id: str, reason: str, received_at_us: int) -> None:
        self.states[token_id].mark_suspect(reason)
        if self._gap_started_us is None:
            self._gap_started_us = received_at_us
            self._open_gap_ids.append(self.store.record_gap(
                component=COMPONENT, slug=self.market.slug, detected_at_us=received_at_us,
                reason=reason, details={"token_id": token_id, "collector_session_id": self.session_id,
                                        "forward_session_id": self.forward_session_id}))

    def _validate_rest(self, token_id: str, payload: dict) -> None:
        if self._token_id(payload) != token_id:
            raise RuntimeError("REST_ASSET_MISMATCH")
        if not isinstance(payload.get("bids"), list) or not isinstance(payload.get("asks"), list):
            raise RuntimeError("REST_UNAVAILABLE")

    async def _prime_rest_snapshots(self) -> None:
        """Capture public REST books/constraints, but do not treat them as WS synchronization."""
        for token_id, state in self.states.items():
            payload = await self.rest.get_order_book(token_id)
            self._validate_rest(token_id, payload)
            received_at_us = now_epoch_us()
            server_ts_us = parse_source_timestamp_us(payload.get("timestamp"))
            if server_ts_us is None or server_ts_us>received_at_us:
                # REST and WS clocks are not a common sequence number. A
                # future-dated response must not poison the WS depth watermark.
                raise RuntimeError("REST_FUTURE_SOURCE_TIME" if server_ts_us is not None else "REST_SOURCE_TIME_MISSING")
            self.receive_sequence += 1
            state.apply_snapshot(
                verified_rest=True,
                bids=payload.get("bids") or [],
                asks=payload.get("asks") or [],
                received_at_us=received_at_us,
                server_ts_us=server_ts_us,
                book_hash=payload.get("hash"),
                tick_size=payload.get("tick_size") or payload.get("tickSize"),
                min_order_size=payload.get("min_order_size") or payload.get("minOrderSize"),
                neg_risk=payload.get("neg_risk") if "neg_risk" in payload else payload.get("negRisk"),
                last_trade_price=payload.get("last_trade_price") or payload.get("lastTradePrice"),
            )
            raw_ref = self._raw.write(received_at_us, {"source": "REST_BOOK", "payload": payload})
            self.store.persist_book_checkpoint(
                state,
                received_at_us=received_at_us,
                server_ts_us=server_ts_us,
                receive_sequence=self.receive_sequence,
                raw_payload=raw_ref,
            )
            self.last_book_at_us = received_at_us
        # A REST snapshot is a valid observation, but after opening a new WS session we wait
        # for a stream snapshot (or explicit REST resync) before applying stream deltas.
        for state in self.states.values():
            state.mark_unsynchronized()

    async def _prime_rest_constraints(self) -> None:
        """Fetch independent token metadata without delaying WS or replacing depth.

        Stream snapshots establish depth; a bounded REST fallback can recover it.
        A late metadata response cannot overwrite constraints already learned from
        a newer stream observation. Metadata never refreshes depth timestamps.
        """
        async def one(token_id,state):
            try:
                payload=await asyncio.wait_for(self.rest.get_order_book(token_id),timeout=2.5)
                received=now_epoch_us();self._validate_rest(token_id,payload)
                tick=self._parse_optional_decimal(payload.get('tick_size') or payload.get('tickSize'))
                minimum=self._parse_optional_decimal(payload.get('min_order_size') or payload.get('minOrderSize'))
                if tick is None or minimum is None or not tick.is_finite() or not minimum.is_finite() or tick<=0 or minimum<=0:
                    raise ValueError('STARTUP_CONSTRAINTS_MISSING_OR_INVALID')
                if state.tick_size is None:state.tick_size=tick
                if state.min_order_size is None:state.min_order_size=minimum
                raw_ref=self._raw.write(received,{'source':'REST_STARTUP_CONSTRAINTS_ONLY','payload':payload})
                self.store.update_tick_size(state,source_ts_us=parse_source_timestamp_us(payload.get('timestamp')),
                    received_at_us=received,raw_payload=raw_ref)
                self.readiness.observe(self.market,self.states,at_us=now_epoch_us())
            except (httpx.HTTPError,ValueError,RuntimeError,asyncio.TimeoutError):
                self._startup_constraints_errors[state.outcome]='STARTUP_CONSTRAINTS_UNAVAILABLE'
                self.counters.error_count+=1;self.store.bump_error(self.session_id)
        await asyncio.gather(*(one(token,state) for token,state in self.states.items()))

    async def _rest_resync(self, token_id: str, reason: str) -> None:
        """Transactionally replace one token book from REST.

        Crucially, a good local book remains usable while the REST request is in
        flight.  We only replace it after a complete response arrives.  This
        prevents the previous READY/UNSYNC flapping caused by setting
        ``synchronized=False`` before awaiting public HTTP.
        """
        state = self.states[token_id]
        generation = self._connection_generation
        baseline_generation = state.sync_generation
        baseline_revision = state.mutation_revision
        detected = now_epoch_us()
        gap_id = self.store.record_gap(
            component=COMPONENT,
            slug=self.market.slug,
            detected_at_us=detected,
            reason=reason,
            details={"token_id": token_id, "outcome": state.outcome, "collector_session_id": self.session_id,
                     "forward_session_id": self.forward_session_id},
        )
        try:
            payload = await asyncio.wait_for(self.rest.get_order_book(token_id), timeout=2.5)
            received_at_us=now_epoch_us();received_mono_ns=time.monotonic_ns()
            if generation != self._connection_generation or self._reader_failed.is_set():
                raise RuntimeError("REST_CONNECTION_SUPERSEDED")
            self._validate_rest(token_id, payload)
            if state.sync_generation != baseline_generation or state.mutation_revision != baseline_revision:
                # A full WS baseline arrived while HTTP was in flight. Preserve
                # it and the intervening deltas; no cross-transport order is proven.
                self._raw.write(received_at_us, {"source": "REST_RESYNC_SUPERSEDED", "payload": payload})
                self.counters.resync_superseded += 1
                if state.depth_reliable:
                    self.store.recover_gap(gap_id, received_at_us)
                else:
                    self._open_gap_ids.append(gap_id)
                return
            server_ts_us = parse_source_timestamp_us(payload.get("timestamp"))
            self.clock_correlation.observe(received_us=received_at_us,received_mono_ns=received_mono_ns,
                processed_us=now_epoch_us(),processed_mono_ns=time.monotonic_ns(),source_us=server_ts_us,transport='REST')
            bids = payload.get("bids") or []
            if server_ts_us is None or server_ts_us>received_at_us:
                raise RuntimeError("REST_FUTURE_SOURCE_TIME" if server_ts_us is not None else "REST_SOURCE_TIME_MISSING")
            asks = payload.get("asks") or []

            # If the optional best-bid/ask stream says both sides exist but a
            # transient REST response is empty, do not destroy a known-good book.
            advertised = self._last_bba.get(token_id)
            # An older auxiliary quote cannot contradict a newer full snapshot.
            # Unknown/same/newer quote times retain the conservative veto.
            if advertised is not None and (state.last_bba_server_ts_us is None or server_ts_us<=state.last_bba_server_ts_us):
                advertised_bid, advertised_ask = advertised
                if advertised_bid is not None and advertised_ask is not None and (not bids or not asks):
                    raise RuntimeError("REST_BOOK_EMPTY_WHILE_BBA_NONEMPTY")
            if (
                server_ts_us is not None
                and state.last_server_ts_us is not None
                and server_ts_us < state.last_server_ts_us
            ):
                raise RuntimeError("STALE_REST_SNAPSHOT")

            self.receive_sequence += 1
            state.apply_snapshot(
                verified_rest=True,
                bids=bids,
                asks=asks,
                received_at_us=received_at_us,
                server_ts_us=server_ts_us,
                book_hash=payload.get("hash"),
                tick_size=payload.get("tick_size") or payload.get("tickSize"),
                min_order_size=payload.get("min_order_size") or payload.get("minOrderSize"),
                neg_risk=payload.get("neg_risk") if "neg_risk" in payload else payload.get("negRisk"),
                last_trade_price=payload.get("last_trade_price") or payload.get("lastTradePrice"),
            )
            raw_ref = self._raw.write(
                received_at_us, {"source": "REST_RESYNC", "reason": reason, "payload": payload}
            )
            self.store.persist_book_checkpoint(
                state,
                received_at_us=received_at_us,
                server_ts_us=server_ts_us,
                receive_sequence=self.receive_sequence,
                raw_payload=raw_ref,
            )
            self.store.recover_gap(gap_id, received_at_us)
            self.last_book_at_us = received_at_us
            self.counters.resync_successes += 1
            self._soft_failure_streak.pop(token_id, None)
            self._soft_next_allowed_mono.pop(token_id, None)
            self._recover_if_ready(received_at_us)
        except Exception as exc:
            self.counters.resync_failures += 1
            streak = min(10, self._soft_failure_streak.get(token_id, 0) + 1)
            self._soft_failure_streak[token_id] = streak
            self._soft_next_allowed_mono[token_id] = time.monotonic() + min(120.0, 3.0 * (2 ** min(streak - 1, 6)))
            cause = type(exc).__name__
            if isinstance(exc, (TimeoutError, httpx.TimeoutException)):
                cause = "TIMEOUT"
            elif isinstance(exc, httpx.HTTPStatusError):
                cause = "REST_RATE_LIMIT" if exc.response.status_code == 429 else "REST_UNAVAILABLE"
                if exc.response.status_code == 429:
                    try:
                        delay = float(exc.response.headers.get("Retry-After", "0"))
                        if 0 < delay < float("inf"):
                            self._soft_next_allowed_mono[token_id] = max(
                                self._soft_next_allowed_mono[token_id], time.monotonic()+delay)
                    except ValueError:
                        pass
            elif str(exc) in {"STALE_REST_SNAPSHOT", "REST_BOOK_EMPTY_WHILE_BBA_NONEMPTY",
                              "REST_ASSET_MISMATCH", "REST_UNAVAILABLE", "REST_CONNECTION_SUPERSEDED",
                              "REST_FUTURE_SOURCE_TIME", "REST_SOURCE_TIME_MISSING"}:
                cause = str(exc)
            self.counters.resync_failure_causes[cause] = self.counters.resync_failure_causes.get(cause, 0) + 1
            failure_at_us = now_epoch_us()
            # A soft resync is an integrity-check ATTEMPT, not an L2 outage.
            # Close its hint even if REST fails while a valid WS book remains.
            # Truly unsynchronized books continue to fail closed and retain an
            # open gap until a valid baseline arrives.
            if state.depth_reliable:
                self.store.recover_gap(gap_id, failure_at_us)
            else:
                self._open_gap_ids.append(gap_id)
                if self._gap_started_us is None:
                    self._gap_started_us = detected
            failure_id = self.store.record_gap(
                component=COMPONENT, slug=self.market.slug,
                detected_at_us=failure_at_us, reason="REST_RESYNC_FAILED",
                details={"trigger": reason, "token_id": token_id,
                         "collector_session_id": self.session_id, "forward_session_id": self.forward_session_id,
                         "exception": type(exc).__name__, "cause": cause,
                         "message": str(exc)[:300],
                         "still_synchronized": state.synchronized},
            )
            if state.depth_reliable:
                self.store.recover_gap(failure_id, failure_at_us)
            else:
                self._open_gap_ids.append(failure_id)
            # Preserve the last good synchronized state.  If there never was a
            # good baseline the token remains unsynchronized and the startup
            # fallback will retry on later evidence.
            self._raw.write(now_epoch_us(), {
                "source": "REST_RESYNC_FAILED",
                "reason": reason,
                "token_id": token_id,
                "exception": type(exc).__name__,
                "message": str(exc),
            })

    def _schedule_resync(self, token_id: str, reason: str) -> None:
        if token_id not in self.states or self._stop_requested.is_set():
            return
        existing = self._resync_tasks.get(token_id)
        if existing is not None and not existing.done():
            return
        if time.monotonic() < self._soft_next_allowed_mono.get(token_id, 0.0):
            self.counters.soft_resync_suppressed += 1
            self.counters.resync_rate_limited += 1
            return
        if reason == "TOP_OF_BOOK_MISMATCH_SOFT":
            now_mono = time.monotonic()
            previous = self._last_soft_resync_mono.get(token_id)
            next_allowed = self._soft_next_allowed_mono.get(token_id, 0.0)
            if now_mono < next_allowed or (previous is not None and now_mono - previous < 3.0):
                self.counters.soft_resync_suppressed += 1
                return
            self._last_soft_resync_mono[token_id] = now_mono
        # Bound every integrity-repair path, including rapid failed REST calls
        # after clock-inconsistent depth. Raw WS admission remains fail-closed.
        self._soft_next_allowed_mono[token_id]=max(self._soft_next_allowed_mono.get(token_id,0),time.monotonic()+1.0)
        self.counters.resync_requests += 1

        async def runner() -> None:
            try:
                if (reason in {"DELTA_BEFORE_SYNCHRONIZED_SNAPSHOT", "TOP_OF_BOOK_BEFORE_SYNCHRONIZED_SNAPSHOT",
                               "STARTUP_WS_SNAPSHOT_TIMEOUT","SOURCE_TIME_MISSING","SOURCE_TIME_AFTER_RECEIPT"} and self.states[token_id].depth_reliable):
                    return  # A queued request became unnecessary after a WS baseline.
                await self._rest_resync(token_id, reason)
            finally:
                self._resync_tasks.pop(token_id, None)

        self._resync_tasks[token_id] = asyncio.create_task(runner())

    async def _startup_snapshot_fallback(self, delay_seconds: float = 1.25) -> None:
        """Give the documented WS ``book`` snapshot first chance to establish L2.

        Polymarket's market channel sends a full ``book`` on subscription.  REST
        is therefore a fallback, not a competing baseline racing the initial WS
        snapshot.
        """
        await asyncio.sleep(delay_seconds)
        for token_id, state in self.states.items():
            if not state.depth_reliable:
                self._schedule_resync(token_id, "STARTUP_WS_SNAPSHOT_TIMEOUT")

    async def _post_subscribe_resync(self) -> None:
        """Explicit synchronous fallback retained for probes/tests.

        Production WS startup no longer calls this method directly; it consumes
        the documented stream snapshot first and uses
        :meth:`_startup_snapshot_fallback` only if needed.
        """
        for token_id, state in self.states.items():
            if not state.synchronized:
                await self._rest_resync(token_id, "POST_SUBSCRIBE_INITIAL_SYNC")

    @staticmethod
    def _parse_optional_decimal(value: object) -> Decimal | None:
        if value is None or str(value).strip() == "":
            return None
        try:
            return Decimal(str(value))
        except (InvalidOperation, ValueError):
            return None

    @staticmethod
    def _event_type(event: dict[str, Any]) -> str | None:
        return event.get("event_type") or event.get("type")

    @staticmethod
    def _payload(event: dict[str, Any]) -> dict[str, Any]:
        payload = event.get("payload")
        return payload if isinstance(payload, dict) else event

    @staticmethod
    def _token_id(payload: dict[str, Any]) -> str | None:
        value = payload.get("asset_id") or payload.get("token_id") or payload.get("tokenId")
        return str(value) if value is not None else None

    @staticmethod
    def _prices_equal(left: object, right: object) -> bool:
        """Compare CLOB prices numerically so formatting differences do not trigger resync."""
        try:
            return Decimal(str(left)) == Decimal(str(right))
        except (InvalidOperation, ValueError):
            return str(left).strip() == str(right).strip()

    async def _process_event(
        self,
        event: dict[str, Any],
        received_at_us: int,
        raw_ref: dict[str, object] | None = None,
    ) -> None:
        if raw_ref is None:
            # Unit/probe callers may inject parsed events directly. Production
            # websocket/REST paths always provide a full-fidelity archive ref.
            raw_ref = {
                "storage": "direct_event_no_archive_ref",
                "component": COMPONENT,
                "received_at_us": int(received_at_us),
            }
        event_type = self._event_type(event)
        payload = self._payload(event)
        server_ts_us = parse_source_timestamp_us(payload.get("timestamp") or event.get("timestamp"))
        if self._message_timing is not None:
            self.clock_correlation.observe(received_us=received_at_us,received_mono_ns=self._message_timing['received_mono_ns'],
                processed_us=now_epoch_us(),processed_mono_ns=time.monotonic_ns(),source_us=server_ts_us,transport='WS')
        if server_ts_us is not None and server_ts_us>received_at_us:
            self.counters.source_ahead_of_receive_events+=1
            self.counters.source_ahead_peak_us=max(self.counters.source_ahead_peak_us,server_ts_us-received_at_us)
        if event_type in {'book','price_change','tick_size_change'} and (server_ts_us is None or server_ts_us>received_at_us):
            if event_type in {'book','tick_size_change'}:
                tokens=[self._token_id(payload)]
            else:
                changes=payload.get('price_changes',payload.get('priceChanges'))
                tokens=(list(self.states) if not isinstance(changes,list) or not changes or any(not isinstance(c,dict) for c in changes)
                    else [self._token_id(c) for c in changes])
            reason='SOURCE_TIME_MISSING' if server_ts_us is None else 'SOURCE_TIME_AFTER_RECEIPT'
            for token in set(tokens)&self.states.keys():
                self._depth_gap(token,reason,received_at_us)
                self._schedule_resync(token,reason)
            return  # Never relabel the source clock or revive this event later.
        if event_type=='best_bid_ask' and (server_ts_us is None or server_ts_us>received_at_us):
            return  # An unverifiable auxiliary hint cannot validate/invalidate depth.

        if event_type == "book":
            token_id = self._token_id(payload)
            if token_id not in self.states:
                return
            state = self.states[token_id]
            if not isinstance(payload.get("bids"), list) or not isinstance(payload.get("asks"), list):
                state.mark_suspect("INCOMPLETE_SNAPSHOT")
                raise ValueError("INCOMPLETE_SNAPSHOT")
            if (
                server_ts_us is not None
                and state.last_server_ts_us is not None
                and server_ts_us < state.last_server_ts_us
            ):
                # A buffered snapshot older than our current causal baseline must
                # never roll the local book backwards.
                return
            state.apply_snapshot(
                bids=payload.get("bids") or [],
                verified_at_us=now_epoch_us(),
                asks=payload.get("asks") or [],
                received_at_us=received_at_us,
                server_ts_us=server_ts_us,
                book_hash=payload.get("hash"),
                tick_size=payload.get("tick_size") or payload.get("tickSize"),
                min_order_size=payload.get("min_order_size") or payload.get("minOrderSize"),
                neg_risk=payload.get("neg_risk") if "neg_risk" in payload else payload.get("negRisk"),
                last_trade_price=payload.get("last_trade_price") or payload.get("lastTradePrice"),
            )
            self.store.persist_book_checkpoint(
                state,
                received_at_us=received_at_us,
                server_ts_us=server_ts_us,
                receive_sequence=self.receive_sequence,
                raw_payload=raw_ref,
            )
            self.counters.book_snapshots += 1
            self.last_book_at_us = received_at_us
            self._recover_if_ready(received_at_us)
            return

        if event_type == "price_change":
            changes = payload.get("price_changes", payload.get("priceChanges"))
            if not isinstance(changes, list) or any(not isinstance(change, dict) for change in changes):
                for token_id in self.states:
                    self._depth_gap(token_id, "INVALID_DEPTH_EVENT", received_at_us)
                raise ValueError("INVALID_DEPTH_EVENT")
            with self.store.coalesced_quotes():
                for change in changes:
                    if not isinstance(change, dict):
                        continue
                    token_id = self._token_id(change)
                    if token_id not in self.states:
                        continue
                    state = self.states[token_id]
                    if not state.depth_reliable:
                        self.counters.ignored_unsynced_deltas += 1
                        self._schedule_resync(token_id, "DELTA_BEFORE_SYNCHRONIZED_SNAPSHOT")
                        continue
                    applied = state.apply_price_change(
                        verified_at_us=now_epoch_us(),
                        order_side=str(change.get("side") or ""),
                        price=str(change.get("price")),
                        size=str(change.get("size")),
                        received_at_us=received_at_us,
                        server_ts_us=server_ts_us,
                        change_hash=str(change.get("hash")) if change.get("hash") is not None else None,
                    )
                    if applied:
                        self.store.persist_book_delta(
                            state,
                            received_at_us=received_at_us,
                            server_ts_us=server_ts_us,
                            receive_sequence=self.receive_sequence,
                            delta=change,
                            raw_payload=raw_ref,
                        )
                        self.counters.book_deltas += 1
                        self.last_book_at_us = received_at_us
                    if not state.depth_reliable:
                        self._depth_gap(token_id, state.integrity_reason or "DEPTH_UNRELIABLE", received_at_us)
                        self._schedule_resync(token_id, state.integrity_reason or "DEPTH_UNRELIABLE")
            return

        if event_type == "best_bid_ask":
            token_id = self._token_id(payload)
            if token_id not in self.states:
                return
            state = self.states[token_id]
            # An older auxiliary observation neither verifies nor contradicts
            # a newer depth baseline. It must not trigger a reconciliation storm.
            if server_ts_us is not None and (
                (state.last_bba_server_ts_us is not None and server_ts_us < state.last_bba_server_ts_us)
                or (state.last_server_ts_us is not None and server_ts_us < state.last_server_ts_us)
            ):
                return
            state.mark_verified(received_at_us, server_ts_us)
            advertised_bid = payload.get("best_bid") or payload.get("bestBid")
            advertised_ask = payload.get("best_ask") or payload.get("bestAsk")
            self._last_bba[token_id] = (
                self._parse_optional_decimal(advertised_bid),
                self._parse_optional_decimal(advertised_ask),
            )
            if not state.synchronized:
                self._schedule_resync(token_id, "TOP_OF_BOOK_BEFORE_SYNCHRONIZED_SNAPSHOT")
                return
            local_bid = state.best_bid
            local_ask = state.best_ask
            bid_mismatch = advertised_bid is not None and (
                local_bid is None or not self._prices_equal(local_bid.price, advertised_bid)
            )
            ask_mismatch = advertised_ask is not None and (
                local_ask is None or not self._prices_equal(local_ask.price, advertised_ask)
            )
            if bid_mismatch or ask_mismatch:
                # ``best_bid_ask`` is an auxiliary custom-feature event.  It can
                # race adjacent price_change/book frames and has no sequence number.
                # Preserve levels for diagnosis; only a full baseline can
                # establish reliable depth again after contradictory evidence.
                self.counters.bba_mismatches += 1
                self._depth_gap(token_id, "TOP_OF_BOOK_MISMATCH", received_at_us)
                self._schedule_resync(token_id, "TOP_OF_BOOK_MISMATCH_SOFT")
            else:
                state.mark_verified(received_at_us, server_ts_us)
            return

        if event_type == "last_trade_price":
            token_id = self._token_id(payload)
            price = payload.get("price")
            size = payload.get("size")
            if price is None or size is None:
                return
            self.store.persist_trade(
                self.market,
                token_id=token_id,
                source_ts_us=server_ts_us,
                received_at_us=received_at_us,
                price=str(price),
                size=str(size),
                taker_side=str(payload.get("side")) if payload.get("side") is not None else None,
                source_trade_id=str(payload.get("transaction_hash") or payload.get("transactionHash") or "") or None,
                raw_payload=raw_ref,
            )
            self.counters.trades += 1
            return

        if event_type == "tick_size_change":
            token_id = self._token_id(payload)
            if token_id not in self.states:
                return
            new_tick = payload.get("new_tick_size") or payload.get("newTickSize")
            if new_tick is None:
                return
            state = self.states[token_id]
            if not state.apply_tick_size(str(new_tick), received_at_us, server_ts_us):
                return
            self.store.update_tick_size(
                state,
                source_ts_us=server_ts_us,
                received_at_us=received_at_us,
                raw_payload=raw_ref,
            )
            return

    def _socket_reader(self, out: queue.Queue, stopped: threading.Event, wake: asyncio.Event,
                       loop: asyncio.AbstractEventLoop) -> None:
        """Network-only thread. Main event loop owns all book/SQLite/archive mutations.

        Blocking disk writes can no longer prevent this thread from draining the
        WebSocket server's send buffer. On bounded queue overflow we disconnect
        and require a new authoritative snapshot; no buffered message is silently
        interpreted as a complete book after an overflow.
        """
        def enqueue(item: tuple[object, ...]) -> None:
            try:
                out.put_nowait(item)
            except queue.Full as exc:
                self.counters.receive_queue_overflows += 1
                self._reader_failed.set()
                raise RuntimeError("WS_READER_QUEUE_OVERFLOW") from exc
            self.counters.receive_queue_peak = max(self.counters.receive_queue_peak, out.qsize())
            loop.call_soon_threadsafe(wake.set)

        for state in self.states.values():
            state.transport_fault = self._reader_failed
        try:
            with ws_sync_connect(
                self.ws_url,
                ping_interval=None, close_timeout=3, open_timeout=7,
                max_size=None, max_queue=512,
            ) as ws:
                ws.send(json.dumps({
                    "assets_ids": list(self.market.token_ids),
                    "type": "market", "custom_feature_enabled": True,
                }, separators=(",", ":")))
                enqueue(("connected",))
                last_ping = time.monotonic()
                waiting_since: float | None = None
                while not stopped.is_set():
                    now_mono = time.monotonic()
                    if waiting_since is not None and now_mono-waiting_since >= 3*self.heartbeat_seconds:
                        raise RuntimeError("HEARTBEAT_TIMEOUT")
                    if now_mono-last_ping >= self.heartbeat_seconds:
                        ws.send("PING")
                        self.counters.heartbeat_pings += 1
                        last_ping = now_mono
                        if waiting_since is None:
                            waiting_since = now_mono
                    try:
                        raw_message = ws.recv(timeout=min(0.25, self.heartbeat_seconds))
                    except TimeoutError:
                        continue
                    if raw_message is None:
                        raise ConnectionError("Polymarket WebSocket closed without final message")
                    if raw_message in ("PONG", b"PONG"):
                        self.counters.heartbeat_pongs += 1
                        waiting_since = None
                    enqueue(("message", now_epoch_us(), raw_message, time.monotonic_ns()))
        except Exception as exc:
            self._reader_failed.set()
            # Terminal error must be observed even if the queue was full.
            while not stopped.is_set():
                try:
                    out.put(("error", exc), timeout=0.1)
                    loop.call_soon_threadsafe(wake.set)
                    break
                except queue.Full:
                    continue
        finally:
            try:
                out.put(("end",), timeout=1.0)
                loop.call_soon_threadsafe(wake.set)
            except queue.Full:
                pass

    async def _run_connection(self) -> None:
        if self._reader_thread is not None and self._reader_thread.is_alive():
            raise RuntimeError("WS_PREVIOUS_READER_STILL_ACTIVE")
        # No response from an old connection may heal a new generation.
        pending = list(self._resync_tasks.values())
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        self._connection_generation += 1
        self._last_bba.clear()
        for state in self.states.values():
            state.mark_unsynchronized()
            state.transport_fault = self._reader_failed
        self._reader_failed.clear()
        self._publish_status("CONNECTING")
        buffer: queue.Queue[tuple[object, ...]] = queue.Queue(maxsize=self._ws_queue_capacity)
        stopped = threading.Event()
        self._ws_reader_stop = stopped
        wake = asyncio.Event()
        loop = asyncio.get_running_loop()
        reader = threading.Thread(
            target=self._socket_reader,
            args=(buffer, stopped, wake, loop),
            name="clob-ws-network-receiver", daemon=True,
        )
        self._reader_thread = reader
        reader.start()
        startup_fallback: asyncio.Task | None = None
        processed_count = 0
        slice_started = time.monotonic()
        try:
            while True:
                try:
                    item = buffer.get_nowait()
                except queue.Empty:
                    wake.clear()
                    if not buffer.empty():
                        continue
                    await wake.wait()
                    continue
                kind = item[0]
                if kind == "connected":
                    self._publish_status("SYNCING")
                    startup_fallback = asyncio.create_task(self._startup_snapshot_fallback())
                    continue
                if kind == "error":
                    if not self._stop_requested.is_set():
                        raise item[1]
                    continue
                if kind == "end":
                    break
                received_at_us, raw_message = item[1], item[2]
                received_mono_ns=item[3] if len(item)>3 else None
                assert isinstance(received_at_us, int)
                utc_age_us=now_epoch_us()-received_at_us
                age_us=(time.monotonic_ns()-received_mono_ns)//1000 if received_mono_ns is not None else utc_age_us
                self.counters.receive_backlog_peak_ms = max(
                    self.counters.receive_backlog_peak_ms, age_us / 1000.0
                )
                if age_us > self._ws_backlog_max_age_us:
                    self._reader_failed.set()
                    raise RuntimeError(
                        f"WS_READER_BACKLOG_STALE: age_ms={age_us/1000.0:.1f} "
                        f"queued={buffer.qsize()}, fail closed and resubscribe"
                    )
                if age_us < 0 or utc_age_us < 0:
                    self._reader_failed.set()
                    raise RuntimeError("WS_READER_RECEIVE_IN_FUTURE")
                began = time.monotonic()
                self._message_timing=None if received_mono_ns is None else dict(received_mono_ns=received_mono_ns,
                    processed_at_us=now_epoch_us(),processed_mono_ns=time.monotonic_ns(),connection_generation=self._connection_generation)
                self.last_message_at_us = received_at_us
                self._publish_progress_if_due(received_at_us)
                raw_text = raw_message.decode("utf-8", errors="replace") if isinstance(raw_message, bytes) else str(raw_message)
                if raw_text.upper() != "PONG":
                    self.counters.raw_messages += 1
                    try:
                        decoded = json.loads(raw_text)
                    except json.JSONDecodeError:
                        self._raw.write(received_at_us, {"unparsed": raw_text})
                        raise RuntimeError("WS_MALFORMED_FRAME")
                    else:
                        raw_ref = self._raw.write(received_at_us, decoded,timing=self._message_timing)
                        for event in decoded if isinstance(decoded, list) else [decoded]:
                            if isinstance(event, dict):
                                self.receive_sequence += 1
                                await self._process_event(event, received_at_us, raw_ref)
                                if time.monotonic()-slice_started >= 0.004:
                                    await asyncio.sleep(0)
                                    slice_started = time.monotonic()
                self.counters.receive_processing_peak_ms = max(
                    self.counters.receive_processing_peak_ms, (time.monotonic() - began) * 1000.0
                )
                processed_count += 1
                if processed_count % 32 == 0 or time.monotonic()-slice_started >= 0.004:
                    await asyncio.sleep(0)  # allow settlement/model and REST tasks to run
                    slice_started = time.monotonic()
        finally:
            self._reader_failed.set()
            stopped.set()
            if startup_fallback is not None:
                startup_fallback.cancel()
                await asyncio.gather(startup_fallback, return_exceptions=True)
            await asyncio.to_thread(reader.join, 4.0)
            if not reader.is_alive():
                self._ws_reader_stop = None
                self._reader_thread = None
        if not self._stop_requested.is_set():
            raise RuntimeError("Polymarket WebSocket closed unexpectedly")

    async def request_stop(self) -> None:
        """Request a graceful stop without cancelling in-flight book verification/resync work."""
        self._stop_requested.set()
        if self._ws_reader_stop is not None:
            self._ws_reader_stop.set()
        ws = self._active_ws
        if ws is not None:
            try:
                await ws.close(code=1000, reason="planned collector stop")
            except Exception:
                # The run loop owns connection-error accounting. A close failure here must not
                # turn a planned finite probe into a cancellation race.
                pass

    @staticmethod
    def _disconnect_cause(exc: BaseException) -> tuple[str, int | None]:
        received = getattr(exc, "rcvd", None)
        close_code = getattr(received, "code", None)
        if close_code == 1013:
            return "REMOTE_1013", close_code
        for prefix, cause in (("WS_READER_QUEUE_OVERFLOW", "QUEUE_OVERFLOW"),
                              ("WS_READER_BACKLOG_STALE", "STALE_BACKLOG"),
                              ("HEARTBEAT_TIMEOUT", "HEARTBEAT_TIMEOUT"),
                              ("WS_READER_RECEIVE_IN_FUTURE", "INVALID_RECEIVE_CLOCK"),
                              ("WS_PREVIOUS_READER_STILL_ACTIVE", "READER_SHUTDOWN_PENDING"),
                              ("WS_MALFORMED_FRAME", "MALFORMED_FRAME")):
            if str(exc).startswith(prefix):
                return cause, close_code
        return ("REMOTE_CLOSE" if close_code is not None else "TRANSPORT_ERROR"), close_code

    async def run(self) -> None:
        started_at_us = now_epoch_us()
        self._stop_requested.clear()
        self.store.start_session(self.session_id, COMPONENT, started_at_us)
        self.store.persist_market(self.market, started_at_us)
        fatal = False
        constraints_task=asyncio.create_task(self._prime_rest_constraints())
        try:
            backoff = 1.0
            while not self._stop_requested.is_set():
                try:
                    await self._run_connection()
                    backoff = 1.0
                except asyncio.CancelledError:
                    raise
                except KeyboardInterrupt:
                    raise
                except Exception as exc:
                    if self._stop_requested.is_set():
                        break
                    self.counters.error_count += 1
                    self.store.bump_error(self.session_id)
                    detected = now_epoch_us()
                    cause, close_code = self._disconnect_cause(exc)
                    self.counters.disconnect_causes[cause] = self.counters.disconnect_causes.get(cause, 0)+1
                    gap_id = self.store.record_gap(
                        component=COMPONENT,
                        slug=self.market.slug,
                        detected_at_us=detected,
                        reason="WEBSOCKET_DISCONNECTED",
                        details={"exception": type(exc).__name__, "message": str(exc)[:300],
                                 "cause": cause, "close_code": close_code,
                                 "collector_session_id": self.session_id,
                                 "forward_session_id": self.forward_session_id,
                                 "queue_peak": self.counters.receive_queue_peak,
                                 "overflows": self.counters.receive_queue_overflows,
                                 "backlog_peak_ms": self.counters.receive_backlog_peak_ms},
                    )
                    self._open_gap_ids.append(gap_id)
                    if self._gap_started_us is None:
                        self._gap_started_us = detected
                    for state in self.states.values():
                        state.mark_unsynchronized()
                    self._publish_status("RECONNECTING")
                    print(f"[WARN] Polymarket WS disconnected: {type(exc).__name__}: {exc}")
                    try:
                        await asyncio.wait_for(self._stop_requested.wait(), timeout=backoff)
                    except asyncio.TimeoutError:
                        pass
                    if self._stop_requested.is_set():
                        break
                    self.counters.reconnect_count += 1
                    self.store.bump_reconnect(self.session_id)
                    backoff = min(backoff * 2.0, 30.0)
                    # A retry is not recovery. Close gaps only after both books
                    # have acquired reliable baselines in this connection.
            self._publish_status("STOPPED")
        except asyncio.CancelledError:
            # External cancellation (for example process shutdown) remains fail-closed.
            self._publish_status("STOPPED")
            raise
        except KeyboardInterrupt:
            self._publish_status("STOPPED")
        except Exception:
            fatal = True
            self._publish_status("FATAL")
            raise
        finally:
            if not constraints_task.done():constraints_task.cancel()
            await asyncio.gather(constraints_task,return_exceptions=True)
            pending_resyncs = list(self._resync_tasks.values())
            self._resync_tasks.clear()
            for task in pending_resyncs:
                if not task.done():
                    task.cancel()
            if pending_resyncs:
                await asyncio.gather(*pending_resyncs, return_exceptions=True)
            ended_at_us = now_epoch_us()
            self.store.finish_session(self.session_id, ended_at_us, fatal=fatal)
            self._raw.close()

    async def run_for(self, seconds: float) -> None:
        if seconds <= 0:
            raise ValueError("seconds must be positive")
        task = asyncio.create_task(self.run())
        try:
            await asyncio.sleep(seconds)
        finally:
            # Do not cancel the collector at an arbitrary instruction boundary. A cancellation
            # can land after mark_unsynchronized() but before REST resync completes, leaving a
            # healthy book falsely marked unsynchronized in the final status.
            await self.request_stop()
            await task
