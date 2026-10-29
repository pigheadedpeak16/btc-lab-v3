from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import websockets

from ..storage.external_store import ExternalStore
from ..timeutil import now_epoch_us, parse_source_timestamp_us
from .raw_archive import RawJsonlArchive

COINBASE_WS_URL = "wss://ws-feed.exchange.coinbase.com"
COMPONENT = "coinbase_btc_usd"
PRODUCT = "BTC-USD"


@dataclass(frozen=True)
class CoinbaseTick:
    source_ts_us: int | None
    received_at_us: int
    price: str
    raw_payload: dict[str, Any]


def parse_coinbase_ticker(message: dict[str, Any], received_at_us: int) -> CoinbaseTick | None:
    if message.get("type") != "ticker" or message.get("product_id") != PRODUCT:
        return None
    price = message.get("price")
    if price is None:
        return None
    return CoinbaseTick(
        source_ts_us=parse_source_timestamp_us(message.get("time")),
        received_at_us=received_at_us,
        price=str(price),
        raw_payload=message,
    )


class CoinbaseCollector:
    """Public BTC-USD price collector. No Coinbase authentication or trading methods exist here."""

    def __init__(
        self,
        *,
        root: Path,
        store: ExternalStore,
        ws_url: str = COINBASE_WS_URL,
        reconnect_delay_seconds: float = 1.0,
    ) -> None:
        self.root = root
        self.store = store
        self.ws_url = ws_url
        self.reconnect_delay_seconds = reconnect_delay_seconds
        self.session_id = str(uuid.uuid4())
        self.receive_sequence = 0
        self.reconnect_count = 0
        self.error_count = 0
        self.tick_count = 0
        self.last_message_at_us: int | None = None
        self.last_tick_at_us: int | None = None
        self.last_tick_source_ts_us: int | None = None
        self._stop_requested = asyncio.Event()
        self._active_ws: Any | None = None
        self._raw = RawJsonlArchive(root / "data" / "raw", COMPONENT)

    def _status(self, state: str, **extra: object) -> None:
        observed_at_us = now_epoch_us()
        source_age = None if self.last_tick_source_ts_us is None else (
            observed_at_us - self.last_tick_source_ts_us
        ) / 1e6
        details = {"product": PRODUCT, "tick_count": self.tick_count,
                   "latest_source_ts_us":self.last_tick_source_ts_us,
                   "latest_source_tick_age_seconds": source_age, **extra}
        self.store.set_status(
            component=COMPONENT,
            session_id=self.session_id,
            state=state,
            updated_at_us=now_epoch_us(),
            last_message_at_us=self.last_message_at_us,
            last_tick_at_us=self.last_tick_at_us,
            reconnect_count=self.reconnect_count,
            error_count=self.error_count,
            details=details,
        )

    async def request_stop(self) -> None:
        self._stop_requested.set()
        ws = self._active_ws
        if ws is not None:
            await ws.close()

    async def run_for(self, seconds: float) -> None:
        task = asyncio.create_task(self.run())
        try:
            await asyncio.sleep(seconds)
        finally:
            await self.request_stop()
            await task

    async def run(self) -> None:
        started = now_epoch_us()
        self.store.start_session(self.session_id, COMPONENT, started)
        fatal = False
        try:
            first = True
            while not self._stop_requested.is_set():
                self._status("CONNECTING")
                try:
                    async with websockets.connect(
                        self.ws_url,
                        ping_interval=20,
                        ping_timeout=20,
                        close_timeout=5,
                        max_size=2**20,
                    ) as ws:
                        self._active_ws = ws
                        await ws.send(
                            json.dumps(
                                {
                                    "type": "subscribe",
                                    "product_ids": [PRODUCT],
                                    "channels": ["ticker", "heartbeat"],
                                }
                            )
                        )
                        self._status("RUNNING")
                        if not first:
                            self.store.mark_healthy(self.session_id, now_epoch_us())
                        first = False
                        while not self._stop_requested.is_set():
                            try:
                                text = await asyncio.wait_for(ws.recv(), timeout=2.0)
                            except asyncio.TimeoutError:
                                self._status("RUNNING")
                                continue
                            received_at_us = now_epoch_us()
                            self.last_message_at_us = received_at_us
                            payload = json.loads(text)
                            self._raw.write(received_at_us, payload)
                            if payload.get("type") == "error":
                                raise RuntimeError(f"Coinbase feed error: {payload}")
                            tick = parse_coinbase_ticker(payload, received_at_us)
                            if tick is not None:
                                self.receive_sequence += 1
                                self.tick_count += 1
                                self.last_tick_at_us = received_at_us
                                if tick.source_ts_us is not None:
                                    self.last_tick_source_ts_us = max(
                                        self.last_tick_source_ts_us or tick.source_ts_us, tick.source_ts_us
                                    )
                                self.store.persist_coinbase_tick(
                                    source_ts_us=tick.source_ts_us,
                                    received_at_us=received_at_us,
                                    receive_sequence=self.receive_sequence,
                                    price=tick.price,
                                    raw_payload=tick.raw_payload,
                                )
                            self.store.mark_healthy(self.session_id, received_at_us)
                            self._status("RUNNING")
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    if self._stop_requested.is_set():
                        break
                    self.error_count += 1
                    self.reconnect_count += 1
                    self.store.bump_error(self.session_id)
                    self.store.bump_reconnect(self.session_id)
                    self.store.record_gap(
                        component=COMPONENT,
                        detected_at_us=now_epoch_us(),
                        reason="CONNECTION_ERROR",
                        details={"exception": repr(exc)},
                    )
                    self._status("RECONNECTING", exception=repr(exc))
                    await asyncio.sleep(self.reconnect_delay_seconds)
                finally:
                    self._active_ws = None
        except Exception:
            fatal = True
            raise
        finally:
            self._raw.close()
            self._status("STOPPED" if not fatal else "FATAL")
            self.store.finish_session(self.session_id, now_epoch_us(), fatal=fatal)
