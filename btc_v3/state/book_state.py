from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Iterable, Mapping


def _decimal(value: str | int | float | Decimal) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


@dataclass(frozen=True)
class QuoteLevel:
    price: Decimal
    size: Decimal

    def as_pair(self) -> list[str]:
        return [format(self.price, "f"), format(self.size, "f")]


@dataclass
class OrderBookState:
    slug: str
    token_id: str
    outcome: str
    bids: dict[Decimal, Decimal] = field(default_factory=dict)
    asks: dict[Decimal, Decimal] = field(default_factory=dict)
    synchronized: bool = False
    sync_generation: int = 0
    mutation_revision: int = 0
    last_server_ts_us: int | None = None
    last_received_at_us: int | None = None
    last_verified_at_us: int | None = None
    last_hash: str | None = None
    tick_size: Decimal | None = None
    min_order_size: Decimal | None = None
    neg_risk: bool | None = None
    last_trade_price: Decimal | None = None
    depth_integrity: str = "UNSYNCHRONIZED"
    integrity_reason: str | None = None
    last_bba_received_at_us: int | None = None
    last_bba_server_ts_us: int | None = None
    last_tick_received_at_us: int | None = None
    last_tick_server_ts_us: int | None = None
    transport_fault: object | None = field(default=None, repr=False, compare=False)
    _seen_deltas: set[tuple] = field(default_factory=set, repr=False)

    @property
    def depth_reliable(self) -> bool:
        return (self.synchronized and self.depth_integrity == "VALID"
                and not (self.transport_fault is not None and self.transport_fault.is_set()))

    def mark_suspect(self, reason: str) -> None:
        self.depth_integrity = "SUSPECT"
        self.integrity_reason = reason

    def mark_unsynchronized(self) -> None:
        self.synchronized = False
        self.depth_integrity = "UNSYNCHRONIZED"
        self.integrity_reason = "SNAPSHOT_REQUIRED"

    @staticmethod
    def _level(price: object, size: object) -> tuple[Decimal, Decimal]:
        try:
            px, qty = _decimal(price), _decimal(size)
            if not px.is_finite() or not qty.is_finite() or not 0 <= px <= 1 or qty < 0:
                raise ValueError("INVALID_BOOK_LEVEL")
            return px, qty
        except Exception as exc:
            raise ValueError("INVALID_BOOK_LEVEL") from exc

    @staticmethod
    def _parse_levels(levels: Iterable[Mapping[str, object]]) -> dict[Decimal, Decimal]:
        parsed: dict[Decimal, Decimal] = {}
        for row in levels:
            if "price" not in row or "size" not in row:
                raise ValueError("INCOMPLETE_BOOK_LEVEL")
            price, size = OrderBookState._level(row["price"], row["size"])
            if size > 0:
                parsed[price] = size
        return parsed

    def apply_snapshot(
        self,
        *,
        bids: Iterable[Mapping[str, object]],
        asks: Iterable[Mapping[str, object]],
        received_at_us: int,
        server_ts_us: int | None,
        book_hash: str | None,
        tick_size: str | Decimal | None = None,
        min_order_size: str | Decimal | None = None,
        neg_risk: bool | None = None,
        last_trade_price: str | Decimal | None = None,
        verified_rest: bool = False,
        verified_at_us: int | None = None,
    ) -> None:
        try:
            parsed_bids, parsed_asks = self._parse_levels(bids), self._parse_levels(asks)
            if parsed_bids and parsed_asks and max(parsed_bids) >= min(parsed_asks):
                raise ValueError("CROSSED_BOOK")
        except Exception:
            self.mark_suspect("INVALID_SNAPSHOT")
            raise
        if (not verified_rest and self.depth_reliable and book_hash is not None
                and book_hash == self.last_hash and server_ts_us == self.last_server_ts_us
                and parsed_bids == self.bids and parsed_asks == self.asks):
            return
        self.bids, self.asks = parsed_bids, parsed_asks
        self.sync_generation += 1
        self.mutation_revision += 1
        self.synchronized = True
        self.depth_integrity = "VALID"
        self.integrity_reason = None
        self._seen_deltas.clear()
        self.last_received_at_us = received_at_us
        self.last_verified_at_us = received_at_us if verified_at_us is None else verified_at_us
        self.last_server_ts_us = server_ts_us
        self.last_hash = book_hash
        if tick_size is not None:
            self.tick_size = _decimal(tick_size)
        if min_order_size is not None:
            self.min_order_size = _decimal(min_order_size)
        if neg_risk is not None:
            self.neg_risk = bool(neg_risk)
        if last_trade_price is not None:
            self.last_trade_price = _decimal(last_trade_price)

    def apply_price_change(
        self,
        *,
        order_side: str,
        price: str | Decimal,
        size: str | Decimal,
        received_at_us: int,
        server_ts_us: int | None,
        change_hash: str | None = None,
        verified_at_us: int | None = None,
    ) -> bool:
        if not self.depth_reliable:
            return False
        if (
            server_ts_us is not None
            and self.last_server_ts_us is not None
            and server_ts_us < self.last_server_ts_us
        ):
            if self.last_received_at_us is not None and received_at_us <= self.last_received_at_us:
                return False  # Already covered by a later full/absolute observation.
            self.mark_suspect("OUT_OF_ORDER_DEPTH")
            return False

        side = order_side.upper()
        if side == "BUY":
            levels = self.bids
        elif side == "SELL":
            levels = self.asks
        else:
            self.mark_suspect("INVALID_DELTA")
            raise ValueError(f"unsupported order side: {order_side!r}")

        try:
            px, qty = self._level(price, size)
        except ValueError:
            self.mark_suspect("INVALID_DELTA")
            raise
        identity = (server_ts_us, change_hash, side, px, qty)
        if identity in self._seen_deltas:
            return False
        if server_ts_us != self.last_server_ts_us:
            self._seen_deltas.clear()
        if len(self._seen_deltas) >= 4096:
            self.mark_suspect("AMBIGUOUS_DEPTH_BURST")
            return False
        self._seen_deltas.add(identity)
        self.mutation_revision += 1
        if qty <= 0:
            levels.pop(px, None)
        else:
            levels[px] = qty
        if self.bids and self.asks and max(self.bids) >= min(self.asks):
            self.mark_suspect("CROSSED_BOOK")

        self.last_received_at_us = received_at_us
        self.last_verified_at_us = received_at_us if verified_at_us is None else verified_at_us
        if server_ts_us is not None:
            self.last_server_ts_us = server_ts_us
        if change_hash:
            self.last_hash = change_hash
        return True

    def mark_verified(self, received_at_us: int, server_ts_us: int | None = None) -> None:
        # Auxiliary top quotes cannot verify quantities away from the touch.
        if self.last_bba_server_ts_us is not None and server_ts_us is not None and server_ts_us < self.last_bba_server_ts_us:
            return
        self.last_bba_received_at_us = received_at_us
        self.last_bba_server_ts_us = server_ts_us

    def apply_tick_size(self, new_tick_size: str | Decimal, received_at_us: int,
                        server_ts_us: int | None = None) -> bool:
        if (server_ts_us is not None and self.last_tick_server_ts_us is not None
                and server_ts_us < self.last_tick_server_ts_us):
            return False
        tick = _decimal(new_tick_size)
        if not tick.is_finite() or not 0 < tick <= 1:
            self.mark_suspect("INVALID_TICK_SIZE")
            raise ValueError("INVALID_TICK_SIZE")
        self.tick_size = tick
        self.mutation_revision += 1
        self.last_tick_received_at_us = received_at_us
        self.last_tick_server_ts_us = server_ts_us
        return True

    @property
    def best_bid(self) -> QuoteLevel | None:
        if not self.bids:
            return None
        price = max(self.bids)
        return QuoteLevel(price=price, size=self.bids[price])

    @property
    def best_ask(self) -> QuoteLevel | None:
        if not self.asks:
            return None
        price = min(self.asks)
        return QuoteLevel(price=price, size=self.asks[price])

    @property
    def spread(self) -> Decimal | None:
        bid = self.best_bid
        ask = self.best_ask
        if bid is None or ask is None:
            return None
        return ask.price - bid.price

    def bids_jsonable(self) -> list[list[str]]:
        return [
            [format(price, "f"), format(self.bids[price], "f")]
            for price in sorted(self.bids, reverse=True)
        ]

    def asks_jsonable(self) -> list[list[str]]:
        return [
            [format(price, "f"), format(self.asks[price], "f")]
            for price in sorted(self.asks)
        ]

    def age_seconds(self, now_us: int) -> float | None:
        """Conservative execution age: both source and receipt must be causal.

        A freshly delivered old snapshot never refreshes source-time eligibility.
        Auxiliary BBA/tick messages do not refresh either depth timestamp.
        """
        if (not self.depth_reliable or self.last_received_at_us is None or self.last_server_ts_us is None
                or not 0 <= self.last_server_ts_us <= self.last_received_at_us <= now_us):
            return None
        return max(now_us-self.last_received_at_us,now_us-self.last_server_ts_us)/1_000_000.0
