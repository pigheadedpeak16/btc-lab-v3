from __future__ import annotations

import json
import shutil
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

from ..feeds.polymarket import DiscoveredMarket
from ..state.book_state import OrderBookState
from ..timeutil import now_epoch_us
from .db import connect_sqlite

QUOTE_PROJECTION_POLICY_REVISION='phase595_atomic_depth_frame_v2'
_DELTA_INSERT='''INSERT INTO book_deltas(slug,token_id,side,source_ts_us,server_ts_us,received_at_us,
    receive_sequence,sync_generation,delta_json,raw_payload) VALUES(?,?,?,?,?,?,?,?,?,?)'''


def _json(value: object) -> str:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False, sort_keys=True)


def _fmt(value: object | None) -> str | None:
    if value is None:
        return None
    return format(value, "f") if hasattr(value, "as_tuple") else str(value)


@dataclass(frozen=True)
class StorageHealth:
    observed_at_us: int
    db_bytes: int
    wal_bytes: int
    raw_bytes: int
    free_bytes: int
    db_growth_bytes_per_hour: float | None
    estimated_12h_db_growth_bytes: float | None
    warning_level: str
    row_counts: dict[str, int]


@dataclass(frozen=True)
class MarketHealth:
    slug: str | None
    round_start_us: int | None
    round_end_us: int | None
    up_token_id: str | None
    down_token_id: str | None
    book_checkpoint_count: int
    book_delta_count: int
    last_book_received_at_us: int | None
    collector_state: str | None
    collector_updated_at_us: int | None
    reconnect_count: int
    error_count: int
    collector_details: dict[str, Any]


class MarketStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.conn = connect_sqlite(path)
        self._storage_baseline_at_us: int | None = None
        self._storage_baseline_bytes: int | None = None
        self._quote_event = None
        self._delta_event = None

    @contextmanager
    def coalesced_quotes(self):
        """Keep all normalized deltas; project one quote per token/event boundary.

        Intermediate levels inside one synchronous WS event cannot be decision
        inputs. Raw payloads and each delta retain their original provenance.
        All authoritative rows are retained in order, committed at the synchronous
        event boundary. A malformed frame still retains its valid parsed prefix.
        """
        if self._quote_event is not None:raise RuntimeError('NESTED_QUOTE_EVENT')
        pending={};deltas=[];self._quote_event=pending;self._delta_event=deltas
        try:yield
        finally:
            self._quote_event=None
            self._delta_event=None
            try:
                with self.conn:
                    if deltas:self.conn.executemany(_DELTA_INSERT,deltas)
                    for args in pending.values():
                        if args[0].depth_reliable:self._insert_best_quote(*args)
            except BaseException:
                for args in pending.values():args[0].mark_suspect('STORAGE_COMMIT_FAILED')
                raise

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "MarketStore":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def persist_market(self, market: DiscoveredMarket, observed_at_us: int) -> None:
        raw = {"event": market.raw_event, "market": market.raw_market}
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO markets(
                    slug,event_id,condition_id,round_start_us,round_end_us,question,rules_text,
                    active,first_observed_at_us,last_observed_at_us,raw_payload
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(slug) DO UPDATE SET
                    event_id=excluded.event_id,
                    condition_id=excluded.condition_id,
                    round_start_us=excluded.round_start_us,
                    round_end_us=excluded.round_end_us,
                    question=excluded.question,
                    rules_text=excluded.rules_text,
                    active=excluded.active,
                    last_observed_at_us=excluded.last_observed_at_us,
                    raw_payload=excluded.raw_payload
                """,
                (
                    market.slug,
                    market.event_id,
                    market.condition_id,
                    market.round_start_us,
                    market.round_end_us,
                    market.question,
                    market.rules_text,
                    1 if market.active and not market.closed else 0,
                    observed_at_us,
                    observed_at_us,
                    _json(raw),
                ),
            )
            for side, token_id in (("UP", market.up_token_id), ("DOWN", market.down_token_id)):
                self.conn.execute(
                    """
                    INSERT INTO market_tokens(slug,side,token_id,first_observed_at_us,last_observed_at_us)
                    VALUES(?,?,?,?,?)
                    ON CONFLICT(slug,side) DO UPDATE SET
                        token_id=excluded.token_id,
                        last_observed_at_us=excluded.last_observed_at_us
                    """,
                    (market.slug, side, token_id, observed_at_us, observed_at_us),
                )

    def persist_fee_schedule(
        self,
        *,
        slug: str,
        observed_at_us: int,
        rate: str,
        exponent: int,
        source: str,
        raw_payload: object,
    ) -> None:
        """Persist a versioned public market-fee observation.

        The forward simulator consumes this as research metadata only.  It does
        not submit or sign orders.
        """
        Decimal(rate)
        parameters = {"rate": str(rate), "exponent": int(exponent)}
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO fee_schedule_versions(
                    slug,observed_at_us,fee_model,parameters_json,source,raw_payload
                ) VALUES(?,?,?,?,?,?)
                """,
                (
                    slug,
                    observed_at_us,
                    "POLYMARKET_FD",
                    _json(parameters),
                    source,
                    _json(raw_payload),
                ),
            )

    def latest_fee_schedule(self, slug: str) -> tuple[Decimal, int, int] | None:
        row = self.conn.execute(
            """
            SELECT parameters_json,observed_at_us
            FROM fee_schedule_versions
            WHERE slug=?
            ORDER BY observed_at_us DESC,id DESC
            LIMIT 1
            """,
            (slug,),
        ).fetchone()
        if row is None:
            return None
        try:
            params = json.loads(str(row[0]))
            rate = Decimal(str(params["rate"]))
            exponent = int(params.get("exponent", 1))
            return rate, exponent, int(row[1])
        except Exception:
            return None

    def persist_book_checkpoint(
        self,
        state: OrderBookState,
        *,
        received_at_us: int,
        server_ts_us: int | None,
        receive_sequence: int,
        raw_payload: object,
    ) -> None:
        bid = state.best_bid
        ask = state.best_ask
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO book_checkpoints(
                    slug,token_id,side,source_ts_us,server_ts_us,received_at_us,receive_sequence,
                    sync_generation,bids_json,asks_json,raw_payload
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    state.slug,
                    state.token_id,
                    state.outcome,
                    server_ts_us,
                    server_ts_us,
                    received_at_us,
                    receive_sequence,
                    state.sync_generation,
                    _json(state.bids_jsonable()),
                    _json(state.asks_jsonable()),
                    _json(raw_payload),
                ),
            )
            self._insert_best_quote(state, received_at_us, bid, ask)
            self.conn.execute(
                """
                INSERT INTO market_metadata_versions(
                    slug,source_ts_us,received_at_us,tick_size,min_order_size,raw_payload
                ) VALUES(?,?,?,?,?,?)
                """,
                (
                    state.slug,
                    server_ts_us,
                    received_at_us,
                    _fmt(state.tick_size),
                    _fmt(state.min_order_size),
                    _json(raw_payload),
                ),
            )

    def persist_book_delta(
        self,
        state: OrderBookState,
        *,
        received_at_us: int,
        server_ts_us: int | None,
        receive_sequence: int,
        delta: object,
        raw_payload: object,
    ) -> None:
        bid = state.best_bid
        ask = state.best_ask
        values=(state.slug,state.token_id,state.outcome,server_ts_us,server_ts_us,received_at_us,receive_sequence,
            state.sync_generation,_json(delta),_json(raw_payload))
        if self._delta_event is not None:
            self._delta_event.append(values)
            self._insert_best_quote(state,received_at_us,bid,ask)
            return
        with self.conn:
            self.conn.execute(_DELTA_INSERT,values)
            self._insert_best_quote(state, received_at_us, bid, ask)

    def _insert_best_quote(self, state, received_at_us, bid, ask) -> None:
        if self._quote_event is not None:
            self._quote_event[(state.slug,state.token_id)]=(state,received_at_us,bid,ask)
            return
        self.conn.execute(
            """
            INSERT INTO best_quote_observations(
                slug,token_id,side,received_at_us,best_bid,best_bid_size,best_ask,best_ask_size,
                spread,sync_generation
            ) VALUES(?,?,?,?,?,?,?,?,?,?)
            """,
            (
                state.slug,
                state.token_id,
                state.outcome,
                received_at_us,
                _fmt(bid.price) if bid else None,
                _fmt(bid.size) if bid else None,
                _fmt(ask.price) if ask else None,
                _fmt(ask.size) if ask else None,
                _fmt(state.spread),
                state.sync_generation,
            ),
        )

    def persist_trade(
        self,
        market: DiscoveredMarket,
        *,
        token_id: str | None,
        source_ts_us: int | None,
        received_at_us: int,
        price: str,
        size: str,
        taker_side: str | None,
        source_trade_id: str | None,
        raw_payload: object,
    ) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO market_trades(
                    slug,token_id,source_trade_id,source_ts_us,received_at_us,price,size,taker_side,raw_payload
                ) VALUES(?,?,?,?,?,?,?,?,?)
                """,
                (
                    market.slug,
                    token_id,
                    source_trade_id,
                    source_ts_us,
                    received_at_us,
                    price,
                    size,
                    taker_side,
                    _json(raw_payload),
                ),
            )

    def update_tick_size(
        self,
        state: OrderBookState,
        *,
        source_ts_us: int | None,
        received_at_us: int,
        raw_payload: object,
    ) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO market_metadata_versions(
                    slug,source_ts_us,received_at_us,tick_size,min_order_size,raw_payload
                ) VALUES(?,?,?,?,?,?)
                """,
                (
                    state.slug,
                    source_ts_us,
                    received_at_us,
                    _fmt(state.tick_size),
                    _fmt(state.min_order_size),
                    _json(raw_payload),
                ),
            )

    def start_session(self, session_id: str, component: str, started_at_us: int) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO collector_sessions(session_id,component,started_at_us,last_healthy_at_us)
                VALUES(?,?,?,?)
                """,
                (session_id, component, started_at_us, started_at_us),
            )

    def finish_session(self, session_id: str, ended_at_us: int, *, fatal: bool = False) -> None:
        with self.conn:
            self.conn.execute(
                "UPDATE collector_sessions SET ended_at_us=?, fatal=? WHERE session_id=?",
                (ended_at_us, 1 if fatal else 0, session_id),
            )

    def bump_reconnect(self, session_id: str) -> None:
        with self.conn:
            self.conn.execute(
                "UPDATE collector_sessions SET reconnect_count=reconnect_count+1 WHERE session_id=?",
                (session_id,),
            )

    def bump_error(self, session_id: str) -> None:
        with self.conn:
            self.conn.execute(
                "UPDATE collector_sessions SET error_count=error_count+1 WHERE session_id=?",
                (session_id,),
            )

    def mark_healthy(self, session_id: str, at_us: int) -> None:
        with self.conn:
            self.conn.execute(
                "UPDATE collector_sessions SET last_healthy_at_us=? WHERE session_id=?",
                (at_us, session_id),
            )

    def set_status(
        self,
        *,
        component: str,
        session_id: str | None,
        slug: str | None,
        state: str,
        updated_at_us: int,
        last_message_at_us: int | None,
        last_book_at_us: int | None,
        reconnect_count: int,
        error_count: int,
        details: object,
    ) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO collector_status(
                    component,session_id,slug,state,updated_at_us,last_message_at_us,last_book_at_us,
                    reconnect_count,error_count,details_json
                ) VALUES(?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(component) DO UPDATE SET
                    session_id=excluded.session_id,
                    slug=excluded.slug,
                    state=excluded.state,
                    updated_at_us=excluded.updated_at_us,
                    last_message_at_us=excluded.last_message_at_us,
                    last_book_at_us=excluded.last_book_at_us,
                    reconnect_count=excluded.reconnect_count,
                    error_count=excluded.error_count,
                    details_json=excluded.details_json
                """,
                (
                    component,
                    session_id,
                    slug,
                    state,
                    updated_at_us,
                    last_message_at_us,
                    last_book_at_us,
                    reconnect_count,
                    error_count,
                    _json(details),
                ),
            )

    def record_gap(
        self,
        *,
        component: str,
        slug: str | None,
        detected_at_us: int,
        reason: str,
        details: object,
    ) -> int:
        with self.conn:
            cursor = self.conn.execute(
                """
                INSERT INTO feed_gap_events(component,slug,detected_at_us,reason,details_json)
                VALUES(?,?,?,?,?)
                """,
                (component, slug, detected_at_us, reason, _json(details)),
            )
            return int(cursor.lastrowid)

    def recover_gap(self, gap_id: int, recovered_at_us: int) -> None:
        with self.conn:
            self.conn.execute(
                "UPDATE feed_gap_events SET recovered_at_us=? WHERE id=?",
                (recovered_at_us, gap_id),
            )

    def storage_profile(self) -> str | None:
        row = self.conn.execute(
            "SELECT value FROM schema_meta WHERE key='storage_profile'"
        ).fetchone()
        return None if row is None else str(row[0])

    def _approx_row_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        try:
            for name, seq in self.conn.execute("SELECT name,seq FROM sqlite_sequence").fetchall():
                counts[str(name)] = int(seq or 0)
        except sqlite3.OperationalError:
            pass
        for name in ("markets", "market_tokens"):
            try:
                row = self.conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()
                counts[name] = int(row[0] or 0) if row else 0
            except sqlite3.OperationalError:
                pass
        return counts

    def capture_storage_health(self, *, root: Path, observed_at_us: int | None = None) -> StorageHealth:
        """Persist a lightweight storage-growth snapshot without scanning hot tables.

        Raw archive bytes are measured from filesystem metadata; SQLite row counts
        use sqlite_sequence where possible so this stays safe in the live loop.
        """
        observed_at_us = now_epoch_us() if observed_at_us is None else int(observed_at_us)
        db_bytes = self.path.stat().st_size if self.path.exists() else 0
        wal_path = Path(str(self.path) + "-wal")
        wal_bytes = wal_path.stat().st_size if wal_path.exists() else 0
        raw_root = root / "data" / "raw"
        raw_bytes = 0
        if raw_root.exists():
            try:
                raw_bytes = sum(p.stat().st_size for p in raw_root.rglob("*.gz") if p.is_file())
            except OSError:
                raw_bytes = 0
        free_bytes = int(shutil.disk_usage(root).free)
        total_live_bytes = db_bytes + wal_bytes
        if self._storage_baseline_at_us is None or self._storage_baseline_bytes is None:
            self._storage_baseline_at_us = observed_at_us
            self._storage_baseline_bytes = total_live_bytes
            growth_bph = None
        else:
            elapsed_hours = max(0.0, (observed_at_us - self._storage_baseline_at_us) / 3_600_000_000.0)
            growth_bph = None if elapsed_hours < (2.0 / 60.0) else max(0.0, (total_live_bytes - self._storage_baseline_bytes) / elapsed_hours)
        est_12h = None if growth_bph is None else growth_bph * 12.0

        gib = 1024 ** 3
        warning = "OK"
        if free_bytes < 8 * gib:
            warning = "CRITICAL_LOW_DISK"
        elif free_bytes < 15 * gib:
            warning = "LOW_DISK"
        elif growth_bph is not None and growth_bph > 1.5 * gib:
            warning = "HIGH_DB_GROWTH"
        elif est_12h is not None and est_12h > max(0, free_bytes - 12 * gib):
            warning = "OVERNIGHT_CAPACITY_RISK"

        row_counts = self._approx_row_counts()
        details = {
            "storage_profile": self.storage_profile(),
            "row_counts": row_counts,
            "db_path": str(self.path),
            "baseline_at_us": self._storage_baseline_at_us,
            "baseline_bytes": self._storage_baseline_bytes,
        }
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO storage_health_snapshots(
                    observed_at_us,db_bytes,wal_bytes,raw_bytes,free_bytes,
                    db_growth_bytes_per_hour,estimated_12h_db_growth_bytes,warning_level,details_json
                ) VALUES(?,?,?,?,?,?,?,?,?)
                """,
                (
                    observed_at_us,
                    db_bytes,
                    wal_bytes,
                    raw_bytes,
                    free_bytes,
                    growth_bph,
                    est_12h,
                    warning,
                    _json(details),
                ),
            )
        return StorageHealth(
            observed_at_us=observed_at_us,
            db_bytes=db_bytes,
            wal_bytes=wal_bytes,
            raw_bytes=raw_bytes,
            free_bytes=free_bytes,
            db_growth_bytes_per_hour=growth_bph,
            estimated_12h_db_growth_bytes=est_12h,
            warning_level=warning,
            row_counts=row_counts,
        )

    def latest_storage_health(self) -> StorageHealth | None:
        row = self.conn.execute(
            """
            SELECT observed_at_us,db_bytes,wal_bytes,raw_bytes,free_bytes,
                   db_growth_bytes_per_hour,estimated_12h_db_growth_bytes,warning_level,details_json
            FROM storage_health_snapshots ORDER BY observed_at_us DESC,id DESC LIMIT 1
            """
        ).fetchone()
        if row is None:
            return None
        try:
            details = json.loads(str(row[8]) or "{}")
        except Exception:
            details = {}
        return StorageHealth(
            observed_at_us=int(row[0]),
            db_bytes=int(row[1]),
            wal_bytes=int(row[2]),
            raw_bytes=int(row[3]),
            free_bytes=int(row[4]),
            db_growth_bytes_per_hour=None if row[5] is None else float(row[5]),
            estimated_12h_db_growth_bytes=None if row[6] is None else float(row[6]),
            warning_level=str(row[7]),
            row_counts={str(k): int(v) for k, v in (details.get("row_counts") or {}).items()},
        )

    def current_health(self) -> MarketHealth:
        market_row = self.conn.execute(
            """
            SELECT slug,round_start_us,round_end_us
            FROM markets
            ORDER BY round_end_us DESC
            LIMIT 1
            """
        ).fetchone()
        if market_row:
            slug = str(market_row[0])
            tokens = {
                row[0]: row[1]
                for row in self.conn.execute(
                    "SELECT side,token_id FROM market_tokens WHERE slug=?", (slug,)
                ).fetchall()
            }
            cp_count = self.conn.execute(
                "SELECT COUNT(*) FROM book_checkpoints WHERE slug=?", (slug,)
            ).fetchone()[0]
            delta_count = self.conn.execute(
                "SELECT COUNT(*) FROM book_deltas WHERE slug=?", (slug,)
            ).fetchone()[0]
            last_book = self.conn.execute(
                """
                SELECT MAX(received_at_us) FROM (
                    SELECT received_at_us FROM book_checkpoints WHERE slug=?
                    UNION ALL
                    SELECT received_at_us FROM book_deltas WHERE slug=?
                )
                """,
                (slug, slug),
            ).fetchone()[0]
        else:
            slug = None
            tokens = {}
            cp_count = 0
            delta_count = 0
            last_book = None

        status = self.conn.execute(
            """
            SELECT state,updated_at_us,reconnect_count,error_count,details_json
            FROM collector_status WHERE component='polymarket_clob'
            """
        ).fetchone()
        if status:
            try:
                details = json.loads(status[4])
            except Exception:
                details = {"raw": status[4]}
            collector_state = status[0]
            collector_updated = status[1]
            reconnect_count = int(status[2])
            error_count = int(status[3])
        else:
            details = {}
            collector_state = None
            collector_updated = None
            reconnect_count = 0
            error_count = 0

        return MarketHealth(
            slug=slug,
            round_start_us=market_row[1] if market_row else None,
            round_end_us=market_row[2] if market_row else None,
            up_token_id=tokens.get("UP"),
            down_token_id=tokens.get("DOWN"),
            book_checkpoint_count=int(cp_count),
            book_delta_count=int(delta_count),
            last_book_received_at_us=last_book,
            collector_state=collector_state,
            collector_updated_at_us=collector_updated,
            reconnect_count=reconnect_count,
            error_count=error_count,
            collector_details=details,
        )

    @classmethod
    def open_project(cls, root: Path) -> "MarketStore":
        return cls(root / "data" / "v3_market.sqlite")
