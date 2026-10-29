from __future__ import annotations

import json
import math
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

from .db import connect_sqlite

OFFICIAL_REFERENCE_SOURCE = "CHAINLINK_BTC_USD_TWAP_60S"
OFFICIAL_REFERENCE_STREAM = "prices.crypto.twap:btcusd:60s"
REFERENCE_INTEGRITY_POLICY_REVISION = 'six_hour_reference_causality_v2'


def positive_price(value: str) -> bool:
    try:
        price = Decimal(value)
        return price.is_finite() and price > 0 and math.isfinite(float(price)) and float(price) > 0
    except (ValueError, ArithmeticError):
        return False


def _json(value: object) -> str:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False, sort_keys=True)


@dataclass(frozen=True)
class ExternalComponentHealth:
    component: str
    state: str | None
    updated_at_us: int | None
    last_message_at_us: int | None
    last_tick_at_us: int | None
    reconnect_count: int
    error_count: int
    details: dict[str, Any]


@dataclass(frozen=True)
class ExternalHealth:
    coinbase_count: int
    kraken_count: int
    settlement_observation_count: int
    price_reference_count: int
    latest_coinbase_at_us: int | None
    latest_kraken_at_us: int | None
    latest_settlement_at_us: int | None
    components: dict[str, ExternalComponentHealth]


@dataclass(frozen=True)
class PriceReference:
    slug: str
    round_start_us: int
    round_end_us: int
    price_to_beat: str
    reference_ts_us: int | None
    observed_at_us: int
    source: str
    source_stream: str | None
    mapping_method: str


def _verified_reference(rows) -> PriceReference | None:
    if not rows or any(not positive_price(str(r[3])) or r[4] is None or not 0<=r[4]<=r[5] for r in rows):
        return None
    identities = {(r[0], r[1], r[2], Decimal(str(r[3])), r[4]) for r in rows}
    if len(identities) != 1:
        return None  # Do not choose between contradictory PTB/boundary claims.
    row = rows[0]
    return PriceReference(str(row[0]), int(row[1]), int(row[2]), str(row[3]), int(row[4]),
        int(row[5]), str(row[6]), str(row[7]), str(row[8]))


class ExternalStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.conn = connect_sqlite(path)

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "ExternalStore":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    @classmethod
    def open_project(cls, root: Path) -> "ExternalStore":
        return cls(root / "data" / "v3_external.sqlite")

    def persist_coinbase_tick(
        self,
        *,
        source_ts_us: int | None,
        received_at_us: int,
        receive_sequence: int,
        price: str,
        raw_payload: object,
    ) -> None:
        Decimal(price)
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO coinbase_ticks(source_ts_us,received_at_us,receive_sequence,price,raw_payload)
                VALUES(?,?,?,?,?)
                """,
                (source_ts_us, received_at_us, receive_sequence, price, _json(raw_payload)),
            )

    def persist_kraken_tick(
        self,
        *,
        source_ts_us: int | None,
        received_at_us: int,
        receive_sequence: int,
        price: str,
        raw_payload: object,
    ) -> None:
        Decimal(price)
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO kraken_ticks(source_ts_us,received_at_us,receive_sequence,price,raw_payload)
                VALUES(?,?,?,?,?)
                """,
                (source_ts_us, received_at_us, receive_sequence, price, _json(raw_payload)),
            )

    def persist_settlement_observation(
        self,
        *,
        source_ts_us: int | None,
        received_at_us: int,
        receive_sequence: int,
        value: str,
        source: str,
        stream_id: str | None,
        raw_payload: object,
    ) -> int:
        Decimal(value)
        with self.conn:
            cursor = self.conn.execute(
                """
                INSERT INTO settlement_reference_observations(
                    source_ts_us,received_at_us,receive_sequence,value,source,stream_id,raw_payload
                ) VALUES(?,?,?,?,?,?,?)
                """,
                (
                    source_ts_us,
                    received_at_us,
                    receive_sequence,
                    value,
                    source,
                    stream_id,
                    _json(raw_payload),
                ),
            )
            return int(cursor.lastrowid)

    def persist_price_reference(
        self,
        *,
        slug: str,
        round_start_us: int,
        round_end_us: int,
        price_to_beat: str,
        reference_ts_us: int | None,
        observed_at_us: int,
        source: str,
        source_stream: str | None,
        mapping_method: str,
        raw_payload: object,
    ) -> int:
        Decimal(price_to_beat)
        with self.conn:
            cursor = self.conn.execute(
                """
                INSERT INTO price_references(
                    slug,round_start_us,round_end_us,price_to_beat,reference_ts_us,observed_at_us,
                    source,source_stream,mapping_method,raw_payload
                ) VALUES(?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    slug,
                    round_start_us,
                    round_end_us,
                    price_to_beat,
                    reference_ts_us,
                    observed_at_us,
                    source,
                    source_stream,
                    mapping_method,
                    _json(raw_payload),
                ),
            )
            return int(cursor.lastrowid)

    @staticmethod
    def _tick_table_and_index(source: str) -> tuple[str, str]:
        mapping = {
            "coinbase": ("coinbase_ticks", "idx_coinbase_causal"),
            "kraken": ("kraken_ticks", "idx_kraken_causal"),
        }
        try:
            return mapping[source]
        except KeyError as exc:
            raise ValueError(f"unsupported source: {source}") from exc

    def latest_tick(
        self,
        *,
        source: str,
        target_source_ts_us: int,
        decision_time_us: int,
    ) -> tuple[int, int, str] | None:
        """Return the latest causally observable tick at or before target time.

        The Phase-5.7 live loop calls this path extremely frequently.  The explicit
        expression index avoids an O(N) scan + temporary sort on large historical
        databases.  This is a correctness-preserving performance requirement: a
        slow synchronous SQLite query can starve the asyncio collectors and make a
        healthy network feed *look* stale.
        """
        table, index = self._tick_table_and_index(source)
        row = self.conn.execute(
            f"""
            SELECT COALESCE(source_ts_us, received_at_us), received_at_us, price
            FROM {table} INDEXED BY {index}
            WHERE COALESCE(source_ts_us, received_at_us)<=?
              AND received_at_us<=?
            ORDER BY COALESCE(source_ts_us, received_at_us) DESC, received_at_us DESC, id DESC
            LIMIT 1
            """,
            (min(target_source_ts_us, decision_time_us), decision_time_us),
        ).fetchone()
        if row is None:
            return None
        return int(row[0]), int(row[1]), str(row[2])

    def tick_window(
        self,
        *,
        source: str,
        start_source_ts_us: int,
        end_source_ts_us: int,
        decision_time_us: int,
    ) -> list[tuple[int, int, int, str]]:
        """Bulk-load a causal tick window ordered by effective source time.

        Returns ``(effective_source_ts_us, received_at_us, id, price)``.  Live
        feature construction uses one range query per exchange and performs the
        hundreds of historical lookups in memory instead of issuing hundreds of
        SQLite point queries every second.
        """
        if end_source_ts_us < start_source_ts_us:
            return []
        table, index = self._tick_table_and_index(source)
        rows = self.conn.execute(
            f"""
            SELECT COALESCE(source_ts_us, received_at_us), received_at_us, id, price
            FROM {table} INDEXED BY {index}
            WHERE COALESCE(source_ts_us, received_at_us)>=?
              AND COALESCE(source_ts_us, received_at_us)<=?
              AND received_at_us<=?
            ORDER BY COALESCE(source_ts_us, received_at_us) ASC, received_at_us ASC, id ASC
            """,
            (start_source_ts_us, min(end_source_ts_us, decision_time_us), decision_time_us),
        ).fetchall()
        return [(int(r[0]), int(r[1]), int(r[2]), str(r[3])) for r in rows]

    def latest_settlement_observation(
        self,
        *,
        target_source_ts_us: int,
        decision_time_us: int,
    ) -> tuple[int, int, str] | None:
        row = self.conn.execute(
            """
            SELECT source_ts_us,received_at_us,value
            FROM settlement_reference_observations INDEXED BY idx_settlement_ref_causal
            WHERE source_ts_us IS NOT NULL
              AND source_ts_us<=?
              AND received_at_us<=?
              AND source=? AND stream_id=?
            ORDER BY source_ts_us DESC, received_at_us DESC, id DESC
            LIMIT 1
            """,
            (min(target_source_ts_us, decision_time_us), decision_time_us,
             OFFICIAL_REFERENCE_SOURCE, OFFICIAL_REFERENCE_STREAM),
        ).fetchone()
        if row is None:
            return None
        exact = self.settlement_observation_exact(source_ts_us=int(row[0]), decision_time_us=decision_time_us)
        return None if exact is None else (exact[0], exact[1], exact[2])

    def settlement_observation_exact(
        self,
        *,
        source_ts_us: int,
        decision_time_us: int,
    ) -> tuple[int, int, str, str, str | None, object] | None:
        if source_ts_us > decision_time_us:
            return None
        rows = self.conn.execute(
            """
            SELECT source_ts_us,received_at_us,value,source,stream_id,raw_payload
            FROM settlement_reference_observations INDEXED BY idx_settlement_ref_causal
            WHERE source_ts_us=? AND received_at_us<=? AND source=? AND stream_id=?
            ORDER BY received_at_us ASC, id ASC
            """,
            (source_ts_us, decision_time_us, OFFICIAL_REFERENCE_SOURCE, OFFICIAL_REFERENCE_STREAM),
        ).fetchall()
        if not rows or any(not positive_price(str(r[2])) for r in rows):
            return None
        if len({Decimal(str(r[2])) for r in rows}) != 1:
            return None  # Conflicting official observations are not an authoritative boundary.
        row = rows[0]
        payload = json.loads(row[5]) if row[5] else {}
        return int(row[0]), int(row[1]), str(row[2]), str(row[3]), (str(row[4]) if row[4] is not None else None), payload

    def price_reference_exact(
        self,
        *,
        slug: str,
        reference_ts_us: int,
        decision_time_us: int,
    ) -> PriceReference | None:
        rows = self.conn.execute(
            """
            SELECT slug,round_start_us,round_end_us,price_to_beat,reference_ts_us,observed_at_us,
                   source,source_stream,mapping_method
            FROM price_references INDEXED BY idx_price_ref_exact_causal
            WHERE slug=? AND reference_ts_us=? AND reference_ts_us<=? AND observed_at_us<=?
              AND source=? AND source_stream=?
              AND mapping_method='EXACT_ROUND_START_SOURCE_TIMESTAMP'
            ORDER BY observed_at_us ASC, id ASC
            """,
            (slug, reference_ts_us, decision_time_us, decision_time_us,
             OFFICIAL_REFERENCE_SOURCE, OFFICIAL_REFERENCE_STREAM),
        ).fetchall()
        return _verified_reference(rows)

    def causal_price_reference(self, slug: str, decision_time_us: int) -> PriceReference | None:
        rows = self.conn.execute(
            """
            SELECT slug,round_start_us,round_end_us,price_to_beat,reference_ts_us,observed_at_us,
                   source,source_stream,mapping_method
            FROM price_references
            WHERE slug=? AND observed_at_us<=? AND reference_ts_us<=?
              AND source=? AND source_stream=?
              AND mapping_method='EXACT_ROUND_START_SOURCE_TIMESTAMP'
            ORDER BY observed_at_us DESC, id DESC
            """,
            (slug, decision_time_us, decision_time_us, OFFICIAL_REFERENCE_SOURCE, OFFICIAL_REFERENCE_STREAM),
        ).fetchall()
        return _verified_reference(rows)

    def market_reference_status(self, market, decision_time_us: int) -> dict:
        """Bounded display projection. Retention never grants model/order eligibility."""
        rows=self.conn.execute('''SELECT slug,round_start_us,round_end_us,price_to_beat,reference_ts_us,
            observed_at_us,source,source_stream,mapping_method
            FROM price_references INDEXED BY idx_price_ref_exact_causal
            WHERE slug=? AND reference_ts_us=? AND reference_ts_us<=? AND observed_at_us<=?
              AND source=? AND source_stream=? AND mapping_method='EXACT_ROUND_START_SOURCE_TIMESTAMP'
            ORDER BY observed_at_us,id LIMIT 101''',
            (market.slug,market.round_start_us,decision_time_us,decision_time_us,OFFICIAL_REFERENCE_SOURCE,OFFICIAL_REFERENCE_STREAM)).fetchall()
        result=dict(state='UNAVAILABLE',slug=market.slug,price_to_beat=None,execution_eligible=False,
            display_policy_revision='six_hour_retained_ptb_display_v1',integrity_policy_revision=REFERENCE_INTEGRITY_POLICY_REVISION,
            scope='Retained market-bound reference; current model/reference/book/risk checks remain separate')
        if not rows:return result
        if len(rows)>100:return {**result,'state':'CAPPED_UNVERIFIED'}
        reference=_verified_reference(rows)
        if (reference is None or reference.round_start_us!=market.round_start_us or reference.round_end_us!=market.round_end_us):
            return {**result,'state':'CONFLICTING_OR_INVALID'}
        return {**result,'state':'VERIFIED_RETAINED','price_to_beat':reference.price_to_beat,
            'source':reference.source,'stream':reference.source_stream,'reference_ts_us':reference.reference_ts_us,
            'observed_at_us':reference.observed_at_us,'mapping_method':reference.mapping_method,
            'reference_observation_age_seconds':(decision_time_us-reference.observed_at_us)/1e6}

    def start_session(self, session_id: str, component: str, started_at_us: int) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO external_collector_sessions(session_id,component,started_at_us,last_healthy_at_us)
                VALUES(?,?,?,?)
                """,
                (session_id, component, started_at_us, started_at_us),
            )

    def finish_session(self, session_id: str, ended_at_us: int, *, fatal: bool = False) -> None:
        with self.conn:
            self.conn.execute(
                "UPDATE external_collector_sessions SET ended_at_us=?, fatal=? WHERE session_id=?",
                (ended_at_us, 1 if fatal else 0, session_id),
            )

    def bump_reconnect(self, session_id: str) -> None:
        with self.conn:
            self.conn.execute(
                "UPDATE external_collector_sessions SET reconnect_count=reconnect_count+1 WHERE session_id=?",
                (session_id,),
            )

    def bump_error(self, session_id: str) -> None:
        with self.conn:
            self.conn.execute(
                "UPDATE external_collector_sessions SET error_count=error_count+1 WHERE session_id=?",
                (session_id,),
            )

    def mark_healthy(self, session_id: str, at_us: int) -> None:
        with self.conn:
            self.conn.execute(
                "UPDATE external_collector_sessions SET last_healthy_at_us=? WHERE session_id=?",
                (at_us, session_id),
            )

    def set_status(
        self,
        *,
        component: str,
        session_id: str | None,
        state: str,
        updated_at_us: int,
        last_message_at_us: int | None,
        last_tick_at_us: int | None,
        reconnect_count: int,
        error_count: int,
        details: object,
    ) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO external_status(
                    component,session_id,state,updated_at_us,last_message_at_us,last_tick_at_us,
                    reconnect_count,error_count,details_json
                ) VALUES(?,?,?,?,?,?,?,?,?)
                ON CONFLICT(component) DO UPDATE SET
                    session_id=excluded.session_id,
                    state=excluded.state,
                    updated_at_us=excluded.updated_at_us,
                    last_message_at_us=excluded.last_message_at_us,
                    last_tick_at_us=excluded.last_tick_at_us,
                    reconnect_count=excluded.reconnect_count,
                    error_count=excluded.error_count,
                    details_json=excluded.details_json
                """,
                (
                    component,
                    session_id,
                    state,
                    updated_at_us,
                    last_message_at_us,
                    last_tick_at_us,
                    reconnect_count,
                    error_count,
                    _json(details),
                ),
            )

    def record_gap(self, *, component: str, detected_at_us: int, reason: str, details: object) -> int:
        with self.conn:
            cursor = self.conn.execute(
                """
                INSERT INTO external_gap_events(component,detected_at_us,reason,details_json)
                VALUES(?,?,?,?)
                """,
                (component, detected_at_us, reason, _json(details)),
            )
            return int(cursor.lastrowid)

    def recover_gap(self, gap_id: int, recovered_at_us: int) -> None:
        with self.conn:
            self.conn.execute(
                "UPDATE external_gap_events SET recovered_at_us=? WHERE id=?",
                (recovered_at_us, gap_id),
            )

    def _component_health(self) -> dict[str, ExternalComponentHealth]:
        result: dict[str, ExternalComponentHealth] = {}
        rows = self.conn.execute(
            """
            SELECT component,state,updated_at_us,last_message_at_us,last_tick_at_us,
                   reconnect_count,error_count,details_json
            FROM external_status LIMIT 16
            """
        ).fetchall()
        for row in rows:
            try:
                details = json.loads(row[7])
            except Exception:
                details = {"raw": row[7]}
            result[str(row[0])] = ExternalComponentHealth(
                component=str(row[0]),
                state=str(row[1]) if row[1] is not None else None,
                updated_at_us=int(row[2]) if row[2] is not None else None,
                last_message_at_us=int(row[3]) if row[3] is not None else None,
                last_tick_at_us=int(row[4]) if row[4] is not None else None,
                reconnect_count=int(row[5]),
                error_count=int(row[6]),
                details=details,
            )
        return result

    def component_health(self) -> dict[str, ExternalComponentHealth]:
        """Bounded hot-path status only. Historical counts belong to explicit audits."""
        return self._component_health()

    def current_health(self) -> ExternalHealth:
        def scalar(sql: str):
            return self.conn.execute(sql).fetchone()[0]

        return ExternalHealth(
            coinbase_count=int(scalar("SELECT COUNT(*) FROM coinbase_ticks")),
            kraken_count=int(scalar("SELECT COUNT(*) FROM kraken_ticks")),
            settlement_observation_count=int(
                scalar("SELECT COUNT(*) FROM settlement_reference_observations")
            ),
            price_reference_count=int(scalar("SELECT COUNT(*) FROM price_references")),
            latest_coinbase_at_us=scalar("SELECT MAX(received_at_us) FROM coinbase_ticks"),
            latest_kraken_at_us=scalar("SELECT MAX(received_at_us) FROM kraken_ticks"),
            latest_settlement_at_us=scalar(
                "SELECT MAX(received_at_us) FROM settlement_reference_observations"
            ),
            components=self._component_health(),
        )
