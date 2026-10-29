from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager, suppress
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any, Iterable

from ..storage.db import connect_sqlite


def _json(value: object) -> str:
    if is_dataclass(value):
        value = asdict(value)
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


class ForwardWalletStore:
    """Research-only persistence for Phase 5.6 forward wallet strategies.

    This store intentionally lives in research/v3_research.sqlite.  It contains no
    credential material and no order-submission code.
    """

    def __init__(self, path: Path) -> None:
        self.path = path
        self.conn = connect_sqlite(path)
        self._savepoint_sequence = 0

    @contextmanager
    def transaction(self):
        """Composable atomic operations; inner store calls cannot commit the outer unit."""
        self._savepoint_sequence += 1
        name = f"paper_{self._savepoint_sequence}"
        self.conn.execute(f"SAVEPOINT {name}")
        try:
            yield
            self.conn.execute(f"RELEASE SAVEPOINT {name}")
        except BaseException:
            # SQLite can itself abort a transaction on I/O errors. Preserve the
            # original exception even when its savepoints no longer exist.
            with suppress(sqlite3.Error):
                self.conn.execute(f"ROLLBACK TO SAVEPOINT {name}")
                self.conn.execute(f"RELEASE SAVEPOINT {name}")
            raise

    def order_has_executed(self, order_id: str) -> bool:
        row = self.conn.execute(
            "SELECT status FROM wallet_forward_orders WHERE order_id=?", (order_id,)
        ).fetchone()
        return row is not None and row[0] in {"FILLED", "PARTIAL"}

    def taker_execution_block_reason(self, order_id: str) -> str | None:
        row = self.conn.execute(
            "SELECT status FROM wallet_forward_orders WHERE order_id=?", (order_id,)
        ).fetchone()
        if row is None:
            return "UNKNOWN_PAPER_ORDER"
        if row[0] in {"FILLED", "PARTIAL"}:
            return "ALREADY_EXECUTED"
        return None if row[0] in {"SCHEDULED", "CONFIRMED"} else "ORDER_TERMINAL"

    @classmethod
    def open_project(cls, root: Path) -> "ForwardWalletStore":
        return cls(root / "research" / "v3_research.sqlite")

    def __enter__(self) -> "ForwardWalletStore":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def close(self) -> None:
        self.conn.close()

    def start_session(
        self,
        *,
        session_id: str,
        wallet: str,
        started_at_us: int,
        config: object,
    ) -> None:
        with self.transaction():
            self.conn.execute(
                """
                INSERT INTO wallet_forward_sessions(
                    session_id,wallet,started_at_us,state,config_json,source_stats_json,summary_json
                ) VALUES(?,?,?,?,?,?,?)
                """,
                (session_id, wallet.lower(), started_at_us, "RUNNING", _json(config), "{}", "{}"),
            )

    def finish_session(
        self,
        *,
        session_id: str,
        ended_at_us: int,
        state: str,
        source_stats: object,
        summary: object,
    ) -> None:
        with self.transaction():
            self.conn.execute(
                """
                UPDATE wallet_forward_sessions
                SET ended_at_us=?,state=?,source_stats_json=?,summary_json=?
                WHERE session_id=?
                """,
                (ended_at_us, state, _json(source_stats), _json(summary), session_id),
            )

    def save_source_observation(
        self,
        *,
        session_id: str,
        economic_key: str,
        source: str,
        source_row_id: str,
        seen_at_us: int,
        request_started_us: int | None,
        rtt_us: int | None,
        status_code: int | None,
        raw_json: str,
    ) -> None:
        with self.transaction():
            self.conn.execute(
                """
                INSERT OR IGNORE INTO wallet_forward_source_observations(
                    session_id,economic_key,source,source_row_id,seen_at_us,request_started_us,
                    rtt_us,status_code,raw_json
                ) VALUES(?,?,?,?,?,?,?,?,?)
                """,
                (
                    session_id,
                    economic_key,
                    source,
                    source_row_id,
                    seen_at_us,
                    request_started_us,
                    rtt_us,
                    status_code,
                    raw_json,
                ),
            )

    def save_signal(
        self,
        *,
        signal_id: str,
        session_id: str,
        economic_key: str,
        source: str,
        source_first_seen_us: int,
        trade_ts_us: int | None,
        slug: str | None,
        token_id: str | None,
        side: str | None,
        outcome: str | None,
        target_price: str | None,
        target_size: str | None,
        transaction_hash: str | None,
        publication_lag_lower_us: int | None,
        publication_lag_upper_us: int | None,
        detection_sample_id: str | None,
        raw_json: str,
    ) -> bool:
        with self.transaction():
            cur = self.conn.execute(
                """
                INSERT OR IGNORE INTO wallet_forward_signals(
                    signal_id,session_id,economic_key,source,source_first_seen_us,trade_ts_us,
                    slug,token_id,side,outcome,target_price,target_size,transaction_hash,
                    publication_lag_lower_us,publication_lag_upper_us,detection_sample_id,raw_json
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    signal_id,
                    session_id,
                    economic_key,
                    source,
                    source_first_seen_us,
                    trade_ts_us,
                    slug,
                    token_id,
                    side,
                    outcome,
                    target_price,
                    target_size,
                    transaction_hash,
                    publication_lag_lower_us,
                    publication_lag_upper_us,
                    detection_sample_id,
                    raw_json,
                ),
            )
            return cur.rowcount > 0

    def save_order(
        self,
        *,
        order_id: str,
        session_id: str,
        strategy: str,
        signal_id: str | None,
        slug: str,
        token_id: str,
        outcome: str,
        side: str,
        style: str,
        price_limit: str | None,
        shares: str,
        created_at_us: int,
        scheduled_at_us: int,
        status: str,
        reason: str | None,
        metadata: object,
    ) -> None:
        with self.transaction():
            existing = self.conn.execute(
                "SELECT session_id,strategy,signal_id,slug,token_id,outcome,side,style FROM wallet_forward_orders WHERE order_id=?",
                (order_id,),
            ).fetchone()
            if existing is not None:
                identity = (session_id, strategy, signal_id, slug, token_id, outcome, side, style)
                if tuple(existing) != identity:
                    raise ValueError("ORDER_ID_CONFLICT")
                # Repeated scheduling must not erase executed quantities/status.
                return
            self.conn.execute(
                """
                INSERT OR REPLACE INTO wallet_forward_orders(
                    order_id,session_id,strategy,signal_id,slug,token_id,outcome,side,style,
                    price_limit,shares,created_at_us,scheduled_at_us,status,reason,metadata_json
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    order_id,
                    session_id,
                    strategy,
                    signal_id,
                    slug,
                    token_id,
                    outcome,
                    side,
                    style,
                    price_limit,
                    shares,
                    created_at_us,
                    scheduled_at_us,
                    status,
                    reason,
                    _json(metadata),
                ),
            )

    def update_order_submitted_shares(self, order_id: str, shares: str, *, price_limit: str | None = None) -> None:
        """Record the actually attempted paper quantity, preserving order identity."""
        with self.transaction():
            cur = self.conn.execute(
                "UPDATE wallet_forward_orders SET shares=?,price_limit=COALESCE(?,price_limit) WHERE order_id=?",
                (shares, price_limit, order_id),
            )
            if cur.rowcount != 1:
                raise ValueError("UNKNOWN_PAPER_ORDER")

    def save_signal_strategy_decision(
        self,
        *,
        session_id: str,
        signal_id: str,
        strategy: str,
        decided_at_us: int,
        decision: str,
        reason: str | None,
        context: object,
    ) -> None:
        """Record each event append-only, plus latest state for old dashboards."""
        payload = _json(context)
        with self.transaction():
            self.conn.execute(
                """INSERT INTO wallet_strategy_decision_events(
                    session_id,signal_id,strategy,decided_at_us,decision,reason,context_json
                ) VALUES(?,?,?,?,?,?,?)""",
                (session_id,signal_id,strategy,decided_at_us,decision,reason,payload),
            )
            self.conn.execute(
                """
                INSERT OR REPLACE INTO wallet_signal_strategy_decisions(
                    session_id,signal_id,strategy,decided_at_us,decision,reason,context_json
                ) VALUES(?,?,?,?,?,?,?)
                """,
                (
                    session_id,
                    signal_id,
                    strategy,
                    decided_at_us,
                    decision,
                    reason,
                    payload,
                ),
            )

    def update_order_status(self, order_id: str, *, status: str, reason: str | None = None) -> None:
        with self.transaction():
            self.conn.execute(
                "UPDATE wallet_forward_orders SET status=?,reason=? WHERE order_id=?",
                (status, reason, order_id),
            )

    def save_fill(
        self,
        *,
        fill_id: str,
        order_id: str,
        session_id: str,
        strategy: str,
        slug: str,
        token_id: str,
        outcome: str,
        side: str,
        fill_ts_us: int,
        shares: str,
        price: str,
        notional: str,
        fee: str,
        liquidity_role: str,
        metadata: object,
    ) -> None:
        with self.transaction():
            self.conn.execute(
                """
                INSERT INTO wallet_forward_fills(
                    fill_id,order_id,session_id,strategy,slug,token_id,outcome,side,fill_ts_us,
                    shares,price,notional,fee,liquidity_role,metadata_json
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    fill_id,
                    order_id,
                    session_id,
                    strategy,
                    slug,
                    token_id,
                    outcome,
                    side,
                    fill_ts_us,
                    shares,
                    price,
                    notional,
                    fee,
                    liquidity_role,
                    _json(metadata),
                ),
            )

    def upsert_round(
        self,
        *,
        session_id: str,
        strategy: str,
        slug: str,
        round_start_us: int,
        round_end_us: int,
        up_shares: str,
        down_shares: str,
        up_cost: str,
        down_cost: str,
        sell_proceeds: str,
        fees: str,
        settled_side: str | None,
        settled_at_us: int | None,
        pnl: str | None,
        details: object,
    ) -> None:
        with self.transaction():
            existing = self.conn.execute(
                """SELECT up_shares,down_shares,up_cost,down_cost,sell_proceeds,fees,
                          settled_side,settled_at_us,pnl,details_json
                   FROM wallet_forward_rounds WHERE session_id=? AND strategy=? AND slug=?""",
                (session_id, strategy, slug),
            ).fetchone()
            if existing is not None and existing[8] is not None:
                incoming = (up_shares, down_shares, up_cost, down_cost, sell_proceeds,
                            fees, settled_side, settled_at_us, pnl, _json(details))
                if tuple(existing) != incoming:
                    raise ValueError("POSITION_SETTLED")
                return
            self.conn.execute(
                """
                INSERT INTO wallet_forward_rounds(
                    session_id,strategy,slug,round_start_us,round_end_us,up_shares,down_shares,
                    up_cost,down_cost,sell_proceeds,fees,settled_side,settled_at_us,pnl,details_json
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(session_id,strategy,slug) DO UPDATE SET
                    up_shares=excluded.up_shares,
                    down_shares=excluded.down_shares,
                    up_cost=excluded.up_cost,
                    down_cost=excluded.down_cost,
                    sell_proceeds=excluded.sell_proceeds,
                    fees=excluded.fees,
                    settled_side=excluded.settled_side,
                    settled_at_us=excluded.settled_at_us,
                    pnl=excluded.pnl,
                    details_json=excluded.details_json
                """,
                (
                    session_id,
                    strategy,
                    slug,
                    round_start_us,
                    round_end_us,
                    up_shares,
                    down_shares,
                    up_cost,
                    down_cost,
                    sell_proceeds,
                    fees,
                    settled_side,
                    settled_at_us,
                    pnl,
                    _json(details),
                ),
            )

    def reconcile_session(self, session_id: str) -> dict[str, Any]:
        from .ledger_integrity import reconcile_session
        with self.transaction():
            return reconcile_session(self.conn, session_id)

    def session_summary(self, session_id: str) -> dict[str, Any]:
        result: dict[str, Any] = {"session_id": session_id, "strategies": {},
                                 "pnl_scope": "SETTLED_ONLY_UNSETTLED_EXPOSURE_EXCLUDED"}
        strategies = [
            str(r[0])
            for r in self.conn.execute(
                "SELECT DISTINCT strategy FROM wallet_forward_orders WHERE session_id=? ORDER BY strategy",
                (session_id,),
            ).fetchall()
        ]
        for strategy in strategies:
            orders = self.conn.execute(
                "SELECT COUNT(*),SUM(CASE WHEN status='FILLED' THEN 1 ELSE 0 END),SUM(CASE WHEN status='REJECTED' THEN 1 ELSE 0 END) FROM wallet_forward_orders WHERE session_id=? AND strategy=?",
                (session_id, strategy),
            ).fetchone()
            fills = self.conn.execute(
                "SELECT COUNT(*),COALESCE(SUM(CAST(notional AS REAL)),0),COALESCE(SUM(CAST(fee AS REAL)),0) FROM wallet_forward_fills WHERE session_id=? AND strategy=?",
                (session_id, strategy),
            ).fetchone()
            pnl = self.conn.execute(
                "SELECT COUNT(*),COALESCE(SUM(CAST(pnl AS REAL)),0) FROM wallet_forward_rounds WHERE session_id=? AND strategy=? AND pnl IS NOT NULL",
                (session_id, strategy),
            ).fetchone()
            unsettled = self.conn.execute(
                """SELECT COUNT(*),COALESCE(SUM(CAST(up_cost AS REAL)+CAST(down_cost AS REAL)),0),
                          COALESCE(SUM(CAST(fees AS REAL)),0)
                   FROM wallet_forward_rounds WHERE session_id=? AND strategy=? AND pnl IS NULL""",
                (session_id, strategy),
            ).fetchone()
            result["strategies"][strategy] = {
                "orders": int(orders[0] or 0),
                "filled_orders": int(orders[1] or 0),
                "rejected_orders": int(orders[2] or 0),
                "fills": int(fills[0] or 0),
                "notional": float(fills[1] or 0.0),
                "fees": float(fills[2] or 0.0),
                "settled_rounds": int(pnl[0] or 0),
                "settled_pnl": float(pnl[1] or 0.0),
                "unsettled_rounds": int(unsettled[0] or 0),
                "unsettled_purchase_cost": float(unsettled[1] or 0.0),
                "unsettled_fees": float(unsettled[2] or 0.0),
            }
        result["signals"] = int(
            self.conn.execute(
                "SELECT COUNT(*) FROM wallet_forward_signals WHERE session_id=?",
                (session_id,),
            ).fetchone()[0]
        )
        result["integrity"] = self.reconcile_session(session_id)
        result["rejection_reasons"] = {
            str(row[0]): int(row[1]) for row in self.conn.execute(
                "SELECT reason,COUNT(*) FROM wallet_forward_orders WHERE session_id=? AND status='REJECTED' GROUP BY reason",
                (session_id,),
            ).fetchall()
        }
        result["cancellation_reasons"] = {
            str(row[0]): int(row[1]) for row in self.conn.execute(
                "SELECT reason,COUNT(*) FROM wallet_forward_orders WHERE session_id=? AND status='CANCELLED' GROUP BY reason",
                (session_id,),
            ).fetchall()
        }
        return result
