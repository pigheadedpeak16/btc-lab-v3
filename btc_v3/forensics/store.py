from __future__ import annotations

import csv
import json
import sqlite3
from dataclasses import asdict
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable

from ..storage.db import connect_sqlite
from ..timeutil import now_epoch_us
from .analysis import WalletForensicsReport
from .models import WalletActivity, WalletTrade


def _json(value: Any) -> str:
    def default(obj: Any) -> Any:
        if isinstance(obj, Decimal):
            return str(obj)
        raise TypeError(type(obj).__name__)

    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=default)


class ForensicsStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.conn = connect_sqlite(path)

    @classmethod
    def open_project(cls, root: Path) -> "ForensicsStore":
        return cls(root / "research" / "v3_research.sqlite")

    def __enter__(self) -> "ForensicsStore":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def close(self) -> None:
        self.conn.close()

    def save_profile(
        self,
        wallet: str,
        *,
        label: str | None,
        requested_address: str,
        profile_stats: dict[str, Any] | None,
        volume_stats: dict[str, Any] | None,
        pnl_stats: dict[str, Any] | None,
        gamma_profile: dict[str, Any] | None,
    ) -> None:
        now = now_epoch_us()
        self.conn.execute(
            """
            INSERT INTO forensic_wallets(
                wallet, label, requested_address, first_synced_at_us, last_synced_at_us,
                profile_stats_json, volume_stats_json, pnl_stats_json, gamma_profile_json
            ) VALUES(?,?,?,?,?,?,?,?,?)
            ON CONFLICT(wallet) DO UPDATE SET
                label=excluded.label,
                requested_address=excluded.requested_address,
                last_synced_at_us=excluded.last_synced_at_us,
                profile_stats_json=excluded.profile_stats_json,
                volume_stats_json=excluded.volume_stats_json,
                pnl_stats_json=excluded.pnl_stats_json,
                gamma_profile_json=excluded.gamma_profile_json
            """,
            (
                wallet.lower(),
                label,
                requested_address.lower(),
                now,
                now,
                _json(profile_stats),
                _json(volume_stats),
                _json(pnl_stats),
                _json(gamma_profile),
            ),
        )
        self.conn.commit()

    def save_activities(self, rows: Iterable[WalletActivity]) -> int:
        count = 0
        with self.conn:
            for row in rows:
                cur = self.conn.execute(
                    """
                    INSERT OR IGNORE INTO forensic_activity(
                        activity_id,wallet,event_type,condition_id,token_id,side,outcome,
                        price,size,usdc_size,timestamp_us,transaction_hash,title,slug,event_slug,raw_json
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        row.activity_id,
                        row.wallet,
                        row.event_type,
                        row.condition_id,
                        row.token_id,
                        row.side,
                        row.outcome,
                        str(row.price) if row.price is not None else None,
                        str(row.size) if row.size is not None else None,
                        str(row.usdc_size) if row.usdc_size is not None else None,
                        row.timestamp_us,
                        row.transaction_hash,
                        row.title,
                        row.slug,
                        row.event_slug,
                        row.raw_json,
                    ),
                )
                count += int(cur.rowcount > 0)
        return count

    def save_trades(self, rows: Iterable[WalletTrade]) -> int:
        count = 0
        with self.conn:
            for row in rows:
                cur = self.conn.execute(
                    """
                    INSERT OR IGNORE INTO forensic_trades(
                        trade_id,wallet,condition_id,token_id,side,outcome,price,size,timestamp_us,
                        transaction_hash,title,slug,event_slug,maker_address,taker_address,attribution,raw_json
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        row.trade_id,
                        row.wallet,
                        row.condition_id,
                        row.token_id,
                        row.side,
                        row.outcome,
                        str(row.price) if row.price is not None else None,
                        str(row.size) if row.size is not None else None,
                        row.timestamp_us,
                        row.transaction_hash,
                        row.title,
                        row.slug,
                        row.event_slug,
                        row.maker_address,
                        row.taker_address,
                        row.attribution,
                        row.raw_json,
                    ),
                )
                count += int(cur.rowcount > 0)
        return count

    def save_report(self, report: WalletForensicsReport) -> str:
        report_id = f"wallet_{report.wallet[2:10]}_{now_epoch_us()}"
        with self.conn:
            self.conn.execute("DELETE FROM forensic_rounds WHERE wallet=?", (report.wallet,))
            for row in report.rounds:
                self.conn.execute(
                    """
                    INSERT INTO forensic_rounds(
                        wallet,slug,round_start_us,round_end_us,first_action_us,last_action_us,
                        buy_up_shares,buy_down_shares,sell_up_shares,sell_down_shares,
                        buy_up_cost,buy_down_cost,trade_actions,redeem_actions,classification,
                        traded_both_sides,had_sell,had_redeem,final_60s_trade_actions,details_json
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        report.wallet,
                        row.slug,
                        row.round_start_us,
                        row.round_end_us,
                        row.first_action_us,
                        row.last_action_us,
                        row.buy_up_shares,
                        row.buy_down_shares,
                        row.sell_up_shares,
                        row.sell_down_shares,
                        row.buy_up_cost,
                        row.buy_down_cost,
                        row.trade_actions,
                        row.redeem_actions,
                        row.classification,
                        int(row.traded_both_sides),
                        int(row.had_sell),
                        int(row.had_redeem),
                        row.final_60s_trade_actions,
                        _json(asdict(row)),
                    ),
                )
            self.conn.execute(
                "INSERT INTO forensic_reports(report_id,wallet,created_at_us,report_json) VALUES(?,?,?,?)",
                (report_id, report.wallet, now_epoch_us(), _json(report.to_dict())),
            )
        return report_id

    def load_activities(self, wallet: str) -> list[WalletActivity]:
        rows = self.conn.execute(
            "SELECT raw_json FROM forensic_activity WHERE wallet=? ORDER BY timestamp_us ASC, activity_id ASC",
            (wallet.lower(),),
        ).fetchall()
        return [WalletActivity.from_api(wallet, json.loads(raw_json)) for (raw_json,) in rows]

    def load_trades(self, wallet: str) -> list[WalletTrade]:
        rows = self.conn.execute(
            "SELECT raw_json FROM forensic_trades WHERE wallet=? ORDER BY timestamp_us ASC, trade_id ASC",
            (wallet.lower(),),
        ).fetchall()
        return [WalletTrade.from_api(wallet, json.loads(raw_json)) for (raw_json,) in rows]


    def save_onchain_events(self, wallet: str, source_action: str, rows: Iterable[dict[str, Any]]) -> int:
        import hashlib

        count = 0
        with self.conn:
            for row in rows:
                raw_json = _json(row)
                event_id = "chain_" + hashlib.sha256(
                    f"{wallet.lower()}|{source_action}|{raw_json}".encode("utf-8")
                ).hexdigest()
                tx_hash = row.get("hash") or row.get("transactionHash")
                block_number = row.get("blockNumber")
                timestamp = row.get("timeStamp") or row.get("timestamp")
                try:
                    block_number_i = int(block_number) if block_number is not None else None
                except (TypeError, ValueError):
                    block_number_i = None
                try:
                    timestamp_us = int(timestamp) * 1_000_000 if timestamp is not None else None
                except (TypeError, ValueError):
                    timestamp_us = None
                cur = self.conn.execute(
                    """
                    INSERT OR IGNORE INTO forensic_onchain_events(
                        event_id,wallet,source_action,transaction_hash,block_number,timestamp_us,raw_json
                    ) VALUES(?,?,?,?,?,?,?)
                    """,
                    (event_id, wallet.lower(), source_action, tx_hash, block_number_i, timestamp_us, raw_json),
                )
                count += int(cur.rowcount > 0)
        return count


    def start_live_session(
        self,
        *,
        session_id: str,
        wallet: str,
        requested_wallet: str,
        started_at_us: int,
        config: dict[str, Any],
    ) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT OR REPLACE INTO forensic_live_sessions(
                    session_id,wallet,requested_wallet,started_at_us,ended_at_us,state,target_event_count,config_json
                ) VALUES(?,?,?,?,NULL,'RUNNING',0,?)
                """,
                (session_id, wallet.lower(), requested_wallet.lower(), started_at_us, _json(config)),
            )

    def finish_live_session(
        self,
        *,
        session_id: str,
        ended_at_us: int,
        target_event_count: int,
        state: str,
    ) -> None:
        with self.conn:
            self.conn.execute(
                """
                UPDATE forensic_live_sessions
                SET ended_at_us=?, state=?, target_event_count=?
                WHERE session_id=?
                """,
                (ended_at_us, state, target_event_count, session_id),
            )

    def save_live_sample(self, sample: Any) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT OR REPLACE INTO forensic_live_samples(
                    sample_id,session_id,wallet,sample_ts_us,slug,round_start_us,round_end_us,
                    seconds_remaining,state,unavailable_reason,ptb,official_twap,coinbase_price,
                    kraken_price,predictor_price,p_up_mc,p_up_analytic,distance_dollars,
                    returns_json,volatility_json,lab_json,book_json
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    sample.sample_id, sample.session_id, sample.wallet.lower(), sample.sample_ts_us,
                    sample.slug, sample.round_start_us, sample.round_end_us, sample.seconds_remaining,
                    sample.state, sample.unavailable_reason, sample.ptb, sample.official_twap,
                    sample.coinbase_price, sample.kraken_price, sample.predictor_price, sample.p_up_mc,
                    sample.p_up_analytic, sample.distance_dollars, sample.returns_json,
                    sample.volatility_json, sample.lab_json, sample.book_json,
                ),
            )

    def save_target_event(self, event: Any) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT OR IGNORE INTO forensic_target_events(
                    event_id,session_id,wallet,trade_id,trade_ts_us,detected_at_us,observable_lag_us,
                    slug,side,outcome,price,size,attribution,pre_trade_sample_id,detection_sample_id,
                    current_executable_price,copy_price_delta,market_context_matches,raw_json
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    event.event_id, event.session_id, event.wallet.lower(), event.trade_id,
                    event.trade_ts_us, event.detected_at_us, event.observable_lag_us, event.slug,
                    event.side, event.outcome, event.price, event.size, event.attribution,
                    event.pre_trade_sample_id, event.detection_sample_id, event.current_executable_price,
                    event.copy_price_delta, int(bool(event.market_context_matches)), event.raw_json,
                ),
            )

    def live_session_counts(self, session_id: str) -> tuple[int, int]:
        samples = self.conn.execute(
            "SELECT COUNT(*) FROM forensic_live_samples WHERE session_id=?", (session_id,)
        ).fetchone()[0]
        events = self.conn.execute(
            "SELECT COUNT(*) FROM forensic_target_events WHERE session_id=?", (session_id,)
        ).fetchone()[0]
        return int(samples), int(events)

    def load_wallet_metadata(self, wallet: str) -> tuple[str | None, dict[str, Any], dict[str, Any]]:
        row = self.conn.execute(
            "SELECT label,profile_stats_json,volume_stats_json FROM forensic_wallets WHERE wallet=?",
            (wallet.lower(),),
        ).fetchone()
        if row is None:
            return None, {}, {}
        label, profile_json, volume_json = row
        return label, json.loads(profile_json or "{}"), json.loads(volume_json or "{}")

    def live_events_for_wallet(self, wallet: str) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT observable_lag_us,copy_price_delta,market_context_matches,trade_ts_us,detected_at_us,slug,side,outcome,price,size
            FROM forensic_target_events
            WHERE wallet=?
            ORDER BY detected_at_us ASC
            """,
            (wallet.lower(),),
        ).fetchall()
        keys = (
            "observable_lag_us","copy_price_delta","market_context_matches","trade_ts_us",
            "detected_at_us","slug","side","outcome","price","size",
        )
        return [dict(zip(keys, row)) for row in rows]

    def save_copy_quote(self, quote: Any) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT OR REPLACE INTO forensic_copy_quotes(
                    quote_id,event_id,session_id,wallet,delay_ms,quote_ts_us,slug,side,outcome,
                    target_price,executable_price,price_delta,market_context_matches,details_json
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    quote.quote_id, quote.event_id, quote.session_id, quote.wallet.lower(),
                    quote.delay_ms, quote.quote_ts_us, quote.slug, quote.side, quote.outcome,
                    quote.target_price, quote.executable_price, quote.price_delta,
                    int(bool(quote.market_context_matches)), quote.details_json,
                ),
            )

    def copy_curve_for_wallet(self, wallet: str) -> dict[int, dict[str, float | int | None]]:
        rows = self.conn.execute(
            """
            SELECT delay_ms,price_delta,market_context_matches
            FROM forensic_copy_quotes
            WHERE wallet=?
            ORDER BY delay_ms ASC,quote_ts_us ASC
            """,
            (wallet.lower(),),
        ).fetchall()
        grouped: dict[int, list[tuple[str | None, int]]] = {}
        for delay_ms, price_delta, context in rows:
            grouped.setdefault(int(delay_ms), []).append((price_delta, int(context)))
        result: dict[int, dict[str, float | int | None]] = {}
        for delay_ms, values in grouped.items():
            deltas = [abs(float(delta)) for delta, _ in values if delta not in (None, "")]
            contexts = [context for _, context in values]
            result[delay_ms] = {
                "observations": len(values),
                "median_abs_price_delta": None if not deltas else float(sorted(deltas)[len(deltas) // 2]),
                "context_match_rate": None if not contexts else sum(contexts) / len(contexts),
            }
        return result

    def save_tournament(self, *, tournament_id: str, config: dict[str, Any], manifest: dict[str, Any]) -> None:
        with self.conn:
            self.conn.execute(
                "INSERT INTO forensic_tournaments(tournament_id,created_at_us,config_json,manifest_json) VALUES(?,?,?,?)",
                (tournament_id, now_epoch_us(), _json(config), _json(manifest)),
            )

    def export_tournament(self, root: Path, *, tournament_id: str, manifest: dict[str, Any]) -> Path:
        out = root / "research" / "datasets" / "wallet_tournaments"
        out.mkdir(parents=True, exist_ok=True)
        path = out / f"{tournament_id}.json"
        path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
        return path

    def export(self, root: Path, report: WalletForensicsReport) -> Path:
        out = root / "research" / "datasets" / "wallet_forensics" / report.wallet
        out.mkdir(parents=True, exist_ok=True)
        (out / "report.json").write_text(json.dumps(report.to_dict(), indent=2, sort_keys=True), encoding="utf-8")
        with (out / "rounds.csv").open("w", newline="", encoding="utf-8") as fh:
            fieldnames = list(asdict(report.rounds[0]).keys()) if report.rounds else [
                "slug","round_start_us","round_end_us","classification"
            ]
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            for row in report.rounds:
                writer.writerow(asdict(row))
        return out

    def start_latency_session(
        self, *, session_id: str, wallet: str, requested_wallet: str, started_at_us: int, config: dict[str, Any]
    ) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO forensic_latency_sessions(
                    session_id,wallet,requested_wallet,started_at_us,ended_at_us,state,poll_count,target_event_count,
                    rate_limit_count,service_unavailable_count,config_json,verdict_json
                ) VALUES(?,?,?,?,NULL,'RUNNING',0,0,0,0,?,NULL)
                """,
                (session_id, wallet.lower(), requested_wallet.lower(), started_at_us, _json(config)),
            )

    def finish_latency_session(
        self, *, session_id: str, ended_at_us: int, poll_count: int, target_event_count: int,
        rate_limit_count: int, service_unavailable_count: int, verdict: dict[str, Any], state: str
    ) -> None:
        with self.conn:
            self.conn.execute(
                """
                UPDATE forensic_latency_sessions
                SET ended_at_us=?,state=?,poll_count=?,target_event_count=?,rate_limit_count=?,
                    service_unavailable_count=?,verdict_json=?
                WHERE session_id=?
                """,
                (ended_at_us,state,poll_count,target_event_count,rate_limit_count,service_unavailable_count,_json(verdict),session_id),
            )

    def save_latency_poll(self, observation: Any) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT OR REPLACE INTO forensic_latency_polls(
                    poll_id,session_id,seq,request_started_us,response_received_us,rtt_us,status_code,retry_after_ms,
                    trace_id,row_count,newest_trade_ts_us,error_text
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (observation.poll_id,observation.session_id,observation.seq,observation.request_started_us,
                 observation.response_received_us,observation.rtt_us,observation.status_code,observation.retry_after_ms,
                 observation.trace_id,observation.row_count,observation.newest_trade_ts_us,observation.error_text),
            )

    def save_latency_event(self, event: Any) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT OR REPLACE INTO forensic_latency_events(
                    latency_event_id,session_id,wallet,trade_id,trade_ts_us,api_first_seen_us,api_request_started_us,
                    api_rtt_us,slug,token_id,side,outcome,price,size,transaction_hash,market_trade_id,
                    market_received_at_us,market_source_ts_us,market_to_wallet_us,correlation_quality,
                    correlation_candidate_count,detection_sample_id,current_executable_price,copy_price_delta,raw_json
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (event.latency_event_id,event.session_id,event.wallet.lower(),event.trade_id,event.trade_ts_us,
                 event.api_first_seen_us,event.api_request_started_us,event.api_rtt_us,event.slug,event.token_id,event.side,
                 event.outcome,event.price,event.size,event.transaction_hash,event.market_trade_id,event.market_received_at_us,
                 event.market_source_ts_us,event.market_to_wallet_us,event.correlation_quality,event.correlation_candidate_count,
                 event.detection_sample_id,event.current_executable_price,event.copy_price_delta,event.raw_json),
            )

    def correlated_wallet_latencies(self, wallet: str) -> list[int]:
        rows = self.conn.execute(
            """
            SELECT market_to_wallet_us FROM forensic_latency_events
            WHERE wallet=? AND market_to_wallet_us IS NOT NULL
              AND correlation_quality IN ('EXACT_TX','EXACT_TX_SIGNATURE','UNIQUE_SIGNATURE','UNIQUE_PRICE_TIME')
            ORDER BY api_first_seen_us ASC
            """,
            (wallet.lower(),),
        ).fetchall()
        return [int(row[0]) for row in rows if row[0] is not None and int(row[0]) >= 0]
