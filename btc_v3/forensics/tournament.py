from __future__ import annotations

import json
import math
import statistics
from dataclasses import dataclass
from typing import Any

from .analysis import WalletForensicsReport

COPYABILITY_VERSION = "RESEARCH_HEURISTIC_V1"
INFERENCE_VERSION = "RESEARCH_HEURISTIC_V1"


def _numeric(value: Any) -> float | None:
    if value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _lag_points(seconds: float | None) -> float:
    if seconds is None:
        return 0.0
    if seconds <= 0.5:
        return 35.0
    if seconds <= 1.0:
        return 30.0
    if seconds <= 2.0:
        return 24.0
    if seconds <= 5.0:
        return 12.0
    if seconds <= 10.0:
        return 5.0
    return 0.0


def _drift_points(drift: float | None) -> float:
    if drift is None:
        return 0.0
    drift = abs(drift)
    if drift <= 0.005:
        return 35.0
    if drift <= 0.010:
        return 30.0
    if drift <= 0.020:
        return 22.0
    if drift <= 0.050:
        return 10.0
    return 0.0


@dataclass(frozen=True)
class WalletTournamentRow:
    wallet: str
    label: str
    all_time_pnl: float | None
    volume_usdc: float | None
    profile_markets_traded: int | None
    btc5m_rounds: int
    btc5m_trade_actions: int
    both_side_ratio: float
    final60_action_ratio: float
    live_target_events: int
    median_observable_lag_seconds: float | None
    median_abs_copy_price_delta: float | None
    market_context_match_rate: float | None
    copyability_score: float | None
    copyability_state: str
    inference_usefulness_score: float
    copy_curve: dict[int, dict[str, float | int | None]]

    def to_dict(self) -> dict[str, Any]:
        return {
            **self.__dict__,
            "copy_curve": {str(key): value for key, value in sorted(self.copy_curve.items())},
        }


def build_tournament_row(
    *,
    wallet: str,
    label: str,
    report: WalletForensicsReport,
    profile_stats: dict[str, Any] | None,
    volume_stats: dict[str, Any] | None,
    live_events: list[dict[str, Any]],
    copy_curve: dict[int, dict[str, float | int | None]],
) -> WalletTournamentRow:
    profile_stats = profile_stats or {}
    volume_stats = volume_stats or {}
    rounds = report.btc5m_rounds
    both_ratio = report.both_side_rounds / rounds if rounds else 0.0
    final_ratio = (
        report.final_60s_trade_actions / report.btc5m_trade_actions
        if report.btc5m_trade_actions
        else 0.0
    )

    lags = [float(row["observable_lag_us"]) / 1_000_000 for row in live_events if row.get("observable_lag_us") is not None]
    deltas = [abs(float(row["copy_price_delta"])) for row in live_events if row.get("copy_price_delta") not in (None, "")]
    contexts = [bool(row.get("market_context_matches")) for row in live_events]
    median_lag = statistics.median(lags) if lags else None
    median_delta = statistics.median(deltas) if deltas else None
    context_rate = sum(contexts) / len(contexts) if contexts else None

    n_live = len(live_events)
    if n_live == 0:
        copyability = None
        copy_state = "NO_LIVE_EVENTS"
    else:
        sample_points = 5.0 * min(1.0, n_live / 20.0)
        context_points = 25.0 * (context_rate or 0.0)
        copyability = round(
            sample_points + context_points + _lag_points(median_lag) + _drift_points(median_delta),
            3,
        )
        copy_state = "PROVISIONAL" if n_live < 20 else "MEASURED"

    timing_coverage = sum(1 for value in report.timing_bands.values() if value > 0)
    classification_coverage = sum(
        1
        for value in (report.up_only_rounds, report.down_only_rounds, report.both_side_rounds)
        if value > 0
    )
    inference_score = round(
        40.0 * min(1.0, report.btc5m_rounds / 1000.0)
        + 30.0 * min(1.0, report.btc5m_trade_actions / 5000.0)
        + 15.0 * min(1.0, classification_coverage / 3.0)
        + 15.0 * min(1.0, timing_coverage / 7.0),
        3,
    )

    markets_value = profile_stats.get("trades")
    try:
        profile_markets = int(markets_value) if markets_value is not None else None
    except (TypeError, ValueError):
        profile_markets = None

    return WalletTournamentRow(
        wallet=wallet.lower(),
        label=label,
        all_time_pnl=_numeric(profile_stats.get("all_time_pnl")),
        volume_usdc=_numeric(volume_stats.get("volume_usdc")),
        profile_markets_traded=profile_markets,
        btc5m_rounds=report.btc5m_rounds,
        btc5m_trade_actions=report.btc5m_trade_actions,
        both_side_ratio=round(both_ratio, 6),
        final60_action_ratio=round(final_ratio, 6),
        live_target_events=n_live,
        median_observable_lag_seconds=None if median_lag is None else round(median_lag, 6),
        median_abs_copy_price_delta=None if median_delta is None else round(median_delta, 6),
        market_context_match_rate=None if context_rate is None else round(context_rate, 6),
        copyability_score=copyability,
        copyability_state=copy_state,
        inference_usefulness_score=inference_score,
        copy_curve=copy_curve,
    )


def tournament_manifest(rows: list[WalletTournamentRow]) -> dict[str, Any]:
    def ranked(field: str, reverse: bool = True) -> list[str]:
        available = [row for row in rows if getattr(row, field) is not None]
        return [
            row.wallet
            for row in sorted(
                available,
                key=lambda row: float(getattr(row, field)),
                reverse=reverse,
            )
        ]

    return {
        "copyability_version": COPYABILITY_VERSION,
        "inference_version": INFERENCE_VERSION,
        "wallets": [row.to_dict() for row in rows],
        "rankings": {
            "raw_all_time_pnl": ranked("all_time_pnl"),
            "observable_copyability": ranked("copyability_score"),
            "strategy_inference_usefulness": ranked("inference_usefulness_score"),
        },
        "limitations": [
            "Copyability is a transparent research heuristic, not a promise of future profitability.",
            "Raw wallet PnL is not copy PnL; copy execution starts only after a target trade becomes publicly observable.",
            "Live copyability needs many observed target events; NO_LIVE_EVENTS and PROVISIONAL states must not be overinterpreted.",
            "Historical wallet activity does not reveal cancelled/unfilled orders, private signals, or unavailable historical authoritative TWAP/PTB.",
        ],
    }


def manifest_json(rows: list[WalletTournamentRow]) -> str:
    return json.dumps(tournament_manifest(rows), indent=2, sort_keys=True)
